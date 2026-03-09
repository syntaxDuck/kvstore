import asyncio
import random
import time
from typing import Any

import aiohttp

from .config import settings
from .logging import get_logger
from .metrics import get_metrics
from .types import NodeDetails, RpcRequest, RpcResponse

logger = get_logger(__name__)

IDEMPOTENT_RPC_TYPES = {"PING", "HEARTBEAT", "REQUEST_VOTE", "APPEND_ENTRY"}


class PeerHttpClient:
    def __init__(self, peer_details: NodeDetails) -> None:
        self.node_id = peer_details.id
        self.node_role = peer_details.role
        self.host = peer_details.host
        self.port = peer_details.port
        self.base_url = f"http://{self.host}:{self.port}"
        self.address = (peer_details.host, peer_details.port)

        self._session: aiohttp.ClientSession | None = None
        self._timeout = aiohttp.ClientTimeout(
            total=settings.RPC_HTTP_TOTAL_TIMEOUT_SEC,
            connect=settings.RPC_HTTP_CONNECT_TIMEOUT_SEC,
            sock_read=settings.RPC_HTTP_READ_TIMEOUT_SEC,
        )
        self._max_retries = settings.RPC_HTTP_MAX_RETRIES
        self._retry_backoff_base = settings.RPC_HTTP_RETRY_BACKOFF_BASE_SEC
        self._retry_backoff_max = settings.RPC_HTTP_RETRY_BACKOFF_MAX_SEC

    @property
    def id(self):
        return self.node_id

    @property
    def role(self):
        return self.node_role

    def _should_retry(self, message_type: str) -> bool:
        return message_type in IDEMPOTENT_RPC_TYPES

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _sleep_for_retry(self, attempt: int) -> float:
        base_delay = min(self._retry_backoff_base * (2 ** (attempt - 1)), self._retry_backoff_max)
        jitter = random.uniform(0.0, base_delay / 2)
        delay = base_delay + jitter
        await asyncio.sleep(delay)
        return delay

    def _metric_prefix(self, message_type: str) -> str:
        return f"peer_rpc.{message_type.lower()}"

    def _record_metrics(self, message_type: str, outcome: str, duration_ms: float) -> None:
        metrics = get_metrics()
        prefix = self._metric_prefix(message_type)
        metrics.record_timing_sync(f"{prefix}.duration_ms.{outcome}", duration_ms)
        metrics.increment_counter_sync(f"{prefix}.count.{outcome}")

    def _error_response(
        self,
        message_type: str,
        category: str,
        error: Exception,
        attempt: int,
    ) -> RpcResponse:
        return RpcResponse.err(
            self.id,
            self.role,
            {
                "category": category,
                "error": str(error),
                "rpc_type": message_type,
                "peer_id": self.id,
                "attempt": attempt,
            },
        )

    async def _request_once(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        payload: Any = None,
    ) -> dict[str, Any]:
        session = await self._get_session()
        async with session.post(f"{self.base_url}{endpoint}", params=params, json=payload) as resp:
            resp.raise_for_status()
            return await resp.json()

    def _to_rpc_response(self, message: RpcRequest, data: dict[str, Any]) -> RpcResponse:
        if message.type == "PING":
            return RpcResponse.ok(self.id, self.role, data)

        if message.type == "REQUEST_VOTE":
            return RpcResponse.vote_response(self.id, self.role, data.get("vote", False))

        if message.type in {"APPEND_ENTRY", "HEARTBEAT"}:
            if data.get("success"):
                return RpcResponse.ack(self.id, self.role)
            return RpcResponse.err(self.id, self.role, data)

        return RpcResponse.ok(self.id, self.role, data)

    def _build_request(self, message: RpcRequest) -> tuple[str, dict[str, Any] | None, Any]:
        endpoint_map = {
            "PING": "/internal/v1/ping",
            "HEARTBEAT": "/internal/v1/heartbeat",
            "REQUEST_VOTE": "/internal/v1/vote",
            "APPEND_ENTRY": "/internal/v1/append",
            "CLIENT_WRITE": "/client/v1/kv",
            "CLIENT_GET": "/client/v1/kv",
        }

        endpoint = endpoint_map.get(message.type)
        if not endpoint:
            raise ValueError(f"Unknown message type: {message.type}")

        if message.type == "PING":
            return endpoint, None, None

        if message.type == "REQUEST_VOTE":
            return (
                endpoint,
                {
                    "candidate_id": message.node_id,
                    "term": message.term,
                    "last_log_index": message.last_log_index,
                    "last_log_term": message.last_log_term,
                },
                None,
            )

        if message.type == "APPEND_ENTRY":
            payload = message.payload
            if payload and hasattr(payload, "model_dump"):
                payload = payload.model_dump()
            return (
                endpoint,
                {
                    "leader_id": message.node_id,
                    "term": message.term,
                    "prev_log_index": message.last_log_index,
                    "prev_log_term": message.last_log_term,
                },
                [payload] if payload else None,
            )

        if message.type == "HEARTBEAT":
            return (
                endpoint,
                {
                    "leader_id": message.node_id,
                    "term": message.term,
                    "last_log_index": message.last_log_index,
                    "last_log_term": message.last_log_term,
                    "commit_index": message.commit_index,
                },
                None,
            )

        return endpoint, None, message.payload

    async def send_rpc(self, message: RpcRequest) -> RpcResponse:
        logger.debug("Sending %s to peer %s at %s", message.type, self.id, self.base_url)

        try:
            endpoint, params, payload = self._build_request(message)
        except Exception as exc:
            return self._error_response(message.type, "request_build_error", exc, 1)

        max_attempts = 1 + self._max_retries if self._should_retry(message.type) else 1

        for attempt in range(1, max_attempts + 1):
            start = time.perf_counter()
            try:
                data = await self._request_once(endpoint, params=params, payload=payload)
                duration_ms = (time.perf_counter() - start) * 1000
                self._record_metrics(message.type, "ok", duration_ms)
                return self._to_rpc_response(message, data)
            except aiohttp.ClientResponseError as exc:
                duration_ms = (time.perf_counter() - start) * 1000
                self._record_metrics(message.type, "http_error", duration_ms)
                retryable = exc.status >= 500
                if self._should_retry(message.type) and retryable and attempt < max_attempts:
                    logger.warning(
                        "peer_rpc_retry node_id=%s peer_id=%s rpc_type=%s attempt=%s term=%s category=http_error status=%s",
                        message.node_id,
                        self.id,
                        message.type,
                        attempt,
                        message.term,
                        exc.status,
                    )
                    delay = await self._sleep_for_retry(attempt)
                    delay_ms = max((delay or 0.0) * 1000, 0.0)
                    get_metrics().record_timing_sync(
                        f"{self._metric_prefix(message.type)}.retry_delay_ms",
                        delay_ms,
                    )
                    logger.debug(
                        "peer_rpc_retry_delay node_id=%s peer_id=%s rpc_type=%s attempt=%s delay_ms=%.2f",
                        message.node_id,
                        self.id,
                        message.type,
                        attempt,
                        delay_ms,
                    )
                    continue
                logger.error(
                    "peer_rpc_failure node_id=%s peer_id=%s rpc_type=%s attempt=%s term=%s category=http_error status=%s",
                    message.node_id,
                    self.id,
                    message.type,
                    attempt,
                    message.term,
                    exc.status,
                )
                return self._error_response(message.type, "http_error", exc, attempt)
            except (
                aiohttp.ClientConnectionError,
                aiohttp.ClientOSError,
                aiohttp.ServerTimeoutError,
                aiohttp.ClientPayloadError,
                asyncio.TimeoutError,
            ) as exc:
                duration_ms = (time.perf_counter() - start) * 1000
                self._record_metrics(message.type, "transport_error", duration_ms)
                if self._should_retry(message.type) and attempt < max_attempts:
                    logger.warning(
                        "peer_rpc_retry node_id=%s peer_id=%s rpc_type=%s attempt=%s term=%s category=transport_error err=%s",
                        message.node_id,
                        self.id,
                        message.type,
                        attempt,
                        message.term,
                        exc,
                    )
                    delay = await self._sleep_for_retry(attempt)
                    delay_ms = max((delay or 0.0) * 1000, 0.0)
                    get_metrics().record_timing_sync(
                        f"{self._metric_prefix(message.type)}.retry_delay_ms",
                        delay_ms,
                    )
                    logger.debug(
                        "peer_rpc_retry_delay node_id=%s peer_id=%s rpc_type=%s attempt=%s delay_ms=%.2f",
                        message.node_id,
                        self.id,
                        message.type,
                        attempt,
                        delay_ms,
                    )
                    continue
                logger.error(
                    "peer_rpc_failure node_id=%s peer_id=%s rpc_type=%s attempt=%s term=%s category=transport_error err=%s",
                    message.node_id,
                    self.id,
                    message.type,
                    attempt,
                    message.term,
                    exc,
                )
                return self._error_response(message.type, "transport_error", exc, attempt)
            except ValueError as exc:
                duration_ms = (time.perf_counter() - start) * 1000
                self._record_metrics(message.type, "decode_error", duration_ms)
                logger.error(
                    "peer_rpc_failure node_id=%s peer_id=%s rpc_type=%s attempt=%s term=%s category=decode_error err=%s",
                    message.node_id,
                    self.id,
                    message.type,
                    attempt,
                    message.term,
                    exc,
                )
                return self._error_response(message.type, "decode_error", exc, attempt)
            except Exception as exc:  # pragma: no cover - defensive
                duration_ms = (time.perf_counter() - start) * 1000
                self._record_metrics(message.type, "unexpected_error", duration_ms)
                logger.error(
                    "peer_rpc_failure node_id=%s peer_id=%s rpc_type=%s attempt=%s term=%s category=unexpected_error err=%s",
                    message.node_id,
                    self.id,
                    message.type,
                    attempt,
                    message.term,
                    exc,
                )
                return self._error_response(message.type, "unexpected_error", exc, attempt)

        return RpcResponse.err(
            self.id,
            self.role,
            {
                "category": "retry_exhausted",
                "rpc_type": message.type,
                "peer_id": self.id,
            },
        )

    async def ping(self) -> RpcResponse:
        """Simple ping to check if peer is reachable."""
        message = RpcRequest.ping(self.id, self.role)
        return await self.send_rpc(message)
