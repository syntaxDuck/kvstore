import asyncio
from unittest.mock import AsyncMock

import aiohttp
import pytest

from src.core.metrics import get_metrics
from src.core.peer_http_client import PeerHttpClient
from src.core.types import Command, NodeDetails, RpcRequest, RpcResponse


@pytest.fixture
def peer_client():
    get_metrics().reset()
    peer = NodeDetails(id=2, role="FOLLOWER", host="127.0.0.1", port=5004)
    return PeerHttpClient(peer)


@pytest.mark.asyncio
async def test_idempotent_rpc_retries_then_succeeds(peer_client):
    calls = {"count": 0}

    async def fake_request_once(endpoint, params=None, payload=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise asyncio.TimeoutError("timed out")
        return {"success": True}

    async def no_sleep(attempt):
        return None

    peer_client._request_once = fake_request_once
    peer_client._sleep_for_retry = no_sleep

    req = RpcRequest.heartbeat(
        node_id=1,
        node_role="LEADER",
        term=3,
        last_log_index=10,
        last_log_term=3,
        commit_index=10,
    )
    res = await peer_client.send_rpc(req)

    assert res.is_ack is True
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_non_idempotent_rpc_does_not_retry(peer_client):
    calls = {"count": 0}

    async def always_timeout(endpoint, params=None, payload=None):
        calls["count"] += 1
        raise asyncio.TimeoutError("timed out")

    peer_client._request_once = always_timeout

    req = RpcRequest.client_write(
        node_id=1,
        node_role="LEADER",
        term=3,
        cmd=Command(op="SET", key="foo", val="bar"),
    )
    res = await peer_client.send_rpc(req)

    assert res.is_err is True
    assert res.payload["category"] == "transport_error"
    assert calls["count"] == 1

    metrics = get_metrics().get_all_metrics()
    assert metrics["peer_rpc.client_write.count.transport_error"]["count"] == 1


@pytest.mark.asyncio
async def test_unknown_message_type_returns_structured_error(peer_client):
    req = RpcRequest(type="BOGUS", node_id=1, node_role="LEADER", payload=None)
    res = await peer_client.send_rpc(req)

    assert res.is_err is True
    assert res.payload["category"] == "request_build_error"
    assert res.payload["rpc_type"] == "BOGUS"


@pytest.mark.asyncio
async def test_retry_logs_include_diagnostic_context(peer_client, caplog):
    calls = {"count": 0}

    async def fail_then_succeed(endpoint, params=None, payload=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise asyncio.TimeoutError("timed out")
        return {"success": True}

    async def no_sleep(attempt):
        return None

    peer_client._request_once = fail_then_succeed
    peer_client._sleep_for_retry = no_sleep

    req = RpcRequest.heartbeat(
        node_id=1,
        node_role="LEADER",
        term=8,
        last_log_index=10,
        last_log_term=8,
        commit_index=10,
    )
    with caplog.at_level("WARNING"):
        await peer_client.send_rpc(req)

    log_text = "\n".join(caplog.messages)
    assert "peer_rpc_retry" in log_text
    assert "node_id=1" in log_text
    assert "peer_id=2" in log_text
    assert "rpc_type=HEARTBEAT" in log_text
    assert "attempt=1" in log_text
    assert "term=8" in log_text
    assert "category=transport_error" in log_text


@pytest.mark.asyncio
async def test_peer_rpc_metrics_recorded(peer_client):
    metrics = get_metrics()
    metrics.reset()

    async def fake_request_once(endpoint, params=None, payload=None):
        return {"status": "ok"}

    peer_client._request_once = fake_request_once

    req = RpcRequest.ping(node_id=1, node_role="LEADER")
    res = await peer_client.send_rpc(req)

    assert res.is_ok is True
    all_metrics = metrics.get_all_metrics()
    assert all_metrics["peer_rpc.ping.count.ok"]["count"] == 1


@pytest.mark.asyncio
async def test_close_closes_underlying_http_session(peer_client):
    session = await peer_client._get_session()
    assert session.closed is False

    await peer_client.close()

    assert session.closed is True


@pytest.mark.asyncio
async def test_sleep_for_retry_uses_backoff_plus_jitter(peer_client, monkeypatch):
    recorded = {}

    async def fake_sleep(delay):
        recorded["delay"] = delay

    monkeypatch.setattr("src.core.peer_http_client.random.uniform", lambda _a, _b: 0.25)
    monkeypatch.setattr("src.core.peer_http_client.asyncio.sleep", fake_sleep)

    await peer_client._sleep_for_retry(2)

    # attempt=2 => backoff doubles once from configured base, then adds jitter.
    expected_base = min(
        peer_client._retry_backoff_base * (2 ** (2 - 1)),
        peer_client._retry_backoff_max,
    )
    assert recorded["delay"] == pytest.approx(expected_base + 0.25)


@pytest.mark.asyncio
async def test_request_once_posts_and_decodes_json(peer_client):
    class FakeResponse:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        async def json(self):
            return {"ok": True}

    class FakeSession:
        def post(self, url, params=None, json=None):
            assert url.endswith("/internal/v1/ping")
            assert params == {"a": 1}
            assert json == {"x": 2}
            return FakeResponse()

    async def fake_get_session():
        return FakeSession()

    peer_client._get_session = fake_get_session
    data = await peer_client._request_once("/internal/v1/ping", params={"a": 1}, payload={"x": 2})

    assert data == {"ok": True}


def test_to_rpc_response_maps_vote_and_append_and_default(peer_client):
    vote = peer_client._to_rpc_response(
        RpcRequest.request_vote(1, "LEADER", term=1, last_log_index=0, last_log_term=0),
        {"vote": True},
    )
    append_fail = peer_client._to_rpc_response(
        RpcRequest.append_entry(1, "LEADER", term=1, cmd=Command(op="SET", key="k", val="v")),
        {"success": False, "error": "mismatch"},
    )
    default_ok = peer_client._to_rpc_response(
        RpcRequest.client_get(1, "LEADER", cmd=Command(op="GET", key="k", val=None)),
        {"val": "v"},
    )

    assert vote.payload == {"vote": True}
    assert append_fail.is_err is True
    assert append_fail.payload["error"] == "mismatch"
    assert default_ok.is_ok is True
    assert default_ok.payload == {"val": "v"}


def test_build_request_formats_vote_and_append_payload(peer_client):
    vote_endpoint, vote_params, vote_payload = peer_client._build_request(
        RpcRequest.request_vote(1, "CANDIDATE", term=4, last_log_index=9, last_log_term=3)
    )
    append_endpoint, append_params, append_payload = peer_client._build_request(
        RpcRequest.append_entry(1, "LEADER", term=5, last_log_index=10, last_log_term=5, cmd=Command(op="SET", key="k", val="v"))
    )

    assert vote_endpoint == "/internal/v1/vote"
    assert vote_params["candidate_id"] == 1
    assert vote_payload is None
    assert append_endpoint == "/internal/v1/append"
    assert append_params["prev_log_index"] == 10
    assert append_payload == [{"op": "SET", "key": "k", "val": "v"}]


@pytest.mark.asyncio
async def test_send_rpc_http_error_retries_then_succeeds(peer_client):
    calls = {"count": 0}

    async def fail_then_ok(_endpoint, params=None, payload=None):
        calls["count"] += 1
        if calls["count"] == 1:
            request_info = aiohttp.RequestInfo(
                url=aiohttp.client_reqrep.URL("http://127.0.0.1"),
                method="POST",
                headers={},
                real_url=aiohttp.client_reqrep.URL("http://127.0.0.1"),
            )
            raise aiohttp.ClientResponseError(
                request_info=request_info,
                history=(),
                status=503,
                message="service unavailable",
            )
        return {"success": True}

    peer_client._request_once = fail_then_ok
    async def no_sleep(_attempt):
        return None

    peer_client._sleep_for_retry = no_sleep
    req = RpcRequest.heartbeat(1, "LEADER", term=2, last_log_index=1, last_log_term=1, commit_index=1)

    res = await peer_client.send_rpc(req)

    assert res.is_ack is True
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_send_rpc_http_error_not_retryable_returns_error(peer_client):
    async def always_http_400(_endpoint, params=None, payload=None):
        request_info = aiohttp.RequestInfo(
            url=aiohttp.client_reqrep.URL("http://127.0.0.1"),
            method="POST",
            headers={},
            real_url=aiohttp.client_reqrep.URL("http://127.0.0.1"),
        )
        raise aiohttp.ClientResponseError(
            request_info=request_info,
            history=(),
            status=400,
            message="bad request",
        )

    peer_client._request_once = always_http_400
    req = RpcRequest.heartbeat(1, "LEADER", term=2, last_log_index=1, last_log_term=1, commit_index=1)
    res = await peer_client.send_rpc(req)

    assert res.is_err is True
    assert res.payload["category"] == "http_error"
    assert res.payload["attempt"] == 1


@pytest.mark.asyncio
async def test_send_rpc_decode_error_returns_structured_response(peer_client):
    async def bad_json(_endpoint, params=None, payload=None):
        raise ValueError("not json")

    peer_client._request_once = bad_json
    req = RpcRequest.heartbeat(1, "LEADER", term=2, last_log_index=1, last_log_term=1, commit_index=1)
    res = await peer_client.send_rpc(req)

    assert res.is_err is True
    assert res.payload["category"] == "decode_error"


@pytest.mark.asyncio
async def test_send_rpc_unexpected_error_returns_structured_response(peer_client):
    async def boom(_endpoint, params=None, payload=None):
        raise RuntimeError("unexpected")

    peer_client._request_once = boom
    req = RpcRequest.heartbeat(1, "LEADER", term=2, last_log_index=1, last_log_term=1, commit_index=1)
    res = await peer_client.send_rpc(req)

    assert res.is_err is True
    assert res.payload["category"] == "unexpected_error"


@pytest.mark.asyncio
async def test_ping_calls_send_rpc(peer_client, monkeypatch):
    expected = RpcResponse.ok(peer_client.id, peer_client.role, {"ok": True})
    send_rpc = AsyncMock(return_value=expected)
    monkeypatch.setattr(peer_client, "send_rpc", send_rpc)

    res = await peer_client.ping()

    assert res is expected
    send_rpc.assert_awaited_once()
