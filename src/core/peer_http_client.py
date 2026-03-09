import aiohttp

from .logging import get_logger
from .types import NodeDetails, RpcRequest, RpcResponse

logger = get_logger(__name__)


class PeerHttpClient:
    def __init__(self, peer_details: NodeDetails) -> None:
        self.node_id = peer_details.id
        self.node_role = peer_details.role
        self.host = peer_details.host
        self.port = peer_details.port
        self.base_url = f"http://{self.host}:{self.port}"
        self.address = (peer_details.host, peer_details.port)

    @property
    def id(self):
        return self.node_id

    @property
    def role(self):
        return self.node_role

    async def send_rpc(self, message: RpcRequest) -> RpcResponse:
        logger.debug(f"Sending {message.type} to peer {self.id} at {self.base_url}")

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

        try:
            async with aiohttp.ClientSession() as session:
                if message.type == "PING":
                    async with session.post(f"{self.base_url}{endpoint}") as resp:
                        data = await resp.json()
                        return RpcResponse.ok(self.id, self.role, data)

                elif message.type == "REQUEST_VOTE":
                    params = {
                        "candidate_id": message.node_id,
                        "term": message.term,
                        "last_log_index": message.last_log_index,
                        "last_log_term": message.last_log_term,
                    }
                    async with session.post(
                        f"{self.base_url}{endpoint}", params=params
                    ) as resp:
                        data = await resp.json()
                        return RpcResponse.vote_response(
                            self.id, self.role, data.get("vote", False)
                        )

                elif message.type == "APPEND_ENTRY":
                    payload = message.payload
                    if payload and hasattr(payload, "model_dump"):
                        payload = payload.model_dump()
                    params = {
                        "leader_id": message.node_id,
                        "term": message.term,
                        "prev_log_index": message.last_log_index,
                        "prev_log_term": message.last_log_term,
                    }
                    if payload:
                        async with session.post(
                            f"{self.base_url}{endpoint}", params=params, json=[payload]
                        ) as resp:
                            data = await resp.json()
                            if data.get("success"):
                                return RpcResponse.ack(self.id, self.role)
                            return RpcResponse.err(self.id, self.role, data)
                    else:
                        async with session.post(
                            f"{self.base_url}{endpoint}", params=params
                        ) as resp:
                            data = await resp.json()
                            if data.get("success"):
                                return RpcResponse.ack(self.id, self.role)
                            return RpcResponse.err(self.id, self.role, data)

                elif message.type == "HEARTBEAT":
                    params = {
                        "leader_id": message.node_id,
                        "term": message.term,
                        "last_log_index": message.last_log_index,
                        "last_log_term": message.last_log_term,
                        "commit_index": getattr(message, "commit_index", 0),
                    }
                    async with session.post(
                        f"{self.base_url}{endpoint}", params=params
                    ) as resp:
                        data = await resp.json()
                        if data.get("success"):
                            return RpcResponse.ack(self.id, self.role)
                        return RpcResponse.err(self.id, self.role, data)

        except aiohttp.ClientError as e:
            logger.error(f"HTTP error communicating with peer {self.id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error communicating with peer {self.id}: {e}")
            raise

        raise ValueError(f"Unhandled message type: {message.type}")

    async def ping(self) -> RpcResponse:
        """Simple ping to check if peer is reachable."""
        message = RpcRequest.ping(self.id, self.role)
        return await self.send_rpc(message)
