import asyncio

from .logging import get_logger
from .protocol import Protocol
from .rpc import RpcRequest, RpcResponse
from .types import NodeDetails

logger = get_logger(__name__)


class PeerClient:
    def __init__(self, protocol: Protocol, peer_details: NodeDetails) -> None:
        self.id, self.role, self.host, self.port = peer_details.model_dump().values()
        self.address = peer_details.address
        self._protocol = protocol

    async def send_rpc(self, message: RpcRequest) -> RpcResponse:
        try:
            logger.debug(f"Sending RPC to peer: {self.id} at address: {self.address}")
            reader, writer = await asyncio.open_connection(
                self.address[0], self.address[1]
            )
            await self._protocol.send_message(writer, message.model_dump())
            response = await self._protocol.recv_message(reader)
            logger.debug(f"RPC Response: {response}")

            writer.close()
            await writer.wait_closed()

            return RpcResponse(**response)
        except Exception as e:
            logger.error(f"RPC failed to peer {self.id}: {e}")
            raise
