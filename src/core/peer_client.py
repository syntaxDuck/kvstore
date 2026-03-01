import asyncio

from .logging import get_logger
from .protocol import Protocol
from .types import RpcRequest, RpcResponse, NodeDetails


logger = get_logger(__name__)


class PeerClient:
    def __init__(
        self, peer_details: NodeDetails, protocol: Protocol = Protocol()
    ) -> None:
        self.details: NodeDetails = peer_details
        self.address = peer_details.address
        self._protocol = protocol

    @property
    def id(self):
        return self.details.id

    @property
    def role(self):
        return self.details.role

    async def send_rpc(self, message: RpcRequest) -> RpcResponse:
        try:
            reader, writer = await asyncio.open_connection(
                self.address[0], self.address[1]
            )

            await self._protocol.send_message(writer, message.model_dump())
            response = await self._protocol.recv_message(reader)

            writer.close()
            await writer.wait_closed()

            return RpcResponse(**response)
        except ConnectionRefusedError:
            logger.error(f"Connection refused to peer {self.id}")
            raise
        except asyncio.TimeoutError:
            logger.error(f"Timeout connecting to peer {self.id}")
            raise
        except OSError as e:
            logger.error(f"Network error communicating with peer {self.id}: {e}")
            raise
