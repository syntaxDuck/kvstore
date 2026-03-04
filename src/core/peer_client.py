import asyncio

from .logging import get_logger
from .network.protocol import Protocol
from .types import NodeDetails, RpcRequest, RpcResponse

logger = get_logger(__name__)


class PeerClient:
    def __init__(
        self, peer_details: NodeDetails, protocol: Protocol = Protocol()
    ) -> None:
        self.node_id = peer_details.id
        self.node_role = peer_details.role
        self.address = (peer_details.host, peer_details.port)
        self._protocol = protocol

    @property
    def id(self):
        return self.node_id

    @property
    def role(self):
        return self.node_role

    async def send_rpc(self, message: RpcRequest) -> RpcResponse:
        logger.debug(f"Sending {message.type} to peer {self.id} at {self.address}")
        try:
            reader, writer = await asyncio.open_connection(
                self.address[0], self.address[1]
            )

            await self._protocol.send_message(writer, message.model_dump())
            response = await self._protocol.recv_message(reader)

            writer.close()
            await writer.wait_closed()

            logger.debug(
                f"Received response from peer {self.id}: {response.get('status')}"
            )
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
