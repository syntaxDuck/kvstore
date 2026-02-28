import asyncio

from .key_value_store import KeyValueStore
from .log import WriteAheadLog
from .logging import get_logger
from .peer_client import PeerClient
from .protocol import Protocol
from .rpc import RpcDipatcher, RpcRequest, RpcResponse
from .types import NodeDetails

logger = get_logger(__name__)


class Node:
    def __init__(
        self,
        role: str,
        id: int,
        port: int = 0,
        host: str = "0.0.0.0",
    ) -> None:
        self.id = id
        self.role = role
        self.host = host
        self.port = port
        self.peers: list[PeerClient] = []
        self.log = WriteAheadLog(f"kvs_store_{self.id}")
        self.store = KeyValueStore()
        self.protocol = Protocol()

        self.rpc = RpcDipatcher()
        self.rpc.register("PING", self._handle_ping)
        self.rpc.register("CLIENT_WRITE", self._handle_client_write)
        self.rpc.register("APPEND_ENTRY", self._handle_append_entry)
        self.commit_index = 0

        logger.info(
            f"Node initialized: id={self.id} role={self.role} host={host} port={port}"
        )

    async def _handle_ping(self, _: RpcRequest) -> RpcResponse:
        logger.debug(f"Pinged node: {self.id}")
        return RpcResponse.ok(self.id)

    async def _handle_client_write(self, req: RpcRequest) -> RpcResponse:
        if not self.is_leader:
            return RpcResponse.err(
                self.id,
                {
                    "details": f"Sent CLIENT WRITE command to follower node, should send to Leader node at {self.leader_address}"
                },
            )

        if not req.cmd:
            logger.debug("No command provided")
            return RpcResponse.err(self.id)
        self.log.append(req.cmd)
        responses = await self.send_to_all_peers(RpcRequest.append_entry(req.cmd))

        count = 0
        for res in responses:
            if res.status == "ACK":
                count += 1

        if count > len(self.peers) // 2:
            self.store.apply(req.cmd)
            logger.debug(f"{self.store.value_store}")
            return RpcResponse.ok(self.id)

        logger.debug("Less than half of the peers acknolaged")
        return RpcResponse.err(payload={"Error": "Majority of peers did not acknloage"})

    async def _handle_append_entry(self, req: RpcRequest) -> RpcResponse:
        logger.debug(f"Appending node: {self.id}")
        if not req.cmd:
            return RpcResponse.err(self.id, {"Error": "No command provided"})
        self.log.append(req.cmd)
        self.store.apply(req.cmd)
        logger.debug(f"{self.store.value_store}")
        return RpcResponse.ack(self.id)

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            msg = await self.protocol.recv_message(reader)
            res = await self.rpc.dispatch(RpcRequest(**msg))
            await self.protocol.send_message(writer, res.model_dump())

        except ConnectionError as e:
            logger.warning(f"Connection error handling request: {e}")
            res = RpcResponse.err(self.id, {"Error": "Connection error"})
            await self.protocol.send_message(writer, res.model_dump())
        except ValueError as e:
            logger.warning(f"Invalid request: {e}")
            res = RpcResponse.err(self.id, {"Error": str(e)})
            await self.protocol.send_message(writer, res.model_dump())
        except Exception as e:
            logger.error(f"Unexpected error handling connection: {e}")
            res = RpcResponse.err(self.id, {"Error": "Internal server error"})
            await self.protocol.send_message(writer, res.model_dump())
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self) -> None:
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        logger.info(f"Node {self.id} listening on {self.host}:{self.port}")

        async with server:
            await server.serve_forever()

    async def send_to_all_peers(self, request: RpcRequest) -> list[RpcResponse]:
        if len(self.peers) == 0:
            logger.warning("Cannot send rpc, no peers registered")
            raise ValueError("No peers registered")

        responses = []
        for peer in self.peers:
            try:
                logger.debug(f"Sending rpc to node: {peer.id}")
                responses.append(await peer.send_rpc(request))
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.warning(f"Failed to send rpc to peer {peer.id}: {e}")
                responses.append(RpcResponse.err(peer.id, {"Error": str(e)}))

        return responses

    async def send_to_peer(self, peer_id: int, request: RpcRequest) -> RpcResponse:
        if len(self.peers) == 0:
            raise ValueError("No peers registered")

        for peer in self.peers:
            if peer.id == peer_id:
                return await peer.send_rpc(request)

        raise ValueError("Target Peer not registered")

    async def register_peers(self, peers: list[NodeDetails]) -> None:
        for peer in peers:
            if self.id == peer.id:
                continue
            await self.register_peer(peer)

    async def register_peer(self, peer_details: NodeDetails) -> None:
        logger.info(f"Registering peer to node: {peer_details}")
        peer = PeerClient(peer_details)
        res = await peer.send_rpc(RpcRequest.ping())
        if res.is_ok:
            logger.info(f"Successfully registered peer: {peer_details.address}")
            self.peers.append(peer)
            return

        logger.warning(f"Failed to register peer: {peer}")

    @property
    def is_leader(self):
        return self.role == "Leader"

    @property
    def details(self) -> NodeDetails:
        return NodeDetails(id=self.id, role=self.role, host=self.host, port=self.port)

    @property
    def address(self) -> tuple:
        return (self.host, self.port)

    @property
    def leader_address(self) -> tuple:
        for peer in self.peers:
            if peer.role == "Leader":
                return peer.address
        return ()
