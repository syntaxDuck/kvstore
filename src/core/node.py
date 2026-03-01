import asyncio
import random

from .key_value_store import KeyValueStore
from .log import WriteAheadLog
from .logging import get_logger
from .peer_client import PeerClient
from .protocol import Protocol
from .role_state import Role, RoleState
from .rpc import RpcDipatcher
from .types import NodeDetails, RpcRequest, RpcResponse

logger = get_logger(__name__)

DEFAULT_ELECTION_TIMEOUT_MAX = 2.0
DEFAULT_ELECTION_TIMEOUT_MIN = 1.0
DEFAULT_HEARTBEAT_TIMEOUT = 0.1


class Node:
    def __init__(
        self,
        id: int,
        port: int = 0,
        host: str = "0.0.0.0",
    ) -> None:
        self.id = id
        self.port = port
        self.host = host
        self.role_state = RoleState(
            heartbeat_timeout=DEFAULT_HEARTBEAT_TIMEOUT,
            election_timeout=random.uniform(
                DEFAULT_ELECTION_TIMEOUT_MIN, DEFAULT_ELECTION_TIMEOUT_MAX
            ),
        )
        self.role_state._on_become_leader = self._start_heartbeat
        self.role_state._on_become_follower = self._stop_heartbeat
        self.role_state._on_become_candidate = self._stop_heartbeat

        self._heartbeat_task: asyncio.Task | None = None
        self._election_task: asyncio.Task | None = None
        self._election_reset_flag = False

        self.log = WriteAheadLog(f"kvs_store_{self.id}")
        self.store = KeyValueStore()
        self.protocol = Protocol()

        self.rpc = RpcDipatcher()
        self.rpc.register("PING", self._handle_ping)
        self.rpc.register("CLIENT_WRITE", self._handle_client_write)
        self.rpc.register("CLIENT_GET", self._handle_client_get)
        self.rpc.register("APPEND_ENTRY", self._handle_append_entry)
        self.commit_index = 0

        self.peers: list[PeerClient] = []

        logger.info(f"Node initialized: {self.details}")

    def _start_election_timer(self) -> None:
        self._election_task = asyncio.create_task(self._election_timeout_loop())

    async def _election_timeout_loop(self) -> None:
        while True:
            await asyncio.sleep(self.role_state.election_timeout)

            if self.is_leader:
                break

            if self._election_reset_flag:
                self._election_reset_flag = False
                continue

            await self._start_election()

    async def _start_election(self):
        logger.info(f"Node: {self.id}, Starting election...")
        if len(self.peers) != 0 and not self.leader_address:
            logger.info(f"Node: {self.id}, is almighty leader")
            self.role_state.become_leader()
            return

    def _start_heartbeat(self) -> None:
        if self._election_task:
            self._election_task.cancel()
            self._election_task = None
        self._heartbeat_task = asyncio.create_task(self._handle_heartbeat())

    async def _handle_heartbeat(self) -> None:
        while self.is_leader:
            try:
                await self.send_to_all_peers(RpcRequest.append_entry(self.details))
            except ValueError:
                logger.warning("Cannot send heartbeat, no peers registered")
                pass
            await asyncio.sleep(self.role_state.heartbeat_timeout)

    async def _stop_heartbeat(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    async def _handle_ping(self, _: RpcRequest) -> RpcResponse:
        logger.debug(f"Pinged node: {self.id}")
        return RpcResponse.ok(self.details, {"Success": "Ping success"})

    async def _handle_client_get(self, req: RpcRequest) -> RpcResponse:
        val = None
        if req.cmd:
            val = self.store.value_store.get(req.cmd.key)
        return RpcResponse.ok(self.details, {"val": val})

    async def _handle_client_write(self, req: RpcRequest) -> RpcResponse:
        if not self.is_leader:
            return RpcResponse.err(
                self.details,
                {
                    "Error": f"Sent CLIENT WRITE command to follower node, should send to Leader node at {self.leader_address}"
                },
            )

        if not req.cmd:
            logger.debug("No command provided")
            return RpcResponse.err(self.details)
        self.log.append(req.cmd)
        responses = await self.send_to_all_peers(
            RpcRequest.append_entry(self.details, req.cmd)
        )

        count = 0
        for res in responses:
            if res.status == "ACK":
                count += 1

        if count > len(self.peers) // 2:
            self.store.apply(req.cmd)
            logger.debug(f"{self.store.value_store}")
            return RpcResponse.ok(self.details, {"Success": "Client Write Success"})

        logger.debug("Less than half of the peers acknolaged")
        return RpcResponse.err(
            self.details, {"Error": "Majority of peers did not acknloage"}
        )

    async def _handle_append_entry(self, req: RpcRequest) -> RpcResponse:
        self._election_reset_flag = True
        if not req.cmd:
            return RpcResponse.ok(self.details, {"Success": "Append Entry Success"})
        logger.debug(f"Appending node: {self.id}")
        self.log.append(req.cmd)
        self.store.apply(req.cmd)
        logger.debug(f"{self.store.value_store}")
        return RpcResponse.ack(self.details)

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            msg = await self.protocol.recv_message(reader)
            res = await self.rpc.dispatch(RpcRequest(**msg))
            await self.protocol.send_message(writer, res.model_dump())

        except ConnectionError as e:
            logger.warning(f"Connection error handling request: {e}")
            res = RpcResponse.err(self.details, {"Error": "Connection error"})
            await self.protocol.send_message(writer, res.model_dump())
        except ValueError as e:
            logger.warning(f"Invalid request: {e}")
            res = RpcResponse.err(self.details, {"Error": str(e)})
            await self.protocol.send_message(writer, res.model_dump())
        except Exception as e:
            logger.error(f"Unexpected error handling connection: {e}")
            res = RpcResponse.err(self.details, {"Error": "Internal server error"})
            await self.protocol.send_message(writer, res.model_dump())
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self) -> None:
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        logger.info(f"Node {self.id} listening on {self.host}:{self.port}")

        self._start_election_timer()
        async with server:
            await server.serve_forever()

    async def send_to_all_peers(self, request: RpcRequest) -> list[RpcResponse]:
        if len(self.peers) == 0:
            raise ValueError("No peers registered")

        responses = []
        for peer in self.peers:
            try:
                responses.append(await peer.send_rpc(request))
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.warning(f"Failed to send rpc to peer {peer.id}: {e}")
                responses.append(RpcResponse.err(peer.details, {"Error": str(e)}))

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
        logger.info(f"Registering peer: {peer_details}")
        peer = PeerClient(peer_details)
        res = await peer.send_rpc(RpcRequest.ping(self.details))
        if res.is_ok:
            logger.info(f"Successfully registered peer: {peer_details.address}")
            self.peers.append(peer)
            return

        logger.warning(f"Failed to register peer: {peer}")

    @property
    def details(self) -> NodeDetails:
        return NodeDetails(
            id=self.id, role=self.role_state.role, host=self.host, port=self.port
        )

    @property
    def role(self) -> Role:
        return self.role_state.role

    @property
    def is_leader(self) -> bool:
        return self.role_state.role == Role.LEADER

    @property
    def address(self) -> tuple[str, int]:
        return (self.host, self.port)

    @property
    def leader_address(self) -> tuple[str, int] | None:
        for peer in self.peers:
            if peer.role == Role.LEADER:
                return peer.address
        return None
