import asyncio
import random

from ..exceptions import LogError
from ..logging import get_logger
from ..network.protocol import Protocol
from ..network.rpc import RpcDipatcher
from ..peer_client import PeerClient
from ..types import (
    Command,
    LogDetails,
    NodeDetails,
    RpcRequest,
    RpcResponse,
)
from ..util.timer import TimerTask
from .election_strategy import ElectionStrategy
from .heartbeat_strategy import HeartbeatStrategy
from .key_value_store import KeyValueStore
from .log import WriteAheadLog
from .role_state import Role, RoleState

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

        self.election_task = TimerTask(ElectionStrategy(self))
        self.heartbeat_task = TimerTask(HeartbeatStrategy(self))

        self.role_state._on_become_leader = self._become_leader
        self.role_state._on_become_follower = self._step_down
        self.role_state._on_become_candidate = self._step_down

        self.log = WriteAheadLog(f"kvs_store_{self.id}")
        self.store = KeyValueStore()
        self.protocol = Protocol()

        self.rpc = RpcDipatcher()
        self.rpc.register("PING", self._handle_ping)
        self.rpc.register("CLIENT_WRITE", self._handle_client_write)
        self.rpc.register("CLIENT_GET", self._handle_client_get)
        self.rpc.register("APPEND_ENTRY", self._handle_append_entry)
        self.rpc.register("REQUEST_VOTE", self._handle_vote_request)

        self.peers: list[PeerClient] = []

        logger.info(f"Node initialized: {self.details}")

    # STATE CHANGE METHODS
    ############################################################################
    def _become_leader(self) -> None:
        self.election_task.stop()
        self.heartbeat_task.start()

    def _step_down(self) -> None:
        self.heartbeat_task.stop()
        self.election_task.start()

    # RPC HANDLER METHODS
    ############################################################################
    async def _handle_ping(self, req: RpcRequest) -> RpcResponse:
        if req.term > self.role_state.term:
            self.role_state.become_follower(req.term)
        self.election_task.reset()
        return RpcResponse.ok(self.id, self.role, {"Success": "Ping success"})

    async def _handle_client_get(self, req: RpcRequest) -> RpcResponse:
        if not req.payload:
            return RpcResponse.err(self.id, self.role, {"Error": "No command provided"})

        cmd = Command(**req.payload) if isinstance(req.payload, dict) else req.payload
        val = self.store.value_store.get(cmd.key)
        return RpcResponse.ok(self.id, self.role, {"val": val})

    async def _handle_client_write(self, req: RpcRequest) -> RpcResponse:
        if not self.is_leader:
            return RpcResponse.err(
                self.id,
                self.role,
                {
                    "Error": f"Sent CLIENT WRITE command to follower node, should send to Leader node at {self.leader_address}"
                },
            )

        if not req.payload:
            return RpcResponse.err(self.id, self.role, {"Error": "No command provided"})

        cmd = Command(**req.payload) if isinstance(req.payload, dict) else req.payload

        responses = await self.send_to_all_peers(
            RpcRequest.append_entry(
                self.id,
                self.role,
                self.role_state.term,
                self.log.details.index,
                self.log.details.term,
                cmd,
            )
        )

        count = 0
        for res in responses:
            if res.status == "ACK":
                count += 1

        if count > len(self.peers) // 2:
            self.log.append(self.role_state.term, cmd)
            self.store.apply(cmd)
            logger.debug(f"{self.store.value_store}")
            return RpcResponse.ok(
                self.id, self.role, {"Success": "Client Write Success"}
            )

        logger.debug("Less than half of the peers acknolaged")
        return RpcResponse.err(
            self.id, self.role, {"Error": "Majority of peers did not acknolage"}
        )

    async def _handle_append_entry(self, req: RpcRequest) -> RpcResponse:
        if not req.payload:
            return RpcResponse.err(self.id, self.role, {"Error": "No command provided"})
        if req.term < self.role_state.term:
            return RpcResponse.err(
                self.id,
                self.role,
                {"Error": f"Stale leader term, {req.term}<{self.role_state.term}"},
            )
        if req.term > self.role_state.term:
            self.role_state.become_follower(req.term)
        logger.debug(f"AppendEntry from leader {req.node_id}, term {req.term}")
        cmd = Command(**req.payload) if isinstance(req.payload, dict) else req.payload
        self.log.append(req.term, cmd)
        self.store.apply(cmd)
        logger.debug(f"{self.store.value_store}")
        return RpcResponse.ack(self.id, self.role)

    async def _handle_vote_request(self, req: RpcRequest) -> RpcResponse:
        try:
            if req.term > self.role_state.term:
                self.role_state.become_follower(req.term)
            logger.debug(f"Vote request from node {req.node_id}, term {req.term}")
            vote = self.get_vote_decision(
                req.node_id, req.term, req.last_log_index, req.last_log_term
            )
            logger.debug(f"Vote decision for node {req.node_id}: {vote}")
            self.role_state.voted_for = req.node_id if vote else None
            return RpcResponse.vote_response(self.id, self.role, vote)
        except LogError:
            return RpcResponse.err(
                self.id, self.role, {"Error": "Issue validating log"}
            )

    # SERVER
    ########################################################################
    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            msg = await self.protocol.recv_message(reader)
            req = RpcRequest(**msg)

            if req.node_role == Role.LEADER and self.log.details.term != req.term:
                self.role_state.voted_for = None
            res = await self.rpc.dispatch(req)
            await self.protocol.send_message(writer, res.model_dump())

        except ConnectionError as e:
            logger.warning(f"Connection error handling request: {e}")
            res = RpcResponse.err(self.id, self.role, {"Error": "Connection error"})
            await self.protocol.send_message(writer, res.model_dump())
        except ValueError as e:
            logger.warning(f"Invalid request: {e}")
            res = RpcResponse.err(self.id, self.role, {"Error": str(e)})
            await self.protocol.send_message(writer, res.model_dump())
        except Exception as e:
            logger.error(f"Unexpected error handling connection: {e}")
            res = RpcResponse.err(
                self.id, self.role, {"Error": "Internal server error"}
            )
            await self.protocol.send_message(writer, res.model_dump())
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self) -> None:
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        logger.info(f"Node {self.id} listening on {self.host}:{self.port}")

        self.election_task.start()
        async with server:
            await server.serve_forever()

    # PEER METHODS
    ############################################################################
    async def send_to_all_peers(self, request: RpcRequest) -> list[RpcResponse]:
        if len(self.peers) == 0:
            raise ValueError("No peers registered")

        responses = []
        for peer in self.peers:
            try:
                responses.append(await peer.send_rpc(request))
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.warning(f"Failed to send rpc to peer {peer.id}: {e}")
                responses.append(RpcResponse.err(peer.id, peer.role, {"Error": str(e)}))

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
        res = await peer.send_rpc(
            RpcRequest.ping(
                self.id,
                self.role,
                self.role_state.term,
                self.log.details.index,
                self.log.details.term,
            )
        )
        if res.is_ok:
            logger.info(f"Successfully registered peer: {peer_details.address}")
            self.peers.append(peer)
            return

        logger.warning(f"Failed to register peer: {peer}")

    # Utility
    ###########################################################################
    def get_vote_decision(
        self,
        candidate_id: int,
        candidate_term: int,
        candidate_last_log_index: int,
        candidate_last_log_term: int,
    ) -> bool:
        if candidate_term < self.log.details.term:
            return False

        if self.role_state.voted_for and self.role_state.voted_for != candidate_id:
            return False

        if candidate_last_log_term < self.log.details.term:
            return False
        if (
            candidate_last_log_term == self.log.details.term
            and candidate_last_log_index < self.log.details.index
        ):
            return False

        return True

    # CLASS PROPS
    ############################################################################
    @property
    def details(self) -> NodeDetails:
        return NodeDetails(
            id=self.id,
            role=self.role_state.role,
            host=self.host,
            port=self.port,
        )

    @property
    def log_details(self) -> LogDetails:
        return self.log.details

    @property
    def role(self) -> str:
        return self.role_state.role

    @property
    def term(self) -> int:
        return self.role_state.term

    @property
    def is_leader(self) -> bool:
        return self.role_state.role == Role.LEADER

    @property
    def is_candidate(self) -> bool:
        return self.role_state.role == Role.CANDIDATE

    @property
    def is_follower(self) -> bool:
        return self.role_state.role == Role.FOLLOWER

    @property
    def address(self) -> tuple[str, int]:
        return (self.host, self.port)

    @property
    def leader_address(self) -> tuple[str, int] | None:
        for peer in self.peers:
            if peer.role == Role.LEADER:
                return peer.address
        return None
