import asyncio
import functools
import random

from ..config import settings
from ..exceptions import LogError
from ..logging import get_logger
from ..peer_http_client import PeerHttpClient
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

ROLE_LABEL_KEY = "kvstore-role"
ROLE_LABEL_LEADER = "leader"
ROLE_LABEL_FOLLOWER = "follower"


try:
    from kubernetes import client as k8s_client, config as k8s_config
    from kubernetes.client import ApiException
except ImportError:  # pragma: no cover - optional dep
    k8s_client = None
    k8s_config = None
    ApiException = None

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
        data_dir: str = ".",
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

        self.pod_name = settings.POD_NAME
        self.namespace = settings.NAMESPACE
        self._k8s_api = None
        self._k8s_config_loaded = False

        self.election_task = TimerTask(ElectionStrategy(self))
        self.heartbeat_task = TimerTask(HeartbeatStrategy(self))

        self.role_state._on_become_leader = self._become_leader
        self.role_state._on_become_follower = self._step_down
        self.role_state._on_become_candidate = self._step_down

        self.log = WriteAheadLog(f"kvs_store_{self.id}", path=data_dir)
        self.store = KeyValueStore()

        self.peers: list[PeerHttpClient] = []

        logger.info(f"Node initialized: {self.details}")

        self._schedule_role_label(ROLE_LABEL_FOLLOWER)

        self.election_task.start()

    # STATE CHANGE METHODS
    ############################################################################
    def _become_leader(self) -> None:
        self.election_task.stop()
        self.heartbeat_task.start()

        self._schedule_role_label(ROLE_LABEL_LEADER)

    def _step_down(self) -> None:
        self.heartbeat_task.stop()
        self.election_task.start()

        self._schedule_role_label(ROLE_LABEL_FOLLOWER)

    # K8S
    ############################################################################
    def _schedule_role_label(self, label: str) -> None:
        if not self.pod_name:
            return
        try:
            asyncio.create_task(self._patch_role_label(label))
        except RuntimeError:
            logger.debug("Event loop unavailable; skipping label patch (%s)", label)

    async def _get_k8s_api(self):
        if (
            not self.pod_name
            or not self.namespace
            or k8s_client is None
            or k8s_config is None
        ):
            return None

        if self._k8s_api:
            return self._k8s_api

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, k8s_config.load_incluster_config)
            self._k8s_config_loaded = True
            self._k8s_api = k8s_client.CoreV1Api()
            return self._k8s_api
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning("Failed to load in-cluster config: %s", exc)
            return None

    async def _patch_role_label(self, label_value: str) -> None:
        api = await self._get_k8s_api()
        if api is None:
            return

        body = {"metadata": {"labels": {ROLE_LABEL_KEY: label_value}}}
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                functools.partial(
                    api.patch_namespaced_pod,
                    self.pod_name,
                    self.namespace,
                    body,
                ),
            )
            logger.info(
                "Patched pod %s label %s=%s",
                self.pod_name,
                ROLE_LABEL_KEY,
                label_value,
            )
        except Exception as exc:
            logger.warning("Unable to patch pod label: %s", exc)

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
                logger.warning(f"Failed to send rpc to peer {peer.id}: %s", e)
                responses.append(RpcResponse.err(peer.id, peer.role, {"Error": str(e)}))

        return responses

    async def send_to_all_peers_from_http(
        self, cmd: Command, term: int, last_log_index: int, last_log_term: int
    ) -> list[dict]:
        """Send command to all peers for replication via HTTP."""
        if len(self.peers) == 0:
            return []

        import aiohttp

        responses = []
        for peer in self.peers:
            try:
                async with aiohttp.ClientSession() as session:
                    params = {
                        "leader_id": self.id,
                        "term": term,
                        "prev_log_index": last_log_index,
                        "prev_log_term": last_log_term,
                    }
                    payload = cmd.model_dump() if hasattr(cmd, "model_dump") else cmd
                    async with session.post(
                        f"{peer.base_url}/internal/v1/append",
                        params=params,
                        json=[payload],
                    ) as resp:
                        data = await resp.json()
                        responses.append(data)
            except Exception as e:
                logger.warning(f"Failed to send to peer {peer.id}: %s", e)
                responses.append({"status": "ERROR", "Error": str(e)})

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
        peer = PeerHttpClient(peer_details)
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
