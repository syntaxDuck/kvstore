from asyncio import sleep
from typing import Coroutine, Any
import time

from ..logging import get_logger
from ..types import RpcRequest
from ..util.timer import TimerStrategy
from .node_interface import NodeInterface

logger = get_logger(__name__)


class ElectionStrategy(TimerStrategy):
    def __init__(self, node: NodeInterface) -> None:
        super().__init__()
        self._node = node
        self._election_reset = False
        self._running = False

    def start(self) -> Coroutine[Any, Any, None]:
        self._running = True
        return self._election_loop()

    async def _election_loop(self):
        while self._running:
            await sleep(self._node.role_state.election_timeout)

            if not self._running:
                break

            if self._election_reset:
                self._election_reset = False
                continue

            if self._node.is_follower or self._node.is_candidate:
                await self._start_election()

    async def _start_election(self):
        logger.info(f"Node: {self._node.id}, Starting election...")

        self._node.role_state.become_candidate(self._node.id)

        if hasattr(self._node, "_election_start_time"):
            self._node._election_start_time = time.perf_counter()

        try:
            ballots = await self._node.send_to_all_peers(
                RpcRequest.request_vote(
                    self._node.id,
                    self._node.role,
                    self._node.term,
                    self._node.log_details.index,
                    self._node.log_details.term,
                )
            )
        except ValueError:
            return

        yeahs = 1  # candidates vote for themselves in Raft
        for ballot in ballots:
            logger.info(f"Election ballot for term {self._node.term}: {ballot}")
            if not ballot.is_ok or not ballot.payload:
                continue
            if ballot.payload.get("vote"):
                yeahs += 1

        total_nodes = len(self._node.peers) + 1
        quorum = total_nodes // 2 + 1
        if yeahs >= quorum:
            logger.info(f"Node: {self._node.id} is the leader")
            self._node.role_state.become_leader()

    def stop(self):
        self._running = False

    def cancel(self):
        self.stop()

    def reset(self):
        self._election_reset = True
