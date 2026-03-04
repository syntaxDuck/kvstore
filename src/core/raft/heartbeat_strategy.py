from asyncio import sleep
from typing import Any, Coroutine
from ..util.timer import TimerStrategy
from ..raft.node_interface import NodeInterface
from ..types import RpcRequest
from ..logging import get_logger

logger = get_logger(__name__)


class HeartbeatStrategy(TimerStrategy):
    def __init__(self, node: NodeInterface) -> None:
        super().__init__()
        self._node = node
        self._running = False

    def start(self) -> Coroutine[Any, Any, None]:
        self._running = True
        return self._heartbeat_loop()

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                await self._node.send_to_all_peers(
                    RpcRequest.ping(
                        self._node.id,
                        self._node.role,
                        self._node.term,
                        self._node.log_details.index,
                        self._node.log_details.term,
                    )
                )
            except ValueError:
                logger.warning("Cannot send heartbeat, no peers registered")
                pass
            await sleep(self._node.role_state.heartbeat_timeout)

    def stop(self) -> None:
        self._running = False

    def cancel(self) -> None:
        self.stop()

    def reset(self) -> None:
        pass
