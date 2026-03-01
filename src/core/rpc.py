from .types import RpcRequest, RpcResponse, RpcCoroutine
from .logging import get_logger

logger = get_logger(__name__)


class RpcDipatcher:
    def __init__(self) -> None:
        self._handlers: dict[str, RpcCoroutine] = {}

    def register(self, cmd: str, handler: RpcCoroutine) -> None:
        self._handlers[cmd.upper()] = handler

    async def dispatch(self, req: RpcRequest) -> RpcResponse:
        handler = self._handlers.get(req.type.upper())
        if not handler:
            logger.error(f"Unknown command received: {req.type}")
            raise ValueError(f"Unknown command: {req.type}")
        return await handler(req)
