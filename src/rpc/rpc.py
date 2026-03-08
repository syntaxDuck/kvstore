from src.core.logging import get_logger
from src.core.types import RpcCoroutine, RpcRequest, RpcResponse

logger = get_logger(__name__)


class RpcDipatcher:
    def __init__(self) -> None:
        self._handlers: dict[str, RpcCoroutine] = {}

    def register(self, cmd: str, handler: RpcCoroutine) -> None:
        self._handlers[cmd.upper()] = handler

    async def dispatch(self, req: RpcRequest) -> RpcResponse:
        logger.debug(f"RPC dispatch: {req.type} from node {req.node_id}")
        handler = self._handlers.get(req.type.upper())
        if not handler:
            logger.error(f"Unknown command received: {req.type}")
            raise ValueError(f"Unknown command: {req.type}")
        result = await handler(req)
        logger.debug(f"RPC response: {req.type} -> {result.status}: {result.payload}")
        return result
