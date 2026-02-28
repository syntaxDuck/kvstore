from pydantic import BaseModel
from typing import Any, Callable, Coroutine, TypeAlias

from src.core.command import Command
from .logging import get_logger

logger = get_logger(__name__)


class RpcRequest(BaseModel):
    type: str
    cmd: Command | None

    @classmethod
    def ping(cls):
        return cls(type="PING", cmd=None)

    @classmethod
    def client_write(cls, cmd):
        return cls(type="CLIENT_WRITE", cmd=cmd)

    @classmethod
    def append_entry(cls, cmd):
        return cls(type="APPEND_ENTRY", cmd=cmd)


class RpcResponse(BaseModel):
    status: str
    node_id: int | None
    payload: dict[str, Any] | None

    @classmethod
    def ok(cls, node_id: int, payload: Any = None):
        return cls(status="OK", node_id=node_id, payload=payload or {})

    @classmethod
    def ack(cls, node_id: int):
        return cls(status="ACK", node_id=node_id, payload=None)

    @classmethod
    def err(cls, node_id: int | None = None, payload: Any = None):
        return cls(status="ERROR", node_id=node_id, payload=payload or {})

    @property
    def is_ok(self):
        return self.status == "OK"


RpcCoroutine: TypeAlias = Callable[[RpcRequest], Coroutine[Any, Any, RpcResponse]]


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
