from pydantic import BaseModel
from typing import Any, Callable
from .logging import get_logger

logger = get_logger(__name__)


class RpcRequest(BaseModel):
    type: str
    payload: dict[str, Any]

    @classmethod
    def ping(cls):
        return cls(type="PING", payload={})

    @classmethod
    def client_write(cls):
        return cls(type="CLIENT_WRITE", payload={})

    @classmethod
    def APPEND_ENTRY(cls):
        return cls(type="APPEND_ENTRY", payload={})


class RpcResponse(BaseModel):
    status: str
    node: int | None
    payload: dict[str, Any]

    @classmethod
    def ok(cls, node, payload=None):
        return cls(status="OK", node=node, payload=payload or {})

    @classmethod
    def err(cls, node=None, payload=None):
        return cls(status="ERROR", node=node, payload=payload or {})

    @property
    def is_ok(self):
        return self.status == "OK"


class RpcDipatcher:
    def __init__(self) -> None:
        self._handlers: dict[str, Callable[[RpcRequest], RpcResponse]] = {}

    def register(self, cmd: str, handler: Callable[[RpcRequest], RpcResponse]) -> None:
        self._handlers[cmd.upper()] = handler

    def dispatch(self, req: RpcRequest) -> RpcResponse:
        handler = self._handlers.get(req.type.upper())
        if not handler:
            logger.error(f"Unknown command: {req.type}")
            raise ValueError(f"Unknown command: {req.type}")
        return handler(req)
