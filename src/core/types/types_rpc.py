from typing import Any, Callable, Coroutine, TypeAlias

from pydantic import BaseModel

from . import Command


class RpcRequest(BaseModel):
    type: str
    node_id: int
    node_role: str
    term: int = 0
    last_log_index: int = 0
    last_log_term: int = 0
    commit_index: int = 0
    payload: Any | None = None

    @classmethod
    def ping(
        cls,
        node_id: int,
        node_role: str,
        term: int = 0,
        last_log_index: int = 0,
        last_log_term: int = 0,
    ):
        return cls(
            type="PING",
            node_id=node_id,
            node_role=node_role,
            term=term,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

    @classmethod
    def heartbeat(
        cls,
        node_id: int,
        node_role: str,
        term: int = 0,
        last_log_index: int = 0,
        last_log_term: int = 0,
        commit_index: int = 0,
    ):
        return cls(
            type="HEARTBEAT",
            node_id=node_id,
            node_role=node_role,
            term=term,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
            commit_index=commit_index,
        )

    @classmethod
    def request_vote(
        cls,
        node_id: int,
        node_role: str,
        term: int,
        last_log_index: int,
        last_log_term: int,
    ):
        return cls(
            type="REQUEST_VOTE",
            node_id=node_id,
            node_role=node_role,
            term=term,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

    @classmethod
    def client_write(
        cls,
        node_id: int,
        node_role: str,
        term: int = 0,
        last_log_index: int = 0,
        last_log_term: int = 0,
        cmd: Command | None = None,
    ):
        return cls(
            type="CLIENT_WRITE",
            node_id=node_id,
            node_role=node_role,
            term=term,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
            payload=cmd,
        )

    @classmethod
    def client_get(
        cls,
        node_id: int,
        node_role: str,
        term: int = 0,
        last_log_index: int = 0,
        last_log_term: int = 0,
        cmd: Command | None = None,
    ):
        return cls(
            type="CLIENT_GET",
            node_id=node_id,
            node_role=node_role,
            term=term,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
            payload=cmd,
        )

    @classmethod
    def append_entry(
        cls,
        node_id: int,
        node_role: str,
        term: int = 0,
        last_log_index: int = 0,
        last_log_term: int = 0,
        cmd: Command | None = None,
    ):
        return cls(
            type="APPEND_ENTRY",
            node_id=node_id,
            node_role=node_role,
            term=term,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
            payload=cmd,
        )


class RpcResponse(BaseModel):
    status: str
    node_id: int
    node_role: str
    payload: dict[str, Any] | None

    @classmethod
    def ok(cls, node_id: int, node_role: str, payload: dict | None = None):
        return cls(
            status="OK",
            node_id=node_id,
            node_role=node_role,
            payload=payload or {},
        )

    @classmethod
    def ack(cls, node_id: int, node_role: str):
        return cls(
            status="ACK",
            node_id=node_id,
            node_role=node_role,
            payload=None,
        )

    @classmethod
    def vote_response(cls, node_id: int, node_role: str, vote: bool):
        return cls.ok(node_id, node_role, {"vote": vote})

    @classmethod
    def err(cls, node_id: int, node_role: str, payload: Any = None):
        return cls(
            status="ERROR",
            node_id=node_id,
            node_role=node_role,
            payload=payload or {},
        )

    @property
    def is_ok(self):
        return self.status == "OK"

    @property
    def is_ack(self):
        return self.status == "ACK"

    @property
    def is_err(self):
        return self.status == "ERROR"


RpcCoroutine: TypeAlias = Callable[[RpcRequest], Coroutine[Any, Any, RpcResponse]]
