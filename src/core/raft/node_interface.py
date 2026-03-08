from typing import Protocol

from ..types import RpcRequest, RpcResponse
from .log import LogDetails
from .role_state import RoleState


class NodeInterface(Protocol):
    @property
    def id(self) -> int: ...

    @property
    def term(self) -> int: ...

    @property
    def role(self) -> str: ...

    @property
    def is_leader(self) -> bool: ...

    @property
    def is_follower(self) -> bool: ...

    @property
    def is_candidate(self) -> bool: ...

    @property
    def role_state(self) -> RoleState: ...

    @property
    def log_details(self) -> LogDetails: ...

    @property
    def peers(self) -> list: ...

    async def send_to_all_peers(self, request: RpcRequest) -> list[RpcResponse]: ...
