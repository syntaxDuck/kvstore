from dataclasses import dataclass
from enum import Enum
from typing import Callable


class Role(str, Enum):
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    MASTER = "MASTER"


@dataclass
class RoleState:
    role: Role = Role.FOLLOWER
    term: int = 0
    voted_for: int | None = None
    election_timeout: float = 0.15
    heartbeat_timeout: float = 0.05

    _on_become_leader: Callable | None = None
    _on_become_follower: Callable | None = None
    _on_become_candidate: Callable | None = None

    def become_leader(self):
        self.role = Role.LEADER
        self.term += 1
        self.voted_for = None
        if self._on_become_leader:
            self._on_become_leader()

    def become_follower(self, term: int):
        self.role = Role.FOLLOWER
        if term > self.term:
            self.term = term
            self.voted_for = None
        if self._on_become_follower:
            self._on_become_follower()

    def become_candidate(self, node_id: int):
        self.role = Role.CANDIDATE
        self.term += 1
        self.voted_for = node_id
        if self._on_become_candidate:
            self._on_become_candidate()
