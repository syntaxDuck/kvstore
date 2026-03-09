from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from src.api.routes.internal import ready_check
from src.core.raft.role_state import Role


class FakePeer:
    def __init__(self, peer_id: int, is_ok: bool):
        self.id = peer_id
        self._is_ok = is_ok

    async def ping(self):
        if self._is_ok:
            return SimpleNamespace(is_ok=True, payload={"status": "ok"})
        return SimpleNamespace(is_ok=False, payload={"error": "unreachable"})


def _request_for(node):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(node=node)))


@pytest.mark.asyncio
async def test_ready_returns_503_when_node_missing():
    request = _request_for(None)

    with pytest.raises(HTTPException) as exc:
        await ready_check(request)

    assert exc.value.status_code == 503
    assert exc.value.detail == "Not ready"


@pytest.mark.asyncio
async def test_ready_leader_with_quorum_reachable():
    node = SimpleNamespace(
        role=Role.LEADER,
        term=3,
        is_leader=True,
        commit_index=2,
        last_applied=2,
        log=SimpleNamespace(details=SimpleNamespace(index=2)),
        peers=[FakePeer(2, True), FakePeer(3, False)],
    )

    payload = await ready_check(_request_for(node))

    assert payload["status"] == "ready"
    assert payload["reachable_peers"] == 1


@pytest.mark.asyncio
async def test_ready_leader_without_quorum_returns_503():
    node = SimpleNamespace(
        role=Role.LEADER,
        term=3,
        is_leader=True,
        commit_index=2,
        last_applied=1,
        log=SimpleNamespace(details=SimpleNamespace(index=2)),
        peers=[FakePeer(2, False), FakePeer(3, False)],
    )

    with pytest.raises(HTTPException) as exc:
        await ready_check(_request_for(node))

    assert exc.value.status_code == 503
    issues = exc.value.detail["issues"]
    assert any("leader_without_quorum" in issue for issue in issues)


@pytest.mark.asyncio
async def test_ready_follower_without_reachable_peers_returns_503():
    node = SimpleNamespace(
        role=Role.FOLLOWER,
        term=3,
        is_leader=False,
        commit_index=2,
        last_applied=1,
        log=SimpleNamespace(details=SimpleNamespace(index=2)),
        peers=[FakePeer(2, False)],
    )

    with pytest.raises(HTTPException) as exc:
        await ready_check(_request_for(node))

    assert exc.value.status_code == 503
    assert "no_reachable_peers" in exc.value.detail["issues"]


@pytest.mark.asyncio
async def test_ready_rejects_invalid_raft_state():
    node = SimpleNamespace(
        role=Role.FOLLOWER,
        term=3,
        is_leader=False,
        commit_index=5,
        last_applied=1,
        log=SimpleNamespace(details=SimpleNamespace(index=2)),
        peers=[FakePeer(2, True)],
    )

    with pytest.raises(HTTPException) as exc:
        await ready_check(_request_for(node))

    assert exc.value.status_code == 503
    assert "commit_index_ahead_of_log" in exc.value.detail["issues"]
