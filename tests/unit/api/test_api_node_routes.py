from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from src.api.routes.node import NodeSetRequest, get_state, get_value, set_value


def _make_node_stub():
    return SimpleNamespace(
        id=1,
        role="FOLLOWER",
        term=2,
        is_leader=False,
        commit_index=3,
        last_applied=2,
        peers=[object(), object()],
        log=SimpleNamespace(details=SimpleNamespace(index=5, term=2)),
    )


@pytest.mark.asyncio
async def test_node_set_is_deprecated_endpoint():
    with pytest.raises(HTTPException) as exc:
        await set_value(None, NodeSetRequest(key="a", val=1))

    assert exc.value.status_code == 410
    assert "Use POST /client/v1/kv" in exc.value.detail


@pytest.mark.asyncio
async def test_node_get_is_deprecated_endpoint():
    with pytest.raises(HTTPException) as exc:
        await get_value(None, "a")

    assert exc.value.status_code == 410
    assert "Use GET /client/v1/kv/{key}" in exc.value.detail


@pytest.mark.asyncio
async def test_node_state_returns_runtime_snapshot():
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(node=_make_node_stub())))

    payload = await get_state(request)

    assert payload["node_id"] == 1
    assert payload["role"] == "FOLLOWER"
    assert payload["term"] == 2
    assert payload["is_leader"] is False
    assert payload["commit_index"] == 3
    assert payload["last_applied"] == 2
    assert payload["peer_count"] == 2
    assert payload["log_index"] == 5
    assert payload["log_term"] == 2


@pytest.mark.asyncio
async def test_node_state_requires_initialized_node():
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(node=None)))

    with pytest.raises(HTTPException) as exc:
        await get_state(request)

    assert exc.value.status_code == 503
    assert exc.value.detail == "Node not initialized"
