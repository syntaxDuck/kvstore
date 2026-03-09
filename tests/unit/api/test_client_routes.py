from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from src.api.routes.client import KvSetRequest, delete_value, set_value
from src.api.schemas.client import KvSetRequest as SchemaKvSetRequest
from src.core.types import RpcResponse


def _request_for(node):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(node=node)))


def _leader_node():
    node = SimpleNamespace()
    node.id = 1
    node.role = "LEADER"
    node.is_leader = True
    node.leader_address = ("127.0.0.1", 8080)
    node.role_state = SimpleNamespace(term=3, role="LEADER")
    node.log = SimpleNamespace(
        details=SimpleNamespace(index=4, term=3),
        append=Mock(side_effect=lambda _term, _cmd: setattr(node.log.details, "index", node.log.details.index + 1)),
    )
    node.store = SimpleNamespace(value_store={})
    node.peers = [SimpleNamespace(id=2), SimpleNamespace(id=3)]
    node.match_index = {}
    node.send_to_all_peers = AsyncMock(
        return_value=[
            RpcResponse.ack(2, "FOLLOWER"),
            RpcResponse.err(3, "FOLLOWER", {"Error": "down"}),
        ]
    )
    node._quorum_size = Mock(return_value=2)
    node._has_majority = Mock(side_effect=lambda follower_acks: follower_acks >= 1)
    node.update_match_index = Mock()
    node._update_commit_index = Mock()
    return node


def test_kv_set_request_route_reexport_matches_schema_type():
    assert KvSetRequest is SchemaKvSetRequest


@pytest.mark.asyncio
async def test_set_value_non_leader_returns_409_with_leader_info():
    node = SimpleNamespace(
        id=2,
        is_leader=False,
        role_state=SimpleNamespace(role="FOLLOWER"),
        leader_address=("127.0.0.1", 8080),
    )

    with pytest.raises(HTTPException) as exc:
        await set_value(_request_for(node), KvSetRequest(key="k", val="v"))

    assert exc.value.status_code == 409
    assert exc.value.detail["error"] == "not_leader"
    assert exc.value.detail["leader_address"]["port"] == 8080


@pytest.mark.asyncio
async def test_set_value_replicates_via_peer_rpc_and_commits_on_majority():
    node = _leader_node()

    payload = await set_value(_request_for(node), KvSetRequest(key="k", val="v"))

    assert payload["status"] == "ok"
    node.send_to_all_peers.assert_awaited_once()
    node.log.append.assert_called_once()
    node.update_match_index.assert_called_once_with(2, 5)
    node._update_commit_index.assert_called_once()


@pytest.mark.asyncio
async def test_set_value_returns_500_without_majority():
    node = _leader_node()
    node._has_majority = Mock(return_value=False)

    with pytest.raises(HTTPException) as exc:
        await set_value(_request_for(node), KvSetRequest(key="k", val="v"))

    assert exc.value.status_code == 500
    node.log.append.assert_not_called()


@pytest.mark.asyncio
async def test_delete_value_single_node_commits_without_peer_rpc():
    node = _leader_node()
    node.peers = []

    payload = await delete_value(_request_for(node), "k")

    assert payload["status"] == "ok"
    node.send_to_all_peers.assert_not_called()
    node.log.append.assert_called_once()
