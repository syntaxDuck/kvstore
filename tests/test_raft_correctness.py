from types import SimpleNamespace

import pytest

from src.api.routes.internal import append_entries
from src.core.raft.node import Node
from src.core.raft.role_state import Role
from src.core.types import Command, RpcRequest


@pytest.mark.asyncio
async def test_handle_append_entry_does_not_apply_before_commit():
    node = Node.__new__(Node)
    node.id = 2
    node.role_state = SimpleNamespace(term=3, role=Role.FOLLOWER, become_follower=lambda _: None)
    node.election_task = SimpleNamespace(reset=lambda: None)

    applied = []
    appended = []
    node.store = SimpleNamespace(apply=lambda cmd: applied.append(cmd))
    node.log = SimpleNamespace(append=lambda term, cmd: appended.append((term, cmd)))

    req = RpcRequest.append_entry(
        node_id=1,
        node_role=Role.LEADER,
        term=3,
        last_log_index=0,
        last_log_term=0,
        cmd=Command(op="SET", key="k", val="v"),
    )

    res = await Node._handle_append_entry(node, req)

    assert res.is_ack is True
    assert len(appended) == 1
    assert applied == []


def test_quorum_helper_requires_true_majority_for_even_clusters():
    node = Node.__new__(Node)
    node.peers = [object(), object(), object()]  # 4 total nodes

    assert node._quorum_size() == 3
    assert node._has_majority(1) is False  # leader + 1 = 2/4
    assert node._has_majority(2) is True  # leader + 2 = 3/4


def test_commit_index_advances_when_index_replicated_by_quorum():
    node = Node.__new__(Node)
    node.id = 1
    node.peers = [object(), object(), object()]  # 4 total nodes
    node.role_state = SimpleNamespace(role=Role.LEADER)
    node.log = SimpleNamespace(details=SimpleNamespace(index=6))
    node.match_index = {2: 6, 3: 6, 4: 2}
    node.commit_index = 0

    applied = []
    node._apply_committed = lambda: applied.append(node.commit_index)

    Node._update_commit_index(node)

    assert node.commit_index == 6
    assert applied == [6]


@pytest.mark.asyncio
async def test_append_endpoint_rejects_prev_log_mismatch():
    fake_node = SimpleNamespace(
        role_state=SimpleNamespace(term=5, become_follower=lambda _: None),
        log=SimpleNamespace(
            details=SimpleNamespace(index=7),
            replay_log_from=lambda _: None,
            append=lambda term, cmd: None,
        ),
        election_task=SimpleNamespace(reset=lambda: None),
        id=2,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(node=fake_node)))

    response = await append_entries(
        request=request,
        leader_id=1,
        term=5,
        prev_log_index=7,
        prev_log_term=5,
        entries=[{"op": "SET", "key": "x", "val": 1}],
    )

    assert response["success"] is False
    assert response["last_log_index"] == 7
    assert response["peer_id"] == 2
