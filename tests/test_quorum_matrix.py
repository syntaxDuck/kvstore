from types import SimpleNamespace

import pytest

from src.core.raft.node import Node
from src.core.raft.role_state import Role


@pytest.mark.parametrize(
    "total_nodes,expected_quorum",
    [
        (3, 2),
        (4, 3),
        (5, 3),
    ],
)
def test_quorum_size_matrix(total_nodes: int, expected_quorum: int):
    node = Node.__new__(Node)
    node.peers = [object() for _ in range(total_nodes - 1)]

    assert node._quorum_size() == expected_quorum


@pytest.mark.parametrize(
    "total_nodes,follower_acks,expected",
    [
        # 3-node cluster (quorum=2): leader+0 is not enough, leader+1 is enough.
        (3, 0, False),
        (3, 1, True),
        # 4-node cluster (quorum=3): boundary is leader+2.
        (4, 1, False),
        (4, 2, True),
        # 5-node cluster (quorum=3): boundary is leader+2.
        (5, 1, False),
        (5, 2, True),
    ],
)
def test_majority_boundary_matrix(total_nodes: int, follower_acks: int, expected: bool):
    node = Node.__new__(Node)
    node.peers = [object() for _ in range(total_nodes - 1)]

    assert node._has_majority(follower_acks) is expected


@pytest.mark.parametrize(
    "total_nodes,peer_match_indexes,expected_commit",
    [
        # 3 nodes: commit at index=7 needs at least one follower at >=7.
        (3, {2: 7, 3: 2}, 7),
        # 4 nodes: commit at index=7 needs at least two followers at >=7.
        (4, {2: 7, 3: 7, 4: 2}, 7),
        # 5 nodes: commit at index=7 needs at least two followers at >=7.
        (5, {2: 7, 3: 7, 4: 1, 5: 2}, 7),
        # 5 nodes boundary fail for index=7: falls back to highest quorum index (=2).
        (5, {2: 7, 3: 2, 4: 1, 5: 2}, 2),
    ],
)
def test_commit_index_boundary_matrix(
    total_nodes: int,
    peer_match_indexes: dict[int, int],
    expected_commit: int,
):
    node = Node.__new__(Node)
    node.id = 1
    node.peers = [SimpleNamespace(id=i) for i in range(2, total_nodes + 1)]
    node.role_state = SimpleNamespace(role=Role.LEADER)
    node.log = SimpleNamespace(details=SimpleNamespace(index=7))
    node.match_index = dict(peer_match_indexes)
    node.commit_index = 0

    applied = []
    node._apply_committed = lambda: applied.append(node.commit_index)

    Node._update_commit_index(node)

    assert node.commit_index == expected_commit
    if expected_commit > 0:
        assert applied == [expected_commit]
    else:
        assert applied == []
