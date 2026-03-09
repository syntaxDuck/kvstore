import tempfile
from pathlib import Path

from src.core.raft.key_value_store import KeyValueStore
from src.core.raft.log import WriteAheadLog
from src.core.raft.node import Node
from src.core.types import Command


def _append_commands(log: WriteAheadLog, commands: list[Command], term: int = 1) -> None:
    for cmd in commands:
        log.append(term, cmd)


def _make_node_view(node_id: int, log: WriteAheadLog, commit_index: int, last_applied: int):
    node = Node.__new__(Node)
    node.id = node_id
    node.log = log
    node.store = KeyValueStore()
    node.commit_index = commit_index
    node.last_applied = last_applied
    return node


def test_restarted_follower_catches_up_to_committed_index():
    commands = [
        Command(op="SET", key="alpha", val=1),
        Command(op="SET", key="beta", val=2),
        Command(op="DELETE", key="alpha", val=None),
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir)

        initial_log = WriteAheadLog(name="follower.log", path=str(log_path))
        _append_commands(initial_log, commands)
        initial_log.close()

        restarted_log = WriteAheadLog(name="follower.log", path=str(log_path))
        restarted_follower = _make_node_view(
            node_id=2,
            log=restarted_log,
            commit_index=3,
            last_applied=0,
        )

        Node._apply_committed(restarted_follower)

        assert restarted_follower.last_applied == 3
        assert restarted_follower.store.value_store == {"beta": 2}
        restarted_log.close()


def test_rejoin_apply_keeps_commit_and_applied_invariants():
    commands = [
        Command(op="SET", key="x", val=10),
        Command(op="SET", key="y", val=20),
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        log = WriteAheadLog(name="rejoin.log", path=tmpdir)
        _append_commands(log, commands)

        follower = _make_node_view(node_id=3, log=log, commit_index=1, last_applied=1)

        # Rejoin path: follower learns of a higher commit index from the leader.
        follower.commit_index = 2
        Node._apply_committed(follower)

        assert follower.last_applied == 2
        assert follower.last_applied <= follower.commit_index

        # Re-applying with no new commits remains stable.
        Node._apply_committed(follower)
        assert follower.last_applied == 2
        assert follower.commit_index == 2
        log.close()


def test_committed_state_matches_between_leader_and_rejoined_follower():
    commands = [
        Command(op="SET", key="k1", val="v1"),
        Command(op="SET", key="k2", val="v2"),
        Command(op="DELETE", key="k1", val=None),
    ]

    with tempfile.TemporaryDirectory() as leader_dir, tempfile.TemporaryDirectory() as follower_dir:
        leader_log = WriteAheadLog(name="node.log", path=leader_dir)
        follower_log = WriteAheadLog(name="node.log", path=follower_dir)

        _append_commands(leader_log, commands)
        _append_commands(follower_log, commands)

        leader = _make_node_view(node_id=1, log=leader_log, commit_index=3, last_applied=0)
        follower = _make_node_view(node_id=2, log=follower_log, commit_index=3, last_applied=0)

        Node._apply_committed(leader)
        Node._apply_committed(follower)

        assert leader.store.value_store == follower.store.value_store
        assert leader.last_applied == follower.last_applied == 3

        leader_log.close()
        follower_log.close()
