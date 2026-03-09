import tempfile

import pytest

from src.core.config import settings
from src.core.raft.node import Node
from src.core.types import Command


@pytest.mark.asyncio
async def test_snapshot_policy_creates_snapshot_and_compacts_log(monkeypatch):
    monkeypatch.setattr(settings, "SNAPSHOT_ENABLED", True)
    monkeypatch.setattr(settings, "SNAPSHOT_THRESHOLD", 2)

    with tempfile.TemporaryDirectory() as data_dir:
        node = Node(id=1, port=8080, host="127.0.0.1", data_dir=data_dir)

        node.log.append(1, Command(op="SET", key="a", val=1))
        node.log.append(1, Command(op="SET", key="b", val=2))
        node.log.append(1, Command(op="SET", key="c", val=3))

        node.commit_index = 3
        node._apply_committed()

        snapshot = node.log.load_snapshot()
        assert snapshot is not None
        assert snapshot["last_included_index"] == 3
        assert snapshot["state"] == {"a": 1, "b": 2, "c": 3}

        remaining = list(node.log.replay_log())
        assert remaining == []

        await node.shutdown()


@pytest.mark.asyncio
async def test_startup_restores_from_snapshot_and_remaining_log(monkeypatch):
    monkeypatch.setattr(settings, "SNAPSHOT_ENABLED", False)

    with tempfile.TemporaryDirectory() as data_dir:
        node1 = Node(id=1, port=8080, host="127.0.0.1", data_dir=data_dir)
        node1.log.append(1, Command(op="SET", key="x", val=1))
        node1.log.append(1, Command(op="SET", key="y", val=2))

        node1.store.value_store = {"x": 1, "y": 2}
        node1.last_applied = 2
        node1.commit_index = 2
        node1.log.save_snapshot(2, 1, dict(node1.store.value_store))

        node1.log.append(1, Command(op="DELETE", key="x", val=None))
        await node1.shutdown()

        node2 = Node(id=1, port=8080, host="127.0.0.1", data_dir=data_dir)

        assert node2.store.value_store == {"y": 2}
        assert node2.last_applied == 3
        assert node2.commit_index == 3

        await node2.shutdown()


@pytest.mark.asyncio
async def test_compaction_preserves_entries_after_snapshot_index(monkeypatch):
    monkeypatch.setattr(settings, "SNAPSHOT_ENABLED", False)

    with tempfile.TemporaryDirectory() as data_dir:
        node = Node(id=1, port=8080, host="127.0.0.1", data_dir=data_dir)

        node.log.append(1, Command(op="SET", key="k1", val="v1"))
        node.log.append(1, Command(op="SET", key="k2", val="v2"))
        node.log.append(1, Command(op="SET", key="k3", val="v3"))

        node.log.save_snapshot(2, 1, {"k1": "v1", "k2": "v2"})
        node.log.compact_up_to(2)

        remaining = list(node.log.replay_log())
        assert len(remaining) == 1
        assert remaining[0].index == 3
        assert remaining[0].cmd.key == "k3"

        await node.shutdown()
