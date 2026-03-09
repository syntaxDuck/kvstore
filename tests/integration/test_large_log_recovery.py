import time
import tempfile

import pytest

from src.core.config import settings
from src.core.raft.node import Node
from src.core.types import Command


def _set_key(state: dict[str, int], key: str, value: int) -> None:
    state[key] = value


def _delete_key(state: dict[str, int], key: str) -> None:
    state.pop(key, None)


@pytest.mark.asyncio
async def test_large_history_recovery_correctness_and_timing_baseline(monkeypatch):
    monkeypatch.setattr(settings, "SNAPSHOT_ENABLED", False)

    total_entries = 3000
    timing_budget_sec = 5.0

    with tempfile.TemporaryDirectory() as data_dir:
        writer = Node(id=1, port=8080, host="127.0.0.1", data_dir=data_dir)

        for i in range(total_entries):
            writer.log.append(1, Command(op="SET", key=f"k{i}", val=i))

        writer.commit_index = total_entries
        writer._apply_committed()
        await writer.shutdown()

        start = time.perf_counter()
        recovered = Node(id=1, port=8080, host="127.0.0.1", data_dir=data_dir)
        recovery_sec = time.perf_counter() - start

        assert recovered.last_applied == total_entries
        assert recovered.commit_index == total_entries
        assert len(recovered.store.value_store) == total_entries
        assert recovered.store.value_store["k0"] == 0
        assert recovered.store.value_store[f"k{total_entries - 1}"] == total_entries - 1
        assert recovery_sec < timing_budget_sec, (
            f"Recovery took {recovery_sec:.3f}s for {total_entries} entries; "
            f"budget={timing_budget_sec:.1f}s"
        )

        # Printed for baseline capture in docs (`pytest -s`).
        print(
            f"RECOVERY_BASELINE_NO_SNAPSHOT entries={total_entries} seconds={recovery_sec:.3f}"
        )
        await recovered.shutdown()


@pytest.mark.asyncio
async def test_snapshot_plus_replay_restores_full_state_without_data_loss(monkeypatch):
    monkeypatch.setattr(settings, "SNAPSHOT_ENABLED", False)

    snapshot_entries = 2000
    tail_entries = 1200

    expected_state: dict[str, int] = {}

    with tempfile.TemporaryDirectory() as data_dir:
        node = Node(id=2, port=8081, host="127.0.0.1", data_dir=data_dir)

        for i in range(snapshot_entries):
            key = f"base_{i}"
            node.log.append(1, Command(op="SET", key=key, val=i))
            _set_key(expected_state, key, i)

        node.log.save_snapshot(snapshot_entries, 1, dict(expected_state))
        node.log.compact_up_to(snapshot_entries)

        for i in range(tail_entries):
            if i % 4 == 0:
                key = f"base_{i // 4}"
                val = 10_000 + i
                node.log.append(1, Command(op="SET", key=key, val=val))
                _set_key(expected_state, key, val)
            elif i % 4 == 1:
                key = f"base_{i // 4}"
                node.log.append(1, Command(op="DELETE", key=key, val=None))
                _delete_key(expected_state, key)
            else:
                key = f"tail_{i}"
                val = 20_000 + i
                node.log.append(1, Command(op="SET", key=key, val=val))
                _set_key(expected_state, key, val)

        total_committed = snapshot_entries + tail_entries
        node.commit_index = total_committed
        node._apply_committed()
        await node.shutdown()

        recovered = Node(id=2, port=8081, host="127.0.0.1", data_dir=data_dir)

        assert recovered.last_applied == total_committed
        assert recovered.commit_index == total_committed
        assert recovered.store.value_store == expected_state

        # Spot-check keys around snapshot boundary and tail mutations.
        assert "base_0" not in recovered.store.value_store
        assert recovered.store.value_store["tail_2"] == 20002
        assert recovered.store.value_store["tail_1198"] == 21198

        await recovered.shutdown()
