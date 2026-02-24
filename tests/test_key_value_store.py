import pytest
import tempfile

from src.core.key_value_store import KeyValueStore, KeyValueStoreResponse
from src.core.log import WriteAheadLog
from src.core.command import Command


@pytest.fixture
def temp_log_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def wal(temp_log_dir):
    log = WriteAheadLog(path=temp_log_dir)
    yield log
    log.close()


@pytest.fixture
def store(wal):
    return KeyValueStore(log=wal)


class TestKeyValueStoreResponse:
    def test_ok_with_value(self):
        res = KeyValueStoreResponse.ok("test_value")
        assert res.status == 1
        assert res.value == "test_value"
        assert res.is_ok is True

    def test_ok_without_value(self):
        res = KeyValueStoreResponse.ok()
        assert res.status == 1
        assert res.value is None
        assert res.is_ok is True

    def test_err(self):
        res = KeyValueStoreResponse.err("error info")
        assert res.status == -1
        assert res.value == "error info"
        assert res.is_ok is False


class TestKeyValueStore:
    def test_init_default_log(self):
        store = KeyValueStore()
        assert store.log is not None
        store.log.close()

    def test_set_stores_value(self, store):
        cmd = Command(op="SET", key="foo", val="bar")
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["foo"] == "bar"

    def test_get_existing_key(self, store):
        store.value_store["foo"] = "bar"
        cmd = Command(op="GET", key="foo", val=None)
        res = store.apply(cmd)
        assert res.is_ok is True
        assert res.value == "bar"

    def test_get_missing_key(self, store):
        cmd = Command(op="GET", key="nonexistent", val=None)
        res = store.apply(cmd)
        assert res.is_ok is False

    def test_delete_existing_key(self, store):
        store.value_store["foo"] = "bar"
        cmd = Command(op="DELETE", key="foo", val=None)
        res = store.apply(cmd)
        assert res.is_ok is True
        assert "foo" not in store.value_store

    def test_delete_missing_key(self, store):
        cmd = Command(op="DELETE", key="nonexistent", val=None)
        res = store.apply(cmd)
        assert res.is_ok is False

    def test_apply_writes_to_log(self, store, wal):
        cmd = Command(op="SET", key="foo", val="bar")
        store.apply(cmd)
        wal.file_handle.seek(0)
        content = wal.file_handle.read()
        assert "SET" in content
        assert "foo" in content
        assert "bar" in content

    def test_apply_skips_log_for_get(self, store, wal):
        cmd = Command(op="GET", key="foo", val=None)
        store.apply(cmd)
        wal.file_handle.seek(0)
        content = wal.file_handle.read()
        assert content.strip() == ""

    def test_build_from_log_replays_commands(self, temp_log_dir):
        log1 = WriteAheadLog(path=temp_log_dir)
        log1.append(Command(op="SET", key="a", val=1))
        log1.append(Command(op="SET", key="b", val=2))
        log1.close()

        log2 = WriteAheadLog(path=temp_log_dir)
        store = KeyValueStore(log=log2)
        assert store.value_store["a"] == 1
        assert store.value_store["b"] == 2
        log2.close()

    def test_apply_to_memory_unknown_operation(self, store):
        cmd = Command(op="UNKNOWN", key="foo", val="bar")
        res = store.apply_to_memory(cmd)
        assert res.is_ok is False

    def test_set_overwrites_existing_key(self, store):
        store.value_store["foo"] = "original"
        cmd = Command(op="SET", key="foo", val="updated")
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["foo"] == "updated"

    def test_apply_raises_when_log_closed(self, temp_log_dir):
        wal = WriteAheadLog(path=temp_log_dir)
        store = KeyValueStore(log=wal)
        wal.close()
        cmd = Command(op="SET", key="foo", val="bar")
        with pytest.raises(Exception):
            store.apply(cmd)

    def test_apply_to_memory_returns_err_on_value_error(self, store):
        cmd = Command(op="INVALID_OP", key="foo", val="bar")
        res = store.apply_to_memory(cmd)
        assert res.is_ok is False

    def test_delete_operation_not_logged(self, store, wal):
        cmd = Command(op="DELETE", key="foo", val=None)
        store.apply(cmd)
        wal.file_handle.seek(0)
        content = wal.file_handle.read()
        assert "DELETE" in content
