import pytest

from src.core.raft.key_value_store import KeyValueStore, KeyValueStoreResponse
from src.core.types import Command


@pytest.fixture
def store():
    return KeyValueStore()


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
    def test_init(self, store):
        assert store.value_store == {}

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

    def test_apply_unknown_operation(self, store):
        cmd = Command(op="UNKNOWN", key="foo", val="bar")
        res = store.apply(cmd)
        assert res.is_ok is False

    def test_set_overwrites_existing_key(self, store):
        store.value_store["foo"] = "original"
        cmd = Command(op="SET", key="foo", val="updated")
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["foo"] == "updated"

    def test_apply_returns_err_on_value_error(self, store):
        cmd = Command(op="INVALID_OP", key="foo", val="bar")
        res = store.apply(cmd)
        assert res.is_ok is False


class TestKeyValueStoreEdgeCases:
    def test_set_empty_string_value(self, store):
        cmd = Command(op="SET", key="empty", val="")
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["empty"] == ""

    def test_set_unicode_value(self, store):
        cmd = Command(op="SET", key="unicode", val="hello世界🎉")
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["unicode"] == "hello世界🎉"

    def test_set_unicode_key(self, store):
        cmd = Command(op="SET", key="键", val="value")
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["键"] == "value"

    def test_set_large_value(self, store):
        large_value = "x" * 10000
        cmd = Command(op="SET", key="large", val=large_value)
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["large"] == large_value

    def test_set_none_value(self, store):
        cmd = Command(op="SET", key="none", val=None)
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["none"] is None

    def test_get_after_delete_returns_error(self, store):
        store.value_store["foo"] = "bar"
        delete_cmd = Command(op="DELETE", key="foo", val=None)
        store.apply(delete_cmd)

        get_cmd = Command(op="GET", key="foo", val=None)
        res = store.apply(get_cmd)
        assert res.is_ok is False

    def test_delete_twice_returns_error(self, store):
        store.value_store["foo"] = "bar"
        delete_cmd = Command(op="DELETE", key="foo", val=None)

        res1 = store.apply(delete_cmd)
        assert res1.is_ok is True

        res2 = store.apply(delete_cmd)
        assert res2.is_ok is False

    def test_multiple_keys_different_types(self, store):
        store.value_store["string"] = "value"
        store.value_store["int"] = 42
        store.value_store["float"] = 3.14
        store.value_store["list"] = [1, 2, 3]
        store.value_store["dict"] = {"key": "value"}

        assert store.value_store["string"] == "value"
        assert store.value_store["int"] == 42
        assert store.value_store["float"] == 3.14
        assert store.value_store["list"] == [1, 2, 3]
        assert store.value_store["dict"] == {"key": "value"}

    def test_special_characters_in_key(self, store):
        cmd = Command(op="SET", key="key-with-dash", val="dash")
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["key-with-dash"] == "dash"

        cmd2 = Command(op="SET", key="key.with.dots", val="dots")
        res2 = store.apply(cmd2)
        assert res2.is_ok is True
        assert store.value_store["key.with.dots"] == "dots"

    def test_overwrite_different_types(self, store):
        store.value_store["key"] = "string"
        cmd = Command(op="SET", key="key", val=123)
        res = store.apply(cmd)
        assert res.is_ok is True
        assert store.value_store["key"] == 123
