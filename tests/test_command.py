import pytest

from src.core.command import Command


class TestCommand:
    def test_serialize_returns_json(self):
        cmd = Command(op="SET", key="foo", val="bar")
        result = cmd.serialize()
        assert result.startswith('{"')
        assert '"op": "SET"' in result
        assert '"key": "foo"' in result
        assert '"val": "bar"' in result

    def test_deserialize_parses_json(self):
        json_str = '{"op": "GET", "key": "test", "val": null}'
        cmd = Command.deserialize(json_str)
        assert cmd.op == "GET"
        assert cmd.key == "test"
        assert cmd.val is None

    def test_serialize_deserialize_roundtrip(self):
        original = Command(op="SET", key="key1", val={"nested": "value"})
        serialized = original.serialize()
        deserialized = Command.deserialize(serialized)
        assert deserialized.op == original.op
        assert deserialized.key == original.key
        assert deserialized.val == original.val

    def test_str_representation(self):
        cmd = Command(op="SET", key="foo", val="bar")
        result = str(cmd)
        assert result == "SET foo bar"

    def test_serialize_with_integer_value(self):
        cmd = Command(op="SET", key="count", val=42)
        result = cmd.serialize()
        assert '"val": 42' in result

    def test_serialize_with_list_value(self):
        cmd = Command(op="SET", key="items", val=[1, 2, 3])
        result = cmd.serialize()
        assert '"val": [1, 2, 3]' in result

    def test_deserialize_with_complex_value(self):
        json_str = '{"op": "SET", "key": "data", "val": {"a": 1, "b": 2}}'
        cmd = Command.deserialize(json_str)
        assert cmd.val == {"a": 1, "b": 2}
