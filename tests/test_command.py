import pytest

from src.core.types import Command


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


class TestCommandEdgeCases:
    def test_serialize_empty_string_value(self):
        cmd = Command(op="SET", key="key", val="")
        result = cmd.serialize()
        assert '"val": ""' in result

    def test_serialize_empty_string_key(self):
        cmd = Command(op="SET", key="", val="value")
        result = cmd.serialize()
        assert '""' in result

    def test_serialize_none_value(self):
        cmd = Command(op="SET", key="key", val=None)
        result = cmd.serialize()
        assert '"val": null' in result

    def test_deserialize_empty_string_value(self):
        json_str = '{"op": "GET", "key": "key", "val": ""}'
        cmd = Command.deserialize(json_str)
        assert cmd.val == ""

    def test_deserialize_null_value(self):
        json_str = '{"op": "GET", "key": "key", "val": null}'
        cmd = Command.deserialize(json_str)
        assert cmd.val is None

    def test_serialize_unicode_in_value(self):
        cmd = Command(op="SET", key="key", val="hello世界🎉")
        result = cmd.serialize()
        assert "hello\\u4e16\\u754c\\ud83c\\udf89" in result

    def test_deserialize_unicode_in_value(self):
        json_str = '{"op": "SET", "key": "key", "val": "hello世界🎉"}'
        cmd = Command.deserialize(json_str)
        assert cmd.val == "hello世界🎉"

    def test_serialize_unicode_in_key(self):
        cmd = Command(op="SET", key="键", val="value")
        result = cmd.serialize()
        assert '"\\u952e"' in result

    def test_serialize_special_characters_in_value(self):
        cmd = Command(op="SET", key="key", val='{"nested": "value", "array": [1,2,3]}')
        result = cmd.serialize()
        assert '\\"nested\\"' in result

    def test_serialize_boolean_values(self):
        cmd = Command(op="SET", key="bool", val=True)
        result = cmd.serialize()
        assert '"val": true' in result

    def test_deserialize_boolean_values(self):
        json_str = '{"op": "SET", "key": "bool", "val": false}'
        cmd = Command.deserialize(json_str)
        assert cmd.val is False

    def test_str_with_none_value(self):
        cmd = Command(op="GET", key="key", val=None)
        result = str(cmd)
        assert result == "GET key None"

    def test_str_with_complex_value(self):
        cmd = Command(op="SET", key="key", val={"a": 1})
        result = str(cmd)
        assert "SET" in result
        assert "key" in result
