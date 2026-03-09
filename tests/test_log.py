import tempfile
from pathlib import Path

import pytest

from src.core.exceptions import LogError
from src.core.raft.log import WriteAheadLog, WriteAheadLogResponse
from src.core.types import Command


@pytest.fixture
def temp_log_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestWriteAheadLogResponse:
    def test_ok_returns_status_1(self):
        res = WriteAheadLogResponse.ok()
        assert res.status == 1
        assert res.is_ok is True

    def test_err_returns_status_minus_1(self):
        res = WriteAheadLogResponse.err()
        assert res.status == -1
        assert res.is_ok is False


class TestWriteAheadLog:
    def test_init_default_values(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        assert log.log_name == "store_log.kvs"
        assert str(log.path) == temp_log_dir
        log.close()

    def test_init_custom_values(self, temp_log_dir):
        log = WriteAheadLog(name="custom.log", path=temp_log_dir)
        assert log.log_name == "custom.log"
        log.close()

    def test_context_manager(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        assert log.file_handle is not None
        log.close()
        assert log.file_handle is None

    def test_append_success(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        cmd = Command(op="SET", key="foo", val="bar")
        res = log.append(1, cmd)
        assert res.is_ok is True
        assert log.details.length == 1
        log.close()

    def test_append_multiple_commands(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        log.append(1, Command(op="SET", key="a", val=1))
        log.append(1, Command(op="SET", key="b", val=2))
        log.append(1, Command(op="SET", key="c", val=3))
        assert log.details.length == 3
        log.close()

    def test_append_raises_when_closed(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        log.close()
        cmd = Command(op="SET", key="foo", val="bar")
        with pytest.raises(LogError):
            log.append(1, cmd)

    def test_replay_log_yields_entries(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        cmd1 = Command(op="SET", key="foo", val="bar")
        cmd2 = Command(op="SET", key="baz", val=123)
        log.append(1, cmd1)
        log.append(2, cmd2)
        log.close()

        log2 = WriteAheadLog(path=temp_log_dir)
        replayed = list(log2.replay_log())
        assert len(replayed) == 2
        assert replayed[0].term == 1
        assert replayed[0].cmd.op == "SET"
        assert replayed[0].cmd.key == "foo"
        assert replayed[0].cmd.val == "bar"
        assert replayed[1].term == 2
        assert replayed[1].cmd.key == "baz"
        assert replayed[1].cmd.val == 123
        log2.close()

    def test_close_closes_file_handle(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        assert log.file_handle is not None
        assert log.file_handle.closed is False
        log.close()
        assert log.file_handle is None

    def test_close_idempotent(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        log.close()
        log.close()
        assert log.file_handle is None

    def test_replay_log_raises_when_closed(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        log.close()
        with pytest.raises(LogError):
            list(log.replay_log())

    def test_file_persists_between_instances(self, temp_log_dir):
        log1 = WriteAheadLog(path=temp_log_dir)
        log1.append(1, Command(op="SET", key="persist", val="data"))
        log1.close()

        log2 = WriteAheadLog(path=temp_log_dir)
        log2.append(1, Command(op="SET", key="more", val="data2"))
        log2.close()

        log3 = WriteAheadLog(path=temp_log_dir)
        entries = list(log3.replay_log())
        assert len(entries) == 2
        log3.close()

    def test_log_path_constructed_correctly(self, temp_log_dir):
        log = WriteAheadLog(name="mylog.log", path=temp_log_dir)
        assert log.log_path == Path(temp_log_dir) / "mylog.log"
        log.close()


class TestWriteAheadLogEdgeCases:
    def test_replay_empty_log(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        replayed = list(log.replay_log())
        assert replayed == []
        log.close()

    def test_append_with_none_value(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        cmd = Command(op="SET", key="key", val=None)
        res = log.append(1, cmd)
        assert res.is_ok is True
        log.close()

    def test_append_with_complex_value(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        cmd = Command(op="SET", key="key", val={"nested": [1, 2, {"deep": "value"}]})
        res = log.append(1, cmd)
        assert res.is_ok is True

        replayed = list(log.replay_log())
        assert len(replayed) == 1
        assert replayed[0].cmd.val == {"nested": [1, 2, {"deep": "value"}]}
        log.close()

    def test_append_unicode_content(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        cmd = Command(op="SET", key="键", val="hello世界🎉")
        res = log.append(1, cmd)
        assert res.is_ok is True

        replayed = list(log.replay_log())
        assert replayed[0].cmd.key == "键"
        assert replayed[0].cmd.val == "hello世界🎉"
        log.close()

    def test_append_multiple_types(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        log.append(1, Command(op="SET", key="str", val="text"))
        log.append(1, Command(op="SET", key="int", val=42))
        log.append(1, Command(op="SET", key="float", val=3.14))
        log.append(1, Command(op="SET", key="bool", val=True))
        log.append(1, Command(op="SET", key="list", val=[1, 2, 3]))

        replayed = list(log.replay_log())
        assert len(replayed) == 5
        assert replayed[0].cmd.val == "text"
        assert replayed[1].cmd.val == 42
        assert replayed[2].cmd.val == 3.14
        assert replayed[3].cmd.val is True
        assert replayed[4].cmd.val == [1, 2, 3]
        log.close()

    def test_log_length_accuracy(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        assert log.details.length == 0

        log.append(1, Command(op="SET", key="a", val=1))
        assert log.details.length == 1

        log.append(1, Command(op="SET", key="b", val=2))
        assert log.details.length == 2

        log.append(1, Command(op="SET", key="c", val=3))
        assert log.details.length == 3
        log.close()

    def test_replay_after_close_and_reopen(self, temp_log_dir):
        log1 = WriteAheadLog(path=temp_log_dir)
        log1.append(1, Command(op="SET", key="a", val=1))
        log1.append(1, Command(op="SET", key="b", val=2))
        log1.close()

        log2 = WriteAheadLog(path=temp_log_dir)
        replayed1 = list(log2.replay_log())
        assert len(replayed1) == 2

        log2.append(1, Command(op="SET", key="c", val=3))
        log2.close()

        log3 = WriteAheadLog(path=temp_log_dir)
        replayed2 = list(log3.replay_log())
        assert len(replayed2) == 3
        log3.close()
