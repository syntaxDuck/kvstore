import pytest
import tempfile

from src.core.log import WriteAheadLog, WriteAheadLogResponse
from src.core.command import Command
from src.core.exceptions import LogError


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
        res = log.append(cmd)
        assert res.is_ok is True
        assert log.log_length == 1
        log.close()

    def test_append_multiple_commands(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        log.append(Command(op="SET", key="a", val=1))
        log.append(Command(op="SET", key="b", val=2))
        log.append(Command(op="SET", key="c", val=3))
        assert log.log_length == 3
        log.close()

    def test_append_raises_when_closed(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        log.close()
        cmd = Command(op="SET", key="foo", val="bar")
        with pytest.raises(LogError):
            log.append(cmd)

    def test_replay_log_yields_commands(self, temp_log_dir):
        log = WriteAheadLog(path=temp_log_dir)
        cmd1 = Command(op="SET", key="foo", val="bar")
        cmd2 = Command(op="SET", key="baz", val=123)
        log.append(cmd1)
        log.append(cmd2)
        log.close()

        log2 = WriteAheadLog(path=temp_log_dir)
        replayed = list(log2.replay_log())
        assert len(replayed) == 2
        assert replayed[0].op == "SET"
        assert replayed[0].key == "foo"
        assert replayed[0].val == "bar"
        assert replayed[1].key == "baz"
        assert replayed[1].val == 123
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
