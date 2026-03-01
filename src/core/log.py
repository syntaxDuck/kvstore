from io import TextIOWrapper
import os
from pathlib import Path
from collections.abc import Iterator


from .exceptions import LogError
from .types import Command, LogEntry, LogDetails
from .logging import get_logger
from .types import WriteAheadLogResponse
import json

logger = get_logger(__name__)


class WriteAheadLog:
    def __init__(self, name: str = "store_log.kvs", path: str = ".") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.log_name = name
        self.log_path = self.path / name
        self.details = LogDetails(
            term=0, last_log_index=0, last_log_term=0, log_length=0
        )

        self.file_handle: TextIOWrapper | None = None
        self.open()

    def __enter__(self) -> "WriteAheadLog":
        return self

    def __exit__(self):
        self.close()
        return False

    def open(self) -> None:
        self.file_handle = open(self.log_path, "a+")
        self.file_handle.seek(0)
        entry = None
        for line in self.file_handle:
            self.details.log_length += 1
            entry = LogEntry(**json.loads(line))

        if entry:
            self.details.last_log_index = entry.index
            self.details.last_log_term = entry.term

        self.file_handle.seek(0, 2)

    def get_last(self) -> LogEntry:
        if not self.file_handle:
            raise LogError

        last_line = self.file_handle.readline()
        return LogEntry(**json.loads(last_line))

    def append(self, term: int, cmd: Command) -> WriteAheadLogResponse:
        if not self.file_handle:
            raise LogError

        try:
            self.details.term = term
            self.details.last_log_index += 1
            entry = LogEntry(index=self.details.last_log_index, term=term, cmd=cmd)

            self.file_handle.write(entry.serialize())
            self.file_handle.write("\n")
            self.file_handle.flush()
            os.fsync(self.file_handle.fileno())
            return WriteAheadLogResponse.ok()
        except OSError as e:
            logger.error(f"Failed to append command to log: {e}")
            raise LogError

    def replay_log(self) -> Iterator[LogEntry]:
        if not self.file_handle:
            raise LogError

        self.file_handle.seek(0)
        for line in self.file_handle:
            yield LogEntry.deserialize(line)

    def close(self):
        if self.file_handle and not self.file_handle.closed:
            self.file_handle.close()
            self.file_handle = None
