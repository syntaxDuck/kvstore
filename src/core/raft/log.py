import json
import os
from collections.abc import Iterator
from io import TextIOWrapper
from pathlib import Path

from ..exceptions import LogError
from ..logging import get_logger
from ..types import Command, LogDetails, LogEntry, WriteAheadLogResponse

logger = get_logger(__name__)


class WriteAheadLog:
    def __init__(self, name: str = "store_log.kvs", path: str = ".") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.log_name = name
        self.log_path = self.path / name
        self.details = LogDetails()

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
            self.details.length += 1
            entry = LogEntry(**json.loads(line))

        if entry:
            self.details.index = entry.index
            self.details.term = entry.term

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
            self.details.index += 1
            entry = LogEntry(index=self.details.index, term=term, cmd=cmd)

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
