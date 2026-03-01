from io import TextIOWrapper
import os
from pathlib import Path
from collections.abc import Iterator
from dataclasses import dataclass

from .exceptions import LogError
from .types import Command
from .logging import get_logger

logger = get_logger(__name__)


@dataclass
class WriteAheadLogResponse:
    status: int

    @classmethod
    def ok(cls):
        return cls(1)

    @classmethod
    def err(cls):
        return cls(-1)

    @property
    def is_ok(self):
        return self.status > 0


class WriteAheadLog:
    def __init__(self, name: str = "store_log.kvs", path: str = ".") -> None:
        self.log_name = name
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path = self.path / name
        self.log_length = 0

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
        self.log_length = sum(1 for _ in self.file_handle)
        self.file_handle.seek(0, 2)

    def append(self, cmd: Command) -> WriteAheadLogResponse:
        if not self.file_handle:
            raise LogError

        try:
            self.log_length += 1
            self.file_handle.write(cmd.serialize())
            self.file_handle.write("\n")
            self.file_handle.flush()
            os.fsync(self.file_handle.fileno())
            return WriteAheadLogResponse.ok()
        except OSError as e:
            logger.error(f"Failed to append command to log: {e}")
            raise LogError

    def replay_log(self) -> Iterator[Command]:
        if not self.file_handle:
            raise LogError

        self.file_handle.seek(0)
        for line in self.file_handle:
            yield Command.deserialize(line)

    def close(self):
        if self.file_handle and not self.file_handle.closed:
            self.file_handle.close()
            self.file_handle = None
