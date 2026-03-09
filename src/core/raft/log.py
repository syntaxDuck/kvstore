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
        self.path.mkdir(parents=True, exist_ok=True)
        self.log_name = name
        self.log_path = self.path / name
        self.snapshot_path = self.path / f"{name}.snapshot.json"
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
        self.details = LogDetails()
        snapshot = self.load_snapshot()
        if snapshot:
            self.details.index = snapshot["last_included_index"]
            self.details.term = snapshot["last_included_term"]

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

        entry = self.replay_log_from(self.details.index)
        if not entry:
            raise LogError
        return entry

    def append(self, term: int, cmd: Command) -> WriteAheadLogResponse:
        if not self.file_handle:
            raise LogError

        try:
            self.details.term = term
            self.details.index += 1
            self.details.length += 1
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

    def load_snapshot(self) -> dict | None:
        if not self.snapshot_path.exists():
            return None

        try:
            with open(self.snapshot_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("Failed to load snapshot: %s", exc)
            raise LogError

        required_keys = {"last_included_index", "last_included_term", "state"}
        if not required_keys.issubset(data):
            raise LogError
        return data

    def save_snapshot(self, index: int, term: int, state: dict) -> None:
        tmp_path = self.snapshot_path.with_suffix(".tmp")
        payload = {
            "last_included_index": index,
            "last_included_term": term,
            "state": state,
        }
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.snapshot_path)
        except OSError as exc:
            logger.error("Failed to save snapshot: %s", exc)
            raise LogError

    def compact_up_to(self, index: int) -> None:
        if not self.file_handle:
            raise LogError

        self.file_handle.seek(0)
        remaining = []
        for line in self.file_handle:
            entry = LogEntry.deserialize(line)
            if entry.index > index:
                remaining.append(entry)

        try:
            with open(self.log_path, "w", encoding="utf-8") as f:
                for entry in remaining:
                    f.write(entry.serialize())
                    f.write("\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as exc:
            logger.error("Failed to compact log: %s", exc)
            raise LogError

        self.close()
        self.open()

    def close(self):
        if self.file_handle and not self.file_handle.closed:
            self.file_handle.close()
            self.file_handle = None

    def replay_log_from(self, index: int) -> LogEntry | None:
        """Replay log starting from a specific index."""
        if not self.file_handle:
            raise LogError

        self.file_handle.seek(0)
        for line in self.file_handle:
            entry = LogEntry.deserialize(line)
            if entry.index == index:
                return entry
        return None
