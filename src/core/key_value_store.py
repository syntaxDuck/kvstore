from dataclasses import dataclass
from typing import Any, Callable

from .command import Command
from .log import WriteAheadLog
from .logging import get_logger

logger = get_logger(__name__)


@dataclass
class KeyValueStoreResponse:
    status: int
    value: Any

    @classmethod
    def ok(cls, value: Any = None):
        return cls(1, value)

    @classmethod
    def err(cls, value: Any = None):
        return cls(-1, value)

    @property
    def is_ok(self) -> bool:
        return self.status > 0


class KeyValueStore:
    def __init__(self, log: WriteAheadLog = WriteAheadLog()) -> None:
        self.log = log
        self.value_store = {}

        self._ops_no_log = ["GET"]
        self._ops: dict[str, Callable[[Command], KeyValueStoreResponse]] = {
            "SET": self._handle_set,
            "GET": self._handle_get,
            "DELETE": self._handle_delete,
        }

        self.build_from_log()
        logger.debug(f"Value Store After rebuild: {self.value_store}")

    def _handle_set(self, cmd: Command) -> KeyValueStoreResponse:
        self.value_store[cmd.key] = cmd.val
        return KeyValueStoreResponse.ok(cmd.val)

    def _handle_get(self, cmd: Command) -> KeyValueStoreResponse:
        try:
            return KeyValueStoreResponse.ok(self.value_store[cmd.key])
        except KeyError:
            logger.debug(f"Cannot get Key: {cmd.key} doesn't exist in value store")
        return KeyValueStoreResponse.err()

    def _handle_delete(self, cmd: Command) -> KeyValueStoreResponse:
        try:
            del self.value_store[cmd.key]
            return KeyValueStoreResponse.ok()
        except KeyError:
            logger.debug(f"Cannot delete Key: {cmd.key} doesn't exist in value store")
        return KeyValueStoreResponse.err()

    def build_from_log(self) -> None:
        logger.info("Rebuilding store from log...")
        for cmd in self.log.replay_log():
            logger.debug(f"Replaying command: {cmd}")
            self.apply_to_memory(cmd)

    def apply_to_memory(self, cmd: Command) -> KeyValueStoreResponse:
        try:
            handler = self._ops.get(cmd.op)
            if handler is None:
                raise ValueError(f"No memory operation of type: {cmd.op}")
            res = handler(cmd)
            if res.is_ok:
                logger.debug(f"Applied Command {cmd}, Value store: {self.value_store}")
                return res
            return KeyValueStoreResponse.err()
        except ValueError as e:
            return KeyValueStoreResponse.err(e)

    def apply(self, cmd: Command) -> KeyValueStoreResponse:
        log_res = None
        if cmd.op not in self._ops_no_log:
            log_res = self.log.append(cmd)

        if log_res is None or log_res.is_ok:
            mem_res = self.apply_to_memory(cmd)
            return mem_res

        return KeyValueStoreResponse.err()
