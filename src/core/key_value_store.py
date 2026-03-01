from dataclasses import dataclass
from typing import Any, Callable

from .types import Command
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
    def __init__(self) -> None:
        self.value_store = {}

        self._ops_no_log = ["GET"]
        self._ops: dict[str, Callable[[Command], KeyValueStoreResponse]] = {
            "SET": self._handle_set,
            "GET": self._handle_get,
            "DELETE": self._handle_delete,
        }

    def _handle_set(self, cmd: Command) -> KeyValueStoreResponse:
        self.value_store[cmd.key] = cmd.val
        return KeyValueStoreResponse.ok(cmd.val)

    def _handle_get(self, cmd: Command) -> KeyValueStoreResponse:
        try:
            return KeyValueStoreResponse.ok(self.value_store[cmd.key])
        except KeyError:
            pass
        return KeyValueStoreResponse.err()

    def _handle_delete(self, cmd: Command) -> KeyValueStoreResponse:
        try:
            del self.value_store[cmd.key]
            return KeyValueStoreResponse.ok()
        except KeyError:
            pass
        return KeyValueStoreResponse.err()

    def build_from_log(self, log: WriteAheadLog) -> None:
        logger.info("Rebuilding store from log")
        for cmd in log.replay_log():
            self.apply(cmd)

    def apply(self, cmd: Command) -> KeyValueStoreResponse:
        try:
            handler = self._ops.get(cmd.op)
            if handler is None:
                raise ValueError(f"No memory operation of type: {cmd.op}")
            res = handler(cmd)
            if res.is_ok:
                return res
            return KeyValueStoreResponse.err()
        except ValueError as e:
            return KeyValueStoreResponse.err(e)
        except AttributeError as e:
            logger.error(f"Invalid command object: {e}")
            return KeyValueStoreResponse.err(e)
        except Exception as e:
            logger.error(f"Unexpected error applying command: {e}")
            return KeyValueStoreResponse.err(e)
