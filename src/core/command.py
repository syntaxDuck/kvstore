from typing import Any
from dataclasses import dataclass, asdict
import json


@dataclass
class Command:
    op: str
    key: str
    val: Any

    @classmethod
    def deserialize(cls, serialized_string: str):
        dict = json.loads(serialized_string)
        return cls(**dict)

    def serialize(self):
        cmd_dict = asdict(self)
        return json.dumps(cmd_dict)

    def __str__(self):
        return f"{self.op} {self.key} {self.val}"
