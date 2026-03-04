import json
from typing import Any

from pydantic import BaseModel


class Command(BaseModel):
    op: str
    key: str
    val: Any

    @classmethod
    def deserialize(cls, serialized_string: str):
        data = json.loads(serialized_string)
        return cls(**data)

    def serialize(self):
        data = self.model_dump()
        return json.dumps(data)

    def __str__(self):
        return f"{self.op} {self.key} {self.val}"
