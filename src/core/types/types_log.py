import json

from pydantic import BaseModel

from .types_command import Command


class LogDetails(BaseModel):
    term: int = 0
    index: int = 0
    length: int = 0


class LogEntry(BaseModel):
    index: int
    term: int
    cmd: Command

    @classmethod
    def deserialize(cls, serialized_string: str):
        data = json.loads(serialized_string)
        return cls(**data)

    def serialize(self):
        data = self.model_dump()
        return json.dumps(data)


class WriteAheadLogResponse(BaseModel):
    status: int

    @classmethod
    def ok(cls):
        return cls(status=1)

    @classmethod
    def err(cls):
        return cls(status=-1)

    @property
    def is_ok(self):
        return self.status > 0
