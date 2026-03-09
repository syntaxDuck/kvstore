from typing import Any

from pydantic import BaseModel


class KvSetRequest(BaseModel):
    key: str
    val: Any
