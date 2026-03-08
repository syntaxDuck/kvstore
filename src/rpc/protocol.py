import asyncio
import json
import struct
from typing import Any

from ..logging import get_logger

logger = get_logger(__name__)


class Protocol:
    async def send_message(self, writer: asyncio.StreamWriter, message: dict[str, Any]):
        data = json.dumps(message).encode()
        length = struct.pack(">I", len(data))
        writer.write(length + data)
        await writer.drain()

    async def recv_message(self, reader: asyncio.StreamReader) -> dict[str, Any]:
        try:
            raw_len = await reader.readexactly(4)
            msg_len = struct.unpack(">I", raw_len)[0]
            data = await reader.readexactly(msg_len)
            return json.loads(data.decode())
        except asyncio.IncompleteReadError:
            logger.error("Connection closed unexpectedly during read")
            raise ConnectionError("Connection closed unexpectedly")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON received: {e}")
            raise ValueError(f"Invalid message format: {e}")
