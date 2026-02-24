import json
import asyncio
import struct
from typing import Any


class Protocol:
    async def send_message(self, writer: asyncio.StreamWriter, message: dict[str, Any]):
        data = json.dumps(message).encode()
        length = struct.pack(">I", len(data))
        writer.write(length + data)
        await writer.drain()

    async def recv_message(self, reader: asyncio.StreamReader) -> Any:
        raw_len = await reader.readexactly(4)
        msg_len = struct.unpack(">I", raw_len)[0]
        data = await reader.readexactly(msg_len)
        return json.loads(data.decode())
