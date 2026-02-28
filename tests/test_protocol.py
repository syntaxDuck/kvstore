import pytest
import asyncio
import struct
import json
from unittest.mock import AsyncMock, MagicMock

from src.core.protocol import Protocol


class TestProtocol:
    @pytest.fixture
    def protocol(self):
        return Protocol()

    @pytest.mark.asyncio
    async def test_send_message_encodes_length_prefix(self, protocol):
        writer = AsyncMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        message = {"type": "test", "data": "hello"}
        await protocol.send_message(writer, message)

        writer.write.assert_called_once()
        call_args = writer.write.call_args[0][0]
        assert len(call_args) >= 4
        msg_len = struct.unpack(">I", call_args[:4])[0]
        encoded_msg = call_args[4:]
        assert len(encoded_msg) == msg_len

    @pytest.mark.asyncio
    async def test_send_message_json_encoding(self, protocol):
        writer = AsyncMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        message = {"key": "value", "number": 42}
        await protocol.send_message(writer, message)

        call_args = writer.write.call_args[0][0]
        encoded_msg = call_args[4:]
        decoded = json.loads(encoded_msg.decode())
        assert decoded == message

    @pytest.mark.asyncio
    async def test_recv_message_decodes(self, protocol):
        original_msg = {"type": "PING", "payload": {}}
        encoded = json.dumps(original_msg).encode()

        reader = AsyncMock()
        reader.readexactly = AsyncMock(
            side_effect=[struct.pack(">I", len(encoded)), encoded]
        )

        result = await protocol.recv_message(reader)
        assert result == original_msg

    @pytest.mark.asyncio
    async def test_recv_message_empty_payload(self, protocol):
        original_msg = {"type": "EMPTY", "payload": None}
        encoded = json.dumps(original_msg).encode()

        reader = AsyncMock()
        reader.readexactly = AsyncMock(
            side_effect=[struct.pack(">I", len(encoded)), encoded]
        )

        result = await protocol.recv_message(reader)
        assert result == original_msg

    @pytest.mark.asyncio
    async def test_recv_message_complex_payload(self, protocol):
        original_msg = {
            "type": "COMPLEX",
            "payload": {"nested": {"deep": [1, 2, 3]}, "string": "test", "number": 42},
        }
        encoded = json.dumps(original_msg).encode()

        reader = AsyncMock()
        reader.readexactly = AsyncMock(
            side_effect=[struct.pack(">I", len(encoded)), encoded]
        )

        result = await protocol.recv_message(reader)
        assert result == original_msg

    @pytest.mark.asyncio
    async def test_roundtrip_send_recv(self, protocol):
        original_msg = {"status": "OK", "node": 1, "payload": {"data": "value"}}

        reader = AsyncMock()
        encoded = json.dumps(original_msg).encode()
        reader.readexactly = AsyncMock(
            side_effect=[struct.pack(">I", len(encoded)), encoded]
        )

        writer = AsyncMock()
        written_data = bytearray()

        async def capture_write(data):
            written_data.extend(data)

        writer.write = MagicMock(side_effect=capture_write)
        writer.drain = AsyncMock()

        await protocol.send_message(writer, original_msg)
        result = await protocol.recv_message(reader)

        assert result == original_msg

    @pytest.mark.asyncio
    async def test_send_message_unicode(self, protocol):
        writer = AsyncMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        message = {"text": "hello世界🎉"}
        await protocol.send_message(writer, message)

        call_args = writer.write.call_args[0][0]
        encoded_msg = call_args[4:]
        decoded = json.loads(encoded_msg.decode())
        assert decoded["text"] == "hello世界🎉"

    @pytest.mark.asyncio
    async def test_recv_message_unicode(self, protocol):
        original_msg = {"text": "hello世界🎉"}
        encoded = json.dumps(original_msg).encode()

        reader = AsyncMock()
        reader.readexactly = AsyncMock(
            side_effect=[struct.pack(">I", len(encoded)), encoded]
        )

        result = await protocol.recv_message(reader)
        assert result == original_msg
