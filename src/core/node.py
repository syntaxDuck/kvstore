import asyncio
import json
import struct
from typing import Any, Callable
from dataclasses import dataclass, asdict

from .key_value_store import KeyValueStore
from .log import WriteAheadLog
from .logging import get_logger

logger = get_logger(__name__)


@dataclass
class NodeDetails:
    id: int
    role: str
    host: str
    port: int

    @property
    def address(self):
        return (self.host, self.port)


@dataclass
class RpcRequest:
    type: str
    payload: dict[str, Any]


@dataclass
class RpcResponse:
    status: str
    node: str
    payload: dict[str, Any]


class Node:
    def __init__(
        self,
        role: str,
        id: int,
        port: int = 0,
        host: str = "0.0.0.0",
        peers: list = [],
    ) -> None:
        self.id = id
        self.role = role
        self.host = host
        self.port = port
        self.peers: list[NodeDetails] = peers
        self.log = WriteAheadLog()
        self.store = KeyValueStore()
        self.commit_index = 0

        self.rpc_commands: dict[str, Callable[[RpcRequest], RpcResponse]] = {
            "PING": self._handle_ping,
            "CLIENT_WRITE": self._handle_client_write,
            "APPEND_ENTRY": self._handle_append_entry,
        }

        logger.info(f"Node Initilized: {self.details}")

    def register_peer(self, peer: NodeDetails):
        logger.info(f"Registering peer to node: {peer}")
        self.peers.append(peer)

    def _handle_ping(self, req: RpcRequest) -> RpcResponse:
        return RpcResponse(
            "OK",
            f"{self.id}",
            {
                "host": self.host,
                "port": self.port,
            },
        )

    def _handle_client_write(self, req: RpcRequest) -> RpcResponse:
        if not self.is_leader:
            return RpcResponse(
                "Error",
                str(self.id),
                {
                    "details": "Sent CLIENT WRITE command to follower node, should send to Leader node"
                },
            )

        return RpcResponse(
            "OK",
            f"{self.id}",
            {
                "host": self.host,
                "port": self.port,
            },
        )

    def _handle_append_entry(self, req: RpcRequest) -> RpcResponse:
        return RpcResponse(
            "OK",
            f"{self.id}",
            {
                "host": self.host,
                "port": self.port,
            },
        )

    def _handle_rpc(self, req) -> RpcResponse:
        req = RpcRequest(**req)
        handler = self.rpc_commands.get(req.type.upper())
        if not handler:
            raise ValueError(f"No rpc command of type: {req.type}")

        res = handler(req)
        return res

    async def start_server(self) -> None:
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        logger.info(f"Node {self.id} listening on {self.host}:{self.port}")

        async with server:
            await server.serve_forever()

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            msg = await self.recv_message(reader)
            print(f"Node {self.id} received:", msg)
            res = self._handle_rpc(msg)

            await self.send_message(writer, asdict(res))

        except Exception as e:
            print("Error:", e)

        writer.close()
        await writer.wait_closed()

    async def send_rpc(self, peer: tuple, message: RpcRequest) -> None:
        if self.peers is None:
            logger.info("Cannot send rpc, no peers registered")
            return
        try:
            logger.info(f"Sending RPC to peer at address: {peer}")
            reader, writer = await asyncio.open_connection(peer[0], peer[1])
            await self.send_message(writer, asdict(message))
            response = await self.recv_message(reader)

            writer.close()
            await writer.wait_closed()

            return response
        except Exception as e:
            print("RPC failed:", e)
            return None

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

    @property
    def is_leader(self):
        return self.role == "Leader"

    @property
    def details(self):
        return NodeDetails(self.id, self.role, self.host, self.port)

    @property
    def address(self):
        return (self.host, self.port)
