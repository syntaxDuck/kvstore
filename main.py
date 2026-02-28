from src.core.command import Command
from src.core.node import Node
from src.core.peer_client import PeerClient
from src.core.rpc import RpcRequest
from src.core.logging import get_logger, setup_logging
import asyncio
import ast

from src.core.types import NodeDetails

setup_logging()
logger = get_logger(__name__)


async def main():
    node1 = Node("Leader", 1, port=5003)
    node2 = Node("Follower", 2, port=5004)
    node3 = Node("Follower", 3, port=5005)

    nodes: list[NodeDetails] = [node1.details, node2.details, node3.details]

    asyncio.create_task(node1.start_server())
    asyncio.create_task(node2.start_server())
    asyncio.create_task(node3.start_server())

    await asyncio.sleep(1)

    await node1.register_peers(nodes)
    await node2.register_peers(nodes)
    await node3.register_peers(nodes)

    await asyncio.sleep(1)

    await console_loop(node1)


async def console_loop(node: Node):
    client = PeerClient(node.details)

    try:
        while True:
            request = input("Enter Command:").split("->")
            if not request:
                continue

            if request[0] == "quit":
                exit(1)

            rpc_type = request[0]
            cmd = None
            if len(request) == 4:
                op = request[1]
                key = request[2]

                try:
                    val = ast.literal_eval(request[3])
                    cmd = Command(op=op, key=key, val=val)
                except ValueError:
                    logger.warning(f"Bad value input: {request[3]}")

            if not rpc_type and not cmd:
                continue

            request = RpcRequest(type=rpc_type, cmd=cmd)
            try:
                await client.send_rpc(request)
            except Exception:
                continue
    except KeyboardInterrupt:
        logger.info("Shutting down...")


asyncio.run(main())
