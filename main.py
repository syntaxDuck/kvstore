from src.core.node import Node
from src.core.rpc import RpcRequest
from src.core.logging import setup_logging, get_logger
import asyncio

setup_logging()
logger = get_logger(__name__)


async def main():
    node1 = Node("Leader", 1, port=5003)
    node2 = Node("Follower", 2, port=5004)

    asyncio.create_task(node1.start_server())
    asyncio.create_task(node2.start_server())

    await asyncio.sleep(1)

    await node1.register_peer(node2.details)
    await node2.register_peer(node1.details)

    await asyncio.sleep(1)

    response = await node1.peers[0].send_rpc(RpcRequest.ping())

    logger.info("Response: %s", response)

    await asyncio.sleep(100)


asyncio.run(main())
