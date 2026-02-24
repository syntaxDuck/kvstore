from src.core.node import Node, RpcRequest
from src.core.logging import setup_logging, get_logger
import asyncio

setup_logging()
logger = get_logger(__name__)


async def main():
    node1 = Node("Leader", 1, port=5001)
    node2 = Node("Follower", 2, port=5002, peers=[node1.details])

    node1.peers.append(node2.details)

    asyncio.create_task(node1.start_server())
    asyncio.create_task(node2.start_server())

    await asyncio.sleep(1)

    # Node1 sends RPC to Node2
    response = await node1.send_rpc(node2.address, RpcRequest("ping", {}))

    print("Response:", response)

    await asyncio.sleep(100)


asyncio.run(main())
