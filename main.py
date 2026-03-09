import asyncio
import os
import uvicorn

from src.api.main import create_api
from src.core.config import settings
from src.core.logging import get_logger, setup_logging
from src.core.raft.node import Node
from src.core.types import NodeDetails


setup_logging()
logger = get_logger(__name__)

node: Node | None = None

LOCAL_DEV = os.environ.get("LOCAL_DEV", "false").lower() == "true"
BASE_PORT = int(os.environ.get("BASE_PORT", "8080"))


def get_ordinal_from_pod_name(pod_name: str) -> int:
    if "-" in pod_name:
        try:
            return int(pod_name.rsplit("-", 1)[1])
        except (ValueError, IndexError):
            pass
    return 0


def compute_peer_addresses() -> list[tuple[str, int]]:
    peers = []

    if LOCAL_DEV:
        for i in range(settings.CLUSTER_SIZE):
            if i == settings.NODE_ID:
                continue
            port = BASE_PORT + i
            peers.append(("localhost", port))
        return peers

    cluster_size = settings.CLUSTER_SIZE
    service_name = settings.SERVICE_NAME
    namespace = settings.NAMESPACE
    port = settings.API_PORT

    pod_ordinal = get_ordinal_from_pod_name(settings.POD_NAME)

    for i in range(cluster_size):
        if i == pod_ordinal:
            continue
        hostname = f"{service_name}-{i}.{service_name}.{namespace}.svc.cluster.local"
        peers.append((hostname, port))

    return peers


async def discover_and_register_peers(node: Node) -> None:
    peer_addresses = compute_peer_addresses()
    logger.info(f"Node {node.id}: Discovering peers: {peer_addresses}")

    timeout = settings.PEER_DISCOVERY_TIMEOUT
    loop = asyncio.get_running_loop()
    start_time = loop.time()

    while loop.time() - start_time < timeout:
        registered = 0
        for host, port in peer_addresses:
            if LOCAL_DEV:
                peer_ordinal = port - BASE_PORT
            else:
                peer_ordinal = int(host.split("-")[1].split(".")[0])
            try:
                peer_details = NodeDetails(
                    id=peer_ordinal,
                    role="follower",
                    host=host,
                    port=port,
                )
                if await node.register_peer(peer_details):
                    registered += 1
                    logger.info(
                        f"Node {node.id}: Registered peer {peer_ordinal} at {host}:{port}"
                    )
            except Exception as e:
                logger.warning(
                    f"Node {node.id}: Failed to register peer {host}:{port}: {e}"
                )

        if registered >= len(peer_addresses):
            logger.info(
                f"Node {node.id}: Successfully registered all {registered} peers"
            )
            return

        await asyncio.sleep(2)

    logger.warning(
        f"Node {node.id}: Peer discovery timeout, registered {len(node.peers)}/{len(peer_addresses)} peers"
    )


async def main():
    global node

    if LOCAL_DEV:
        settings.DATA_DIR = f"data/node{settings.NODE_ID}"

    logger.info("Starting KVStore node with config:")
    logger.info(f"  NODE_ID: {settings.NODE_ID}")
    logger.info(f"  API_PORT: {settings.API_PORT}")
    logger.info(f"  POD_NAME: {settings.POD_NAME}")
    logger.info(f"  CLUSTER_SIZE: {settings.CLUSTER_SIZE}")
    logger.info(f"  SERVICE_NAME: {settings.SERVICE_NAME}")
    logger.info(f"  NAMESPACE: {settings.NAMESPACE}")
    logger.info(f"  DATA_DIR: {settings.DATA_DIR}")
    if LOCAL_DEV:
        logger.info("  LOCAL_DEV: true")

    node = Node(
        id=settings.NODE_ID,
        port=settings.API_PORT,
        host=settings.NODE_HOST,
        data_dir=settings.DATA_DIR,
    )

    app = create_api()
    app.state.node = node

    asyncio.create_task(discover_and_register_peers(node))

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=settings.API_PORT,
        log_level="info",
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    if settings.POD_NAME:
        settings.NODE_ID = get_ordinal_from_pod_name(settings.POD_NAME)

    asyncio.run(main())
