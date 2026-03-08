import asyncio
from aiohttp import web

from src.core.config import settings
from src.core.logging import get_logger, setup_logging
from src.core.raft.node import Node
from src.core.types import NodeDetails


setup_logging()
logger = get_logger(__name__)

node: Node | None = None
is_ready = False


def get_ordinal_from_pod_name(pod_name: str) -> int:
    if "-" in pod_name:
        try:
            return int(pod_name.rsplit("-", 1)[1])
        except (ValueError, IndexError):
            pass
    return 0


def compute_peer_addresses() -> list[tuple[str, int]]:
    peers = []
    cluster_size = settings.CLUSTER_SIZE
    service_name = settings.SERVICE_NAME
    namespace = settings.NAMESPACE
    port = settings.NODE_PORT

    pod_ordinal = get_ordinal_from_pod_name(settings.POD_NAME)

    for i in range(cluster_size):
        if i == pod_ordinal:
            continue
        # DNS: kvstore-1.kvstore.svc.cluster.local (service.namespace.svc.cluster.local)
        hostname = f"{service_name}-{i}.{service_name}.{namespace}.svc.cluster.local"
        peers.append((hostname, port))

    return peers


async def discover_and_register_peers(node: Node) -> bool:
    global is_ready

    peer_addresses = compute_peer_addresses()
    logger.info(f"Node {node.id}: Discovering peers: {peer_addresses}")

    timeout = settings.PEER_DISCOVERY_TIMEOUT
    start_time = asyncio.get_event_loop().time()

    while asyncio.get_event_loop().time() - start_time < timeout:
        registered = 0
        for host, port in peer_addresses:
            peer_ordinal = int(host.split("-")[1].split(".")[0])
            try:
                peer_details = NodeDetails(
                    id=peer_ordinal,
                    role="follower",
                    host=host,
                    port=port,
                )
                await node.register_peer(peer_details)
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
            is_ready = True
            return True

        await asyncio.sleep(2)

    logger.warning(
        f"Node {node.id}: Peer discovery timeout, registered {len(node.peers)}/{len(peer_addresses)} peers"
    )
    # Mark ready even with partial peers - cluster can still function
    if len(node.peers) > 0:
        is_ready = True
        return True

    return False


async def health_handler(request):
    return web.Response(text="OK", status=200)


async def ready_handler(request):
    global is_ready

    # Ready if RPC server is running (node is not None)
    if node is not None:
        return web.Response(text="Ready", status=200)

    return web.Response(text="Not Ready", status=503)


async def leader_handler(request):
    global node

    if node is None:
        return web.Response(text="No node", status=503)

    if node.is_leader:
        return web.Response(text=str(node.id), status=200)

    # Not leader, check if we know who the leader is
    leader_addr = node.leader_address
    if leader_addr:
        # Can't easily get leader's ID from address, so return redirect info
        return web.Response(
            text=f"redirect:{leader_addr[0]}:{leader_addr[1]}", status=200
        )

    return web.Response(text="unknown", status=200)


async def main():
    global node, is_ready

    logger.info("Starting KVStore node with config:")
    logger.info(f"  NODE_ID: {settings.NODE_ID}")
    logger.info(f"  NODE_PORT: {settings.NODE_PORT}")
    logger.info(f"  POD_NAME: {settings.POD_NAME}")
    logger.info(f"  CLUSTER_SIZE: {settings.CLUSTER_SIZE}")
    logger.info(f"  SERVICE_NAME: {settings.SERVICE_NAME}")
    logger.info(f"  NAMESPACE: {settings.NAMESPACE}")
    logger.info(f"  DATA_DIR: {settings.DATA_DIR}")

    node = Node(
        id=settings.NODE_ID,
        port=settings.NODE_PORT,
        host=settings.NODE_HOST,
        data_dir=settings.DATA_DIR,
    )

    asyncio.create_task(node.start_rpc_server())

    await asyncio.sleep(0.5)

    # Start peer discovery in background
    asyncio.create_task(discover_and_register_peers(node))

    # Set ready after brief delay (RPC server is up)
    await asyncio.sleep(1)
    is_ready = True

    # Set up health HTTP server using AppRunner (not run_app to avoid event loop conflict)
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/ready", ready_handler)
    app.router.add_get("/leader", leader_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()

    logger.info(f"Node {node.id}: Started. Health endpoint on port 8080")

    # Run forever
    await asyncio.Event().wait()


if __name__ == "__main__":
    if settings.POD_NAME:
        settings.NODE_ID = get_ordinal_from_pod_name(settings.POD_NAME)

    asyncio.run(main())
