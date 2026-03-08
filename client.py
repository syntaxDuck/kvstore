import asyncio
import json
import aiohttp
from src.core.logging import get_logger, setup_logging
from src.core.peer_client import PeerClient
from src.core.types import Command, NodeDetails, RpcRequest, RpcResponse


setup_logging()
logger = get_logger(__name__)

SERVICE_NAME = "kvstore"
NAMESPACE = "kvstore"
RPC_PORT = 5003
HEALTH_PORT = 8080
CLUSTER_SIZE = 3

# Connection mode: "cluster" (inside K8s) or "localhost" (port-forward/NodePort)
CONNECTION_MODE = "localhost"

# For localhost mode, use NodePort
LOCALHOST_RPC_PORT = 30003
LOCALHOST_HEALTH_PORT = 30080

LEADER_SERVICE_NAME = "kvstore-leader"
LEADER_SERVICE_HOST = f"{LEADER_SERVICE_NAME}.{NAMESPACE}.svc.cluster.local"
LEADER_LOCALHOST_RPC_PORT = 30013

SERVICE_HOST = f"{SERVICE_NAME}.{NAMESPACE}.svc.cluster.local"
LEADER_RETRY_LIMIT = 5
LEADER_RETRY_DELAY = 0.25
WRITE_RETRY_LIMIT = 6
WRITE_RETRY_DELAY = 0.5

COMMANDS_HELP = """
Commands:
  set <key> <value>   - Set a value
  get <key> [node_id] - Get a value (default: leader)
  delete <key>        - Delete a key
  status [node_id]    - Show node status (default: leader)
  leader             - Show current leader
  nodes              - Show available nodes
  help               - Show this help
  quit               - Exit
"""


def print_commands() -> None:
    print(COMMANDS_HELP.strip())


def _use_service_endpoint() -> bool:
    return CONNECTION_MODE != "localhost"


def _node_probe_range() -> range:
    return range(CLUSTER_SIZE if not _use_service_endpoint() else 1)


async def _http_get(
    session: aiohttp.ClientSession, host: str, port: int, path: str
) -> str | None:
    url = f"http://{host}:{port}{path}"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                return await resp.text()
    except Exception as exc:
        logger.debug("Failed to reach %s: %s", url, exc)
    return None


async def _probe_leader_from_host(
    session: aiohttp.ClientSession, host: str, port: int
) -> int | None:
    for attempt in range(LEADER_RETRY_LIMIT):
        text = await _http_get(session, host, port, "/leader")
        if text is None:
            await asyncio.sleep(LEADER_RETRY_DELAY)
            continue
        if text.isdigit():
            return int(text)
        logger.debug(
            "Leader endpoint on %s returned non-digit (%s), attempt %s",
            host,
            text,
            attempt + 1,
        )
        await asyncio.sleep(LEADER_RETRY_DELAY)
    return None


def get_node_host(ordinal: int) -> str:
    if _use_service_endpoint():
        return SERVICE_HOST
    return "localhost"


def get_node_port(ordinal: int) -> int:
    if _use_service_endpoint():
        return RPC_PORT
    return LOCALHOST_RPC_PORT


def get_health_port(ordinal: int) -> int:
    if _use_service_endpoint():
        return HEALTH_PORT
    return LOCALHOST_HEALTH_PORT


async def find_leader() -> int | None:
    """Query /leader on each node to find the leader."""
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if _use_service_endpoint():
            leader = await _probe_leader_from_host(
                session, LEADER_SERVICE_HOST, HEALTH_PORT
            )
            if leader is not None:
                return leader
            logger.info(
                "Leader service endpoint not ready; falling back to shared service"
            )
            return await _probe_leader_from_host(session, SERVICE_HOST, HEALTH_PORT)
        for i in _node_probe_range():
            host = get_node_host(i)
            port = get_health_port(i)
            text = await _http_get(session, host, port, "/leader")
            if text is None:
                continue
            if text.isdigit():
                return int(text)
            logger.debug("Node %s returned unexpected leader payload: %s", i, text)
    return None


async def find_nodes() -> list[int]:
    """Find all available nodes."""
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if _use_service_endpoint():
            if await _http_get(session, SERVICE_HOST, HEALTH_PORT, "/health"):
                return [0]
            return []

        nodes: list[int] = []
        for i in _node_probe_range():
            host = get_node_host(i)
            port = get_health_port(i)
            if await _http_get(session, host, port, "/health"):
                nodes.append(i)
        return nodes


async def get_node_status(node_id: int) -> dict | None:
    """Get status from a specific node."""
    host = get_node_host(node_id)
    port = get_node_port(node_id)
    try:
        peer_details = NodeDetails(
            id=node_id,
            role="follower",
            host=host,
            port=port,
        )
        client = PeerClient(peer_details)
        request = RpcRequest.ping(node_id, "follower")
        response = await client.send_rpc(request)
        if response.is_ok:
            return {
                "id": node_id,
                "host": host,
                "port": port,
                "status": "ok",
                "payload": response.payload,
            }
    except Exception as e:
        return {
            "id": node_id,
            "host": host,
            "port": port,
            "status": "error",
            "error": str(e),
        }
    return None


async def send_to_leader(request: RpcRequest) -> tuple[RpcResponse | None, int | None]:
    """Send request to leader, auto-discovering if needed."""
    leader_id = await find_leader()
    if leader_id is None:
        return None, None

    if _use_service_endpoint():
        host = LEADER_SERVICE_HOST
        port = RPC_PORT
    else:
        host = "localhost"
        port = LEADER_LOCALHOST_RPC_PORT
    peer_details = NodeDetails(
        id=leader_id,
        role="leader",
        host=host,
        port=port,
    )
    client = PeerClient(peer_details)
    response = await client.send_rpc(request)
    return response, leader_id


async def send_client_write(request: RpcRequest) -> RpcResponse:
    last_error: str | None = None
    for attempt in range(1, WRITE_RETRY_LIMIT + 1):
        response, leader_id = await send_to_leader(request)
        if response and response.is_ok:
            return response

        if response:
            payload = response.payload or {}
            err = payload.get("Error") or payload.get("error")
            last_error = err
            if err and "CLIENT WRITE command to follower" in err:
                print(
                    f"Attempt {attempt}/{WRITE_RETRY_LIMIT}: still waiting for leader, retrying…"
                )
            else:
                raise RuntimeError(err or "write failed")

        if attempt < WRITE_RETRY_LIMIT:
            await asyncio.sleep(WRITE_RETRY_DELAY)
    raise RuntimeError(last_error or "leader unreachable after retries")


async def send_to_node(node_id: int, request: RpcRequest) -> RpcResponse:
    """Send request to a specific node."""
    host = get_node_host(node_id)
    port = get_node_port(node_id)
    peer_details = NodeDetails(
        id=node_id,
        role="follower",
        host=host,
        port=port,
    )
    client = PeerClient(peer_details)
    response = await client.send_rpc(request)
    return response


async def resolve_command_target(
    prefer_leader: bool = True,
) -> tuple[int | None, str | None, int | None, bool]:
    """Return (node_id, host, port, used_fallback)."""
    if prefer_leader:
        leader_id = await find_leader()
    else:
        leader_id = None

    if leader_id is not None:
        return leader_id, get_node_host(leader_id), get_node_port(leader_id), False

    nodes = await find_nodes()
    if nodes:
        node_id = nodes[0]
        return node_id, get_node_host(node_id), get_node_port(node_id), True

    return None, None, None, False


def parse_value(s: str):
    """Try to parse as JSON, fall back to string."""
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return s


async def main():
    print("KVStore Client")
    print("=" * 50)
    print("Connecting to cluster...")

    nodes = await find_nodes()
    if not nodes:
        print("ERROR: No nodes available in cluster!")
        return

    print(f"Found {len(nodes)} nodes: {nodes}")

    leader = await find_leader()
    if leader is not None:
        print(f"Leader: node {leader}")
    else:
        print("Leader: Not found (cluster may still be electing)")

    print()
    print_commands()
    print()

    running = True
    while running:
        try:
            line = await asyncio.get_event_loop().run_in_executor(None, input, "Cmd: ")

            if not line:
                continue

            parts = line.split()
            if not parts:
                continue

            cmd = parts[0].lower()

            if cmd == "help":
                print()
                print_commands()
                print()

            elif cmd == "quit" or cmd == "exit":
                running = False

            elif cmd == "leader":
                leader = await find_leader()
                if leader is not None:
                    print(f"Leader: node {leader} ({get_node_host(leader)})")
                else:
                    print("Leader: Not found")

            elif cmd == "nodes":
                nodes = await find_nodes()
                print(f"Available nodes: {nodes}")

            elif cmd == "status":
                target_id = None
                if len(parts) > 1 and parts[1].isdigit():
                    target_id = int(parts[1])
                elif len(parts) > 1 and parts[1] == "leader":
                    target_id = await find_leader()

                if target_id is None:
                    target_id = await find_leader()

                if target_id is None:
                    print("No nodes available!")
                    continue

                status = await get_node_status(target_id)
                if status:
                    print(f"\nNode {status['id']} Status:")
                    print(f"  Host: {status['host']}")
                    print(f"  Status: {status['status']}")
                    if "payload" in status:
                        print(f"  Response: {status['payload']}")
                    if "error" in status:
                        print(f"  Error: {status['error']}")
                else:
                    print(f"Failed to get status from node {target_id}")
                print()

            elif cmd == "set":
                if len(parts) < 3:
                    print("Usage: set <key> <value>")
                    continue

                key = parts[1]
                val = parse_value(" ".join(parts[2:]))

                cmd_obj = Command(op="SET", key=key, val=val)

                request = RpcRequest.client_write(0, "leader", cmd=cmd_obj)
                try:
                    response = await send_client_write(request)
                    print(f"OK - Set {key} = {val}")
                except Exception as e:
                    print(f"Error: {e}")

            elif cmd == "get":
                if len(parts) < 2:
                    print("Usage: get <key> [node_id]")
                    continue

                key = parts[1]
                target_id: int | None = None
                host: str | None = None
                port: int | None = None
                used_fallback = False

                if len(parts) > 2 and parts[2].isdigit():
                    target_id = int(parts[2])
                    host = get_node_host(target_id)
                    port = get_node_port(target_id)
                elif len(parts) > 2 and parts[2] == "leader":
                    target_id = await find_leader()
                    if target_id is not None:
                        host = get_node_host(target_id)
                        port = get_node_port(target_id)
                    else:
                        (
                            target_id,
                            host,
                            port,
                            used_fallback,
                        ) = await resolve_command_target(prefer_leader=False)
                else:
                    (
                        target_id,
                        host,
                        port,
                        used_fallback,
                    ) = await resolve_command_target()

                if target_id is None or host is None or port is None:
                    print("ERROR: No nodes available!")
                    continue

                if used_fallback:
                    print(
                        "Leader not directly resolvable; using service endpoint for read."
                    )

                peer_details = NodeDetails(
                    id=target_id,
                    role="follower",
                    host=host,
                    port=port,
                )
                client = PeerClient(peer_details)
                cmd_obj = Command(op="GET", key=key, val=None)
                request = RpcRequest.client_get(target_id, "follower", cmd=cmd_obj)

                try:
                    response = await client.send_rpc(request)
                    if response.is_ok and response.payload:
                        val = response.payload.get("val")
                        if val is not None:
                            print(f"{key} = {val}")
                        else:
                            print(f"Key not found: {key}")
                    else:
                        print(f"Error: {response.payload}")
                except Exception as e:
                    print(f"Error: {e}")

            elif cmd == "delete":
                if len(parts) < 2:
                    print("Usage: delete <key>")
                    continue

                key = parts[1]

                cmd_obj = Command(op="DELETE", key=key, val=None)
                request = RpcRequest.client_write(0, "leader", cmd=cmd_obj)

                try:
                    response = await send_client_write(request)
                    print(f"OK - Deleted {key}")
                except Exception as e:
                    print(f"Error: {e}")

            else:
                print(f"Unknown command: {cmd}")

        except KeyboardInterrupt:
            running = False
        except EOFError:
            running = False
        except Exception as e:
            print(f"Error: {e}")

    print("\nGoodbye!")


if __name__ == "__main__":
    asyncio.run(main())
