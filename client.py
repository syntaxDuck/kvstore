import asyncio
import json

import aiohttp
from src.core.logging import get_logger, setup_logging


setup_logging()
logger = get_logger(__name__)

SERVICE_NAME = "kvstore"
NAMESPACE = "kvstore"
API_PORT = 8080
CLUSTER_SIZE = 3

CONNECTION_MODE = "localhost"

LOCALHOST_API_PORT = 30080

LEADER_SERVICE_NAME = "kvstore-leader"
LEADER_SERVICE_HOST = f"{LEADER_SERVICE_NAME}.{NAMESPACE}.svc.cluster.local"
LEADER_LOCALHOST_PORT = 30080

SERVICE_HOST = f"{SERVICE_NAME}.{NAMESPACE}.svc.cluster.local"
LEADER_RETRY_LIMIT = 5
LEADER_RETRY_DELAY = 0.25
WRITE_RETRY_LIMIT = 6
WRITE_RETRY_DELAY = 0.5

COMMANDS_HELP = """
Commands:
  set <key> <value>   - Set a value
  get <key>          - Get a value (default: leader)
  delete <key>       - Delete a key
  status [node_id]   - Show node status (default: leader)
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


async def _http_post(
    session: aiohttp.ClientSession,
    host: str,
    port: int,
    path: str,
    json_data: dict | None = None,
) -> dict | None:
    url = f"http://{host}:{port}{path}"
    try:
        async with session.post(url, json=json_data) as resp:
            if resp.status in (200, 201):
                return await resp.json()
            elif resp.status == 307:
                return {"error": "redirect", "detail": await resp.text()}
            return {"error": await resp.text()}
    except Exception as exc:
        logger.debug("Failed to reach %s: %s", url, exc)
    return None


async def _http_delete(
    session: aiohttp.ClientSession, host: str, port: int, path: str
) -> dict | None:
    url = f"http://{host}:{port}{path}"
    try:
        async with session.delete(url) as resp:
            if resp.status == 200:
                return await resp.json()
            return {"error": await resp.text()}
    except Exception as exc:
        logger.debug("Failed to reach %s: %s", url, exc)
    return None


async def _probe_leader_from_host(
    session: aiohttp.ClientSession, host: str, port: int
) -> int | None:
    for attempt in range(LEADER_RETRY_LIMIT):
        text = await _http_get(session, host, port, "/internal/v1/leader")
        if text is None:
            await asyncio.sleep(LEADER_RETRY_DELAY)
            continue
        try:
            data = json.loads(text)
            leader_id = data.get("leader_id")
            if leader_id is not None:
                return leader_id
        except json.JSONDecodeError:
            pass
        logger.debug(
            "Leader endpoint on %s returned non-JSON (%s), attempt %s",
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
        return API_PORT
    return LOCALHOST_API_PORT


async def find_leader() -> int | None:
    """Query /leader on each node to find the leader."""
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if _use_service_endpoint():
            leader = await _probe_leader_from_host(
                session, LEADER_SERVICE_HOST, API_PORT
            )
            if leader is not None:
                return leader
            logger.info(
                "Leader service endpoint not ready; falling back to shared service"
            )
            return await _probe_leader_from_host(session, SERVICE_HOST, API_PORT)
        for i in _node_probe_range():
            host = get_node_host(i)
            port = get_node_port(i)
            leader_id = await _probe_leader_from_host(session, host, port)
            if leader_id is not None:
                return leader_id
    return None


async def find_nodes() -> list[int]:
    """Find all available nodes."""
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if _use_service_endpoint():
            if await _http_get(session, SERVICE_HOST, API_PORT, "/internal/v1/health"):
                return [0]
            return []

        nodes: list[int] = []
        for i in _node_probe_range():
            host = get_node_host(i)
            port = get_node_port(i)
            if await _http_get(session, host, port, "/internal/v1/health"):
                nodes.append(i)
        return nodes


async def get_node_status(node_id: int) -> dict | None:
    """Get status from a specific node."""
    host = get_node_host(node_id)
    port = get_node_port(node_id)
    timeout = aiohttp.ClientTimeout(total=2)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            result = await _http_get(session, host, port, "/internal/v1/ping")
            if result:
                return {
                    "id": node_id,
                    "host": host,
                    "port": port,
                    "status": "ok",
                    "payload": result,
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


async def find_leader_host_port() -> tuple[str, int] | None:
    """Find leader host and port."""
    leader_id = await find_leader()
    if leader_id is None:
        return None

    if _use_service_endpoint():
        return LEADER_SERVICE_HOST, API_PORT
    return "localhost", LEADER_LOCALHOST_PORT


async def kv_set(key: str, value: any) -> bool:
    """Set a key-value pair."""
    leader = await find_leader_host_port()
    if leader is None:
        print("ERROR: No leader available!")
        return False

    host, port = leader
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        result = await _http_post(
            session, host, port, "/client/v1/kv", {"key": key, "val": value}
        )
        if result and "error" not in result:
            return True
        if result and result.get("error") == "redirect":
            print(f"Redirected to {result.get('detail')}")
        return False


async def kv_get(key: str) -> any:
    """Get a value by key."""
    leader = await find_leader_host_port()
    if leader is None:
        print("ERROR: No leader available!")
        return None

    host, port = leader
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        result = await _http_get(session, host, port, f"/client/v1/kv/{key}")
        if result:
            try:
                data = json.loads(result)
                return data.get("val")
            except json.JSONDecodeError:
                return result
    return None


async def kv_delete(key: str) -> bool:
    """Delete a key."""
    leader = await find_leader_host_port()
    if leader is None:
        print("ERROR: No leader available!")
        return False

    host, port = leader
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        result = await _http_delete(session, host, port, f"/client/v1/kv/{key}")
        if result and "error" not in result:
            return True
    return False


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
                    print(f"Leader: node {leader}")
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

                try:
                    success = await kv_set(key, val)
                    if success:
                        print(f"OK - Set {key} = {val}")
                    else:
                        print(f"Error: Failed to set {key}")
                except Exception as e:
                    print(f"Error: {e}")

            elif cmd == "get":
                if len(parts) < 2:
                    print("Usage: get <key>")
                    continue

                key = parts[1]

                try:
                    val = await kv_get(key)
                    if val is not None:
                        print(f"{key} = {val}")
                    else:
                        print(f"Key not found: {key}")
                except Exception as e:
                    print(f"Error: {e}")

            elif cmd == "delete":
                if len(parts) < 2:
                    print("Usage: delete <key>")
                    continue

                key = parts[1]

                try:
                    success = await kv_delete(key)
                    if success:
                        print(f"OK - Deleted {key}")
                    else:
                        print(f"Error: Failed to delete {key}")
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
