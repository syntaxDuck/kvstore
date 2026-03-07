import asyncio
import json

from src.core.logging import get_logger, setup_logging
from src.core.raft.node import Node
from src.core.peer_client import PeerClient
from src.core.types import Command, RpcRequest


setup_logging()
logger = get_logger(__name__)


def parse_value(s: str):
    """Try to parse as JSON, fall back to string."""
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return s


async def main():
    node1 = Node(1, port=5003)
    node2 = Node(2, port=5004)
    node3 = Node(3, port=5005)

    nodes = [node1, node2, node3]

    for node in nodes:
        asyncio.create_task(node.start_server())

    await asyncio.sleep(1)

    for node in nodes:
        await node.register_peers([n.details for n in nodes])

    await asyncio.sleep(2)

    await console_loop(nodes)


async def console_loop(nodes: list[Node]):
    running = True

    while running:
        try:
            loop = asyncio.get_running_loop()
            line = await loop.run_in_executor(None, input, "Cmd: ")

            if not line:  # Handle empty input
                continue

            parts = line.split()
            if not parts or not parts[0]:
                continue

            cmd = parts[0].lower()

            if cmd == "help":
                print_help()

            elif cmd == "status":
                print_status(nodes)

            elif cmd == "quit" or cmd == "exit":
                running = False

            elif cmd == "set":
                await handle_set(nodes, parts)

            elif cmd == "get":
                await handle_get(nodes, parts)

            elif cmd == "delete":
                await handle_delete(nodes, parts)

            else:
                print(f"Unknown command: {cmd}. Type 'help' for available commands.")

        except KeyboardInterrupt:
            running = False
        except (EOFError, OSError):
            running = False
        except Exception as e:
            logger.error(f"Error in console loop: {e}")

    print("Shutting down...")


def print_help():
    print("""
Available commands:
  set <key> <value> [node_id] - Set a value (defaults to leader)
  get <key> [node_id]         - Get a value
  delete <key> [node_id]      - Delete a key
  status                      - Show status of all nodes
  help                       - Show this help
  quit                       - Exit

Examples:
  set foo bar 1          - Set foo=bar on node 1
  set foo bar            - Set foo=bar on leader
  set nums [1,2,3]      - Set nums=[1,2,3] on leader  
  set data {"x":"y"}    - Set data={...} on leader
  get foo 2              - Get foo from node 2
  status                 - Show all node statuses
""")


def print_status(nodes: list[Node]):
    print("\nNode Status:")
    print("-" * 50)
    for node in nodes:
        print(f"Node {node.id}:")
        print(f"  Role: {node.role}")
        print(f"  Term: {node.role_state.term}")
        print(f"  Peers: {len(node.peers)}")
        print(f"  Store: {node.store.value_store}")
    print()


async def handle_set(nodes: list[Node], parts: list[str]):
    if len(parts) < 3:
        print("Usage: set <key> <value> [node_id]")
        return

    try:
        print(parts)
        key = parts[1]
        val = parse_value(parts[2])

        # Last arg is target_id if it's a number
        target_id = None
        if len(parts) > 3 and parts[-1].isdigit():
            target_id = int(parts[-1])

        target = find_node(nodes, target_id) if target_id else find_leader(nodes)
        if not target:
            print("No leader found!")
            return

        cmd = Command(op="SET", key=key, val=val)
        request = RpcRequest.client_write(target.id, target.role.value, cmd=cmd)
        client = PeerClient(target.details)
        response = await client.send_rpc(request)

        if response.is_ok:
            print(f"OK - Set {key} = {val}")
        else:
            print(f"Error: {response.payload}")

    except (ValueError, IndexError) as e:
        print(f"Invalid input: {e}")


async def handle_get(nodes: list[Node], parts: list[str]):
    if len(parts) < 2:
        print("Usage: get <key> [node_id]")
        return

    try:
        key = parts[1]

        # Last arg is target_id if it's a number
        target_id = None
        if len(parts) > 2 and parts[-1].isdigit():
            target_id = int(parts[-1])

        target = find_node(nodes, target_id) if target_id else find_leader(nodes)
        if not target:
            print("No leader found!")
            return

        cmd = Command(op="GET", key=key, val=None)
        request = RpcRequest.client_get(target.id, target.role.value, cmd=cmd)
        client = PeerClient(target.details)
        response = await client.send_rpc(request)
        print(response)
        if response.is_ok and response.payload:
            print(f"{key} = {response.payload['val']}")
        else:
            print(f"Key not found: {key}")

    except (ValueError, IndexError) as e:
        print(f"Invalid input: {e}")


async def handle_delete(nodes: list[Node], parts: list[str]):
    if len(parts) < 2:
        print("Usage: delete <key> [node_id]")
        return

    try:
        key = parts[1]

        # Last arg is target_id if it's a number
        target_id = None
        if len(parts) > 2 and parts[-1].isdigit():
            target_id = int(parts[-1])

        target = find_node(nodes, target_id) if target_id else find_leader(nodes)
        if not target:
            print("No leader found!")
            return

        cmd = Command(op="DELETE", key=key, val=None)
        request = RpcRequest.client_write(target.id, target.role.value, cmd=cmd)
        client = PeerClient(target.details)
        response = await client.send_rpc(request)

        if response.is_ok:
            print(f"OK - Deleted {key}")
        else:
            print(f"Error: {response.payload}")

    except (ValueError, IndexError) as e:
        print(f"Invalid input: {e}")


def find_node(nodes: list[Node], node_id: int) -> Node | None:
    for node in nodes:
        if node.id == node_id:
            return node
    return None


def find_leader(nodes: list[Node]) -> Node | None:
    for node in nodes:
        if node.is_leader:
            return node
    return nodes[0] if nodes else None


asyncio.run(main())
