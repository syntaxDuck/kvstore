# KVStore

A distributed key-value store implementation with Raft-like consensus protocol.

## Overview

KVStore is a distributed key-value store that provides:
- Write-ahead logging for durability
- Raft-like consensus for distributed consistency
- TCP-based RPC communication between nodes

## Project Structure

```
src/core/
├── command.py      # Command serialization/deserialization
├── config.py       # Configuration settings
├── exceptions.py   # Custom exceptions
├── key_value_store.py  # In-memory key-value store
├── log.py         # Write-ahead log implementation
├── logging.py      # Logging configuration
├── node.py         # Node implementation
├── peer_client.py  # RPC client for peer communication
├── protocol.py     # Wire protocol for messages
├── rpc.py          # RPC request/response types
└── types.py        # Shared type definitions
```

## Usage

```python
import asyncio
from src.core.node import Node
from src.core.rpc import RpcRequest
from src.core.logging import setup_logging

setup_logging()

async def main():
    # Create nodes
    node1 = Node("Leader", 1, port=5003)
    node2 = Node("Follower", 2, port=5004)

    # Start servers
    asyncio.create_task(node1.start_server())
    asyncio.create_task(node2.start_server())

    # Register peers
    await node1.register_peer(node2.details)
    await node2.register_peer(node1.details)

    # Send RPC
    response = await node1.peers[0].send_rpc(RpcRequest.ping())

asyncio.run(main())
```

## Running Tests

```bash
uv run pytest tests/
```

## Configuration

Environment variables:
- `LOGGING_LEVEL` - Log level (DEBUG, INFO, WARNING, ERROR)
- `LOGS_DIR` - Directory for log files
- `LOG_FORMAT` - Log format
- `LOG_TO_FILE` - Enable file logging
- `LOG_TO_CONSOLE` - Enable console logging
