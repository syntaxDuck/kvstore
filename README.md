# KVStore

A distributed key-value store implementation with Raft consensus protocol.

## Overview

KVStore is a distributed key-value store that provides:
- Write-ahead logging for durability
- Raft consensus for distributed consistency
- TCP-based RPC communication between nodes
- Leader election with term-based validation
- Log replication across nodes

## Raft Features

- **Leader Election**: Nodes automatically elect a leader using randomized election timeouts
- **Term Validation**: All RPCs include term information for consistency
- **Heartbeat**: Leader sends periodic heartbeats to maintain authority
- **Vote Decision**: Followers vote based on term and log up-to-date check
- **Timer Strategy Pattern**: Election and heartbeat use pluggable timer strategies

### Timer Strategies

The system uses the Strategy pattern for election and heartbeat timers:
- `ElectionStrategy`: Manages election timeout and voting
- `HeartbeatStrategy`: Manages leader heartbeat sending
- `TimerTask`: Wrapper that manages asyncio task lifecycle

## Project Structure

```
src/core/
├── config.py            # Configuration settings
├── exceptions.py        # Custom exceptions
├── logging.py          # Logging configuration
├── peer_client.py      # RPC client for peer communication
├── network/            # Network layer
│   ├── __init__.py
│   ├── protocol.py     # Wire protocol for messages
│   └── rpc.py          # RPC dispatcher
├── raft/               # Raft consensus implementation
│   ├── __init__.py
│   ├── election_strategy.py   # Election timer strategy
│   ├── heartbeat_strategy.py   # Heartbeat timer strategy
│   ├── key_value_store.py     # In-memory key-value store
│   ├── log.py                 # Write-ahead log
│   ├── node.py                # Node implementation
│   ├── node_interface.py      # Node interface protocol
│   └── role_state.py          # Role and term tracking
├── types/              # Type definitions
│   ├── __init__.py
│   ├── types_command.py # Command type
│   ├── types_log.py     # Log entry types
│   ├── types_node.py    # NodeDetails
│   └── types_rpc.py    # RpcRequest, RpcResponse
└── util/               # Utilities
    ├── __init__.py
    └── timer.py         # Timer strategy pattern
```

## Usage

```python
import asyncio
from src.core.raft.node import Node
from src.core.types.types_rpc import RpcRequest
from src.core.logging import setup_logging

setup_logging()

async def main():
    # Create nodes
    node1 = Node(1, port=5003)
    node2 = Node(2, port=5004)

    # Start servers
    asyncio.create_task(node1.start_server())
    asyncio.create_task(node2.start_server())

    # Register peers
    await node1.register_peer(node2.details)
    await node2.register_peer(node1.details)

    # Send RPC
    response = await node1.peers[0].send_rpc(
        RpcRequest.ping(node1.id, node1.role)
    )

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
- `KV_RPC_DEBUG` - Enable debug logging for RPC messages (true/false)
