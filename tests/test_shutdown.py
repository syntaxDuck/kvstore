from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.api.main import create_api
from src.core.raft.node import Node


@pytest.mark.asyncio
async def test_lifespan_calls_node_shutdown_on_exit():
    app = create_api()
    node = SimpleNamespace(shutdown=AsyncMock())
    app.state.node = node

    async with app.router.lifespan_context(app):
        pass

    node.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_node_shutdown_stops_tasks_closes_peers_and_log():
    node = Node.__new__(Node)
    node.id = 1
    node.heartbeat_task = SimpleNamespace(stop=Mock())
    node.election_task = SimpleNamespace(stop=Mock())
    node.log = SimpleNamespace(close=Mock())
    peer_a = SimpleNamespace(id=2, close=AsyncMock())
    peer_b = SimpleNamespace(id=3, close=AsyncMock())
    node.peers = [peer_a, peer_b]

    await Node.shutdown(node)

    node.heartbeat_task.stop.assert_called_once()
    node.election_task.stop.assert_called_once()
    node.log.close.assert_called_once()
    peer_a.close.assert_awaited_once()
    peer_b.close.assert_awaited_once()
