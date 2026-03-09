from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main as main_module


class _FakeLoop:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now


@pytest.mark.asyncio
async def test_discover_and_register_peers_counts_only_successes(monkeypatch):
    async def register_peer(details):
        return details.id == 1

    node = SimpleNamespace(id=0, peers=[], register_peer=AsyncMock(side_effect=register_peer))
    loop = _FakeLoop()

    async def fake_sleep(seconds: float) -> None:
        loop.now += seconds

    monkeypatch.setattr(main_module, "LOCAL_DEV", True)
    monkeypatch.setattr(main_module, "BASE_PORT", 8080)
    monkeypatch.setattr(
        main_module, "compute_peer_addresses", lambda: [("localhost", 8081), ("localhost", 8082)]
    )
    monkeypatch.setattr(main_module.settings, "PEER_DISCOVERY_TIMEOUT", 5.0)
    monkeypatch.setattr(main_module.asyncio, "get_event_loop", lambda: loop)
    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    await main_module.discover_and_register_peers(node)

    # One peer keeps failing, so discovery retries instead of returning early.
    assert node.register_peer.await_count > 2


@pytest.mark.asyncio
async def test_discover_and_register_peers_returns_when_all_succeed(monkeypatch):
    node = SimpleNamespace(id=0, peers=[], register_peer=AsyncMock(return_value=True))
    loop = _FakeLoop()

    async def fake_sleep(seconds: float) -> None:
        loop.now += seconds

    monkeypatch.setattr(main_module, "LOCAL_DEV", True)
    monkeypatch.setattr(main_module, "BASE_PORT", 8080)
    monkeypatch.setattr(
        main_module, "compute_peer_addresses", lambda: [("localhost", 8081), ("localhost", 8082)]
    )
    monkeypatch.setattr(main_module.settings, "PEER_DISCOVERY_TIMEOUT", 5.0)
    monkeypatch.setattr(main_module.asyncio, "get_event_loop", lambda: loop)
    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    await main_module.discover_and_register_peers(node)

    assert node.register_peer.await_count == 2
