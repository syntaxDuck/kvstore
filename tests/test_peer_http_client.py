import asyncio

import pytest

from src.core.metrics import get_metrics
from src.core.peer_http_client import PeerHttpClient
from src.core.types import Command, NodeDetails, RpcRequest


@pytest.fixture
def peer_client():
    peer = NodeDetails(id=2, role="FOLLOWER", host="127.0.0.1", port=5004)
    return PeerHttpClient(peer)


@pytest.mark.asyncio
async def test_idempotent_rpc_retries_then_succeeds(peer_client):
    calls = {"count": 0}

    async def fake_request_once(endpoint, params=None, payload=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise asyncio.TimeoutError("timed out")
        return {"success": True}

    async def no_sleep(attempt):
        return None

    peer_client._request_once = fake_request_once
    peer_client._sleep_for_retry = no_sleep

    req = RpcRequest.heartbeat(
        node_id=1,
        node_role="LEADER",
        term=3,
        last_log_index=10,
        last_log_term=3,
        commit_index=10,
    )
    res = await peer_client.send_rpc(req)

    assert res.is_ack is True
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_non_idempotent_rpc_does_not_retry(peer_client):
    calls = {"count": 0}

    async def always_timeout(endpoint, params=None, payload=None):
        calls["count"] += 1
        raise asyncio.TimeoutError("timed out")

    peer_client._request_once = always_timeout

    req = RpcRequest.client_write(
        node_id=1,
        node_role="LEADER",
        term=3,
        cmd=Command(op="SET", key="foo", val="bar"),
    )
    res = await peer_client.send_rpc(req)

    assert res.is_err is True
    assert res.payload["category"] == "transport_error"
    assert calls["count"] == 1


@pytest.mark.asyncio
async def test_unknown_message_type_returns_structured_error(peer_client):
    req = RpcRequest(type="BOGUS", node_id=1, node_role="LEADER", payload=None)
    res = await peer_client.send_rpc(req)

    assert res.is_err is True
    assert res.payload["category"] == "request_build_error"
    assert res.payload["rpc_type"] == "BOGUS"


@pytest.mark.asyncio
async def test_peer_rpc_metrics_recorded(peer_client):
    metrics = get_metrics()
    metrics.reset()

    async def fake_request_once(endpoint, params=None, payload=None):
        return {"status": "ok"}

    peer_client._request_once = fake_request_once

    req = RpcRequest.ping(node_id=1, node_role="LEADER")
    res = await peer_client.send_rpc(req)

    assert res.is_ok is True
    all_metrics = metrics.get_all_metrics()
    assert all_metrics["peer_rpc.ping.count.ok"]["count"] == 1
