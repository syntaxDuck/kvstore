from types import SimpleNamespace

import pytest

from src.core.peer_http_client import PeerHttpClient
from src.core.raft.node import Node
from src.core.raft.role_state import Role
from src.core.types import Command, NodeDetails, RpcRequest, RpcResponse


class FakeLog:
    def __init__(self):
        self.details = SimpleNamespace(index=0, term=0)
        self._entries = {}

    def append(self, term: int, cmd: Command):
        self.details.term = term
        self.details.index += 1
        self._entries[self.details.index] = SimpleNamespace(
            index=self.details.index,
            term=term,
            cmd=cmd,
        )

    def replay_log_from(self, index: int):
        return self._entries.get(index)


def _build_leader_node(send_rpc_impl):
    node = Node.__new__(Node)
    node.id = 1
    node.host = "127.0.0.1"
    node.port = 8080
    node.role_state = SimpleNamespace(role=Role.LEADER, term=3)
    node.peers = [SimpleNamespace(id=2), SimpleNamespace(id=3)]
    node.match_index = {}
    node.commit_index = 0
    node.last_applied = 0
    node.log = FakeLog()

    applied_cmds = []
    node.store = SimpleNamespace(apply=lambda cmd: applied_cmds.append(cmd))

    async def _send_to_all_peers(_request):
        return await send_rpc_impl(_request)

    node.send_to_all_peers = _send_to_all_peers
    return node, applied_cmds


@pytest.mark.asyncio
async def test_timeout_retry_exhaustion_then_recovery_path():
    peer = NodeDetails(id=2, role="FOLLOWER", host="127.0.0.1", port=5004)
    client = PeerHttpClient(peer)

    state = {"mode": "fail", "calls": 0}

    async def fake_request_once(endpoint, params=None, payload=None):
        state["calls"] += 1
        if state["mode"] == "fail":
            raise TimeoutError("simulated timeout")
        return {"success": True}

    async def no_sleep(_attempt):
        return None

    client._request_once = fake_request_once
    client._sleep_for_retry = no_sleep

    req = RpcRequest.heartbeat(
        node_id=1,
        node_role="LEADER",
        term=4,
        last_log_index=10,
        last_log_term=4,
        commit_index=10,
    )

    first = await client.send_rpc(req)
    assert first.is_err is True
    assert first.payload["category"] == "transport_error"
    assert state["calls"] == 1 + client._max_retries

    state["mode"] = "ok"
    second = await client.send_rpc(req)
    assert second.is_ack is True

    await client.close()


@pytest.mark.asyncio
async def test_majority_write_continues_with_minority_transport_failure():
    async def peer_results(_request):
        return [
            RpcResponse.ack(2, "FOLLOWER"),
            RpcResponse.err(3, "FOLLOWER", {"category": "transport_error"}),
        ]

    node, applied_cmds = _build_leader_node(peer_results)
    req = RpcRequest.client_write(
        node_id=1,
        node_role="LEADER",
        term=3,
        cmd=Command(op="SET", key="k", val="v"),
    )

    res = await Node._handle_client_write(node, req)

    assert res.is_ok is True
    assert node.commit_index == 1
    assert node.last_applied == 1
    assert len(applied_cmds) == 1
    assert applied_cmds[0].key == "k"


@pytest.mark.asyncio
async def test_no_false_commit_when_failures_exhaust_below_quorum():
    async def peer_results(_request):
        return [
            RpcResponse.err(2, "FOLLOWER", {"category": "transport_error"}),
            RpcResponse.err(3, "FOLLOWER", {"category": "transport_error"}),
        ]

    node, applied_cmds = _build_leader_node(peer_results)
    req = RpcRequest.client_write(
        node_id=1,
        node_role="LEADER",
        term=3,
        cmd=Command(op="SET", key="k", val="v"),
    )

    res = await Node._handle_client_write(node, req)

    assert res.is_err is True
    assert node.log.details.index == 0
    assert node.commit_index == 0
    assert node.last_applied == 0
    assert applied_cmds == []
