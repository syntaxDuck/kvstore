from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.core.exceptions import LogError
from src.core.raft.node import Node
from src.core.raft.role_state import Role, RoleState
from src.core.types import Command, NodeDetails, RpcRequest, RpcResponse


def _build_node() -> Node:
    node = Node.__new__(Node)
    node.id = 1
    node.host = "127.0.0.1"
    node.port = 8080
    node.role_state = RoleState(role=Role.FOLLOWER, term=2)
    node.election_task = SimpleNamespace(reset=Mock())
    node.store = SimpleNamespace(value_store={"k": "v"}, apply=Mock())
    node.log = SimpleNamespace(
        details=SimpleNamespace(index=3, term=2, length=3),
        append=Mock(),
        replay_log=Mock(return_value=[]),
        replay_log_from=Mock(return_value=None),
        load_snapshot=Mock(return_value=None),
        save_snapshot=Mock(),
        compact_up_to=Mock(),
        close=Mock(),
    )
    node.peers = []
    node.match_index = {}
    node.commit_index = 0
    node.last_applied = 0
    return node


@pytest.mark.asyncio
async def test_handle_ping_steps_down_on_higher_term_and_resets_timer():
    node = _build_node()
    req = RpcRequest.ping(2, Role.LEADER, term=5)

    res = await Node._handle_ping(node, req)

    assert res.is_ok is True
    assert node.role_state.term == 5
    assert node.role_state.role == Role.FOLLOWER
    node.election_task.reset.assert_called_once()


@pytest.mark.asyncio
async def test_handle_client_get_returns_error_without_payload():
    node = _build_node()

    res = await Node._handle_client_get(node, RpcRequest.client_get(1, Role.LEADER))

    assert res.is_err is True
    assert res.payload == {"Error": "No command provided"}


@pytest.mark.asyncio
async def test_handle_client_get_returns_value():
    node = _build_node()
    req = RpcRequest.client_get(1, Role.LEADER, cmd=Command(op="GET", key="k", val=None))

    res = await Node._handle_client_get(node, req)

    assert res.is_ok is True
    assert res.payload == {"val": "v"}


@pytest.mark.asyncio
async def test_handle_vote_request_sets_vote_and_returns_response():
    node = _build_node()
    node.get_vote_decision = Mock(return_value=True)
    req = RpcRequest.request_vote(2, Role.CANDIDATE, term=3, last_log_index=3, last_log_term=2)

    res = await Node._handle_vote_request(node, req)

    assert res.is_ok is True
    assert res.payload == {"vote": True}
    assert node.role_state.voted_for == 2
    assert node.role_state.term == 3


@pytest.mark.asyncio
async def test_handle_vote_request_returns_error_on_log_exception():
    node = _build_node()
    node.get_vote_decision = Mock(side_effect=LogError("bad log"))
    req = RpcRequest.request_vote(2, Role.CANDIDATE, term=2, last_log_index=1, last_log_term=1)

    res = await Node._handle_vote_request(node, req)

    assert res.is_err is True
    assert res.payload == {"Error": "Issue validating log"}


@pytest.mark.asyncio
async def test_send_to_all_peers_raises_when_empty():
    node = _build_node()

    with pytest.raises(ValueError, match="No peers registered"):
        await Node.send_to_all_peers(node, RpcRequest.ping(1, Role.FOLLOWER))


@pytest.mark.asyncio
async def test_send_to_all_peers_collects_transport_errors():
    node = _build_node()
    ok_peer = SimpleNamespace(
        id=2,
        role=Role.FOLLOWER,
        send_rpc=AsyncMock(return_value=RpcResponse.ok(2, Role.FOLLOWER)),
    )
    bad_peer = SimpleNamespace(
        id=3,
        role=Role.FOLLOWER,
        send_rpc=AsyncMock(side_effect=ConnectionError("boom")),
    )
    node.peers = [ok_peer, bad_peer]

    responses = await Node.send_to_all_peers(node, RpcRequest.ping(1, Role.LEADER))

    assert [r.status for r in responses] == ["OK", "ERROR"]
    assert responses[1].payload == {"Error": "boom"}


@pytest.mark.asyncio
async def test_send_to_peer_routes_to_target():
    node = _build_node()
    target = SimpleNamespace(
        id=9,
        role=Role.FOLLOWER,
        send_rpc=AsyncMock(return_value=RpcResponse.ok(9, Role.FOLLOWER)),
    )
    node.peers = [target]

    res = await Node.send_to_peer(node, 9, RpcRequest.ping(1, Role.LEADER))

    assert res.is_ok is True
    target.send_rpc.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_to_peer_raises_when_target_missing():
    node = _build_node()
    node.peers = [SimpleNamespace(id=2, role=Role.FOLLOWER, send_rpc=AsyncMock())]

    with pytest.raises(ValueError, match="Target Peer not registered"):
        await Node.send_to_peer(node, 3, RpcRequest.ping(1, Role.LEADER))


@pytest.mark.asyncio
async def test_register_peers_skips_self_id():
    node = _build_node()
    node.register_peer = AsyncMock()
    peers = [
        NodeDetails(id=1, role=Role.FOLLOWER, host="127.0.0.1", port=8080),
        NodeDetails(id=2, role=Role.FOLLOWER, host="127.0.0.1", port=8081),
    ]

    await Node.register_peers(node, peers)

    node.register_peer.assert_awaited_once()
    assert node.register_peer.await_args.args[0].id == 2


@pytest.mark.asyncio
async def test_register_peer_appends_on_success(monkeypatch):
    node = _build_node()

    class FakePeerClient:
        def __init__(self, details):
            self.id = details.id
            self.role = details.role
            self.address = details.address

        async def send_rpc(self, _request):
            return RpcResponse.ok(self.id, self.role)

    monkeypatch.setattr("src.core.raft.node.PeerHttpClient", FakePeerClient)
    peer = NodeDetails(id=2, role=Role.FOLLOWER, host="127.0.0.1", port=8081)

    registered = await Node.register_peer(node, peer)

    assert len(node.peers) == 1
    assert node.peers[0].id == 2
    assert registered is True


@pytest.mark.asyncio
async def test_register_peer_does_not_append_on_error(monkeypatch):
    node = _build_node()

    class FakePeerClient:
        def __init__(self, details):
            self.id = details.id
            self.role = details.role
            self.address = details.address
            self.close = AsyncMock()

        async def send_rpc(self, _request):
            return RpcResponse.err(self.id, self.role, {"Error": "nope"})

    monkeypatch.setattr("src.core.raft.node.PeerHttpClient", FakePeerClient)
    peer = NodeDetails(id=2, role=Role.FOLLOWER, host="127.0.0.1", port=8081)

    registered = await Node.register_peer(node, peer)

    assert node.peers == []
    assert registered is False


@pytest.mark.asyncio
async def test_register_peer_skips_duplicate_peer_id(monkeypatch):
    node = _build_node()
    node.peers = [SimpleNamespace(id=2, address=("127.0.0.1", 8081))]

    class FakePeerClient:
        def __init__(self, _details):
            raise AssertionError("should not construct duplicate peer client")

    monkeypatch.setattr("src.core.raft.node.PeerHttpClient", FakePeerClient)
    peer = NodeDetails(id=2, role=Role.FOLLOWER, host="127.0.0.1", port=8081)

    registered = await Node.register_peer(node, peer)

    assert registered is True
    assert len(node.peers) == 1


@pytest.mark.asyncio
async def test_close_peer_sessions_continues_after_exception():
    node = _build_node()
    peer_ok = SimpleNamespace(id=2, close=AsyncMock())
    peer_fail = SimpleNamespace(id=3, close=AsyncMock(side_effect=RuntimeError("x")))
    node.peers = [peer_fail, peer_ok]

    await Node.close_peer_sessions(node)

    peer_fail.close.assert_awaited_once()
    peer_ok.close.assert_awaited_once()


def test_restore_state_from_storage_uses_snapshot_and_replay():
    node = _build_node()
    node.commit_index = 0
    node.last_applied = 0
    node.log.load_snapshot.return_value = {
        "last_included_index": 2,
        "state": {"seed": "x"},
    }
    node.log.replay_log.return_value = [
        SimpleNamespace(index=2, cmd=Command(op="SET", key="seed", val="x")),
        SimpleNamespace(index=3, cmd=Command(op="SET", key="k", val="v2")),
    ]

    Node._restore_state_from_storage(node)

    assert node.store.value_store == {"seed": "x"}
    node.store.apply.assert_called_once()
    assert node.last_applied == 3
    assert node.commit_index == 3


def test_maybe_create_snapshot_persists_and_compacts(monkeypatch):
    node = _build_node()
    node.id = 11
    node.last_applied = 5
    node.store.value_store = {"k": "v"}
    node.log.details.length = 10
    node.log.details.term = 7
    node.log.replay_log_from.return_value = SimpleNamespace(term=6)
    monkeypatch.setattr("src.core.raft.node.settings.SNAPSHOT_ENABLED", True)
    monkeypatch.setattr("src.core.raft.node.settings.SNAPSHOT_THRESHOLD", 5)

    Node._maybe_create_snapshot(node)

    node.log.save_snapshot.assert_called_once_with(5, 6, {"k": "v"})
    node.log.compact_up_to.assert_called_once_with(5)


def test_get_vote_decision_rejects_stale_term():
    node = _build_node()
    node.role_state.term = 4

    vote = Node.get_vote_decision(node, 2, 3, 10, 10)

    assert vote is False


def test_get_vote_decision_rejects_when_already_voted_for_other():
    node = _build_node()
    node.role_state.term = 4
    node.role_state.voted_for = 8

    vote = Node.get_vote_decision(node, 2, 4, 10, 10)

    assert vote is False


def test_get_vote_decision_rejects_older_log():
    node = _build_node()
    node.log.details.term = 3

    vote = Node.get_vote_decision(node, 2, 4, 10, 2)

    assert vote is False


def test_get_vote_decision_accepts_up_to_date_candidate():
    node = _build_node()
    node.role_state.term = 4
    node.log.details.term = 3
    node.log.details.index = 9

    vote = Node.get_vote_decision(node, 2, 4, 9, 3)

    assert vote is True


def test_details_and_address_properties():
    node = _build_node()

    assert node.details.id == 1
    assert node.log_details.index == 3
    assert node.address == ("127.0.0.1", 8080)


def test_leader_address_prefers_self_when_leader():
    node = _build_node()
    node.role_state.role = Role.LEADER

    assert node.leader_address == ("127.0.0.1", 8080)


def test_leader_address_uses_known_leader_peer():
    node = _build_node()
    node.role_state.role = Role.FOLLOWER
    node.peers = [
        SimpleNamespace(role=Role.FOLLOWER, address=("127.0.0.1", 8081)),
        SimpleNamespace(role=Role.LEADER, address=("127.0.0.1", 8082)),
    ]

    assert node.leader_address == ("127.0.0.1", 8082)


def test_update_match_index_tracks_peer_and_recomputes_commit():
    node = _build_node()
    node._update_commit_index = Mock()

    Node.update_match_index(node, 3, 8)

    assert node.match_index[3] == 8
    node._update_commit_index.assert_called_once()


def test_become_leader_starts_heartbeat_and_resets_match_index(monkeypatch):
    node = _build_node()
    node.role_state.role = Role.CANDIDATE
    node.peers = [SimpleNamespace(id=2), SimpleNamespace(id=3)]
    node.election_task = SimpleNamespace(stop=Mock())
    node.heartbeat_task = SimpleNamespace(start=Mock())
    node._schedule_role_label = Mock()
    node._election_start_time = 0.0

    metrics = SimpleNamespace(
        increment_counter_sync=Mock(),
        record_timing_sync=Mock(),
    )
    monkeypatch.setattr("src.core.raft.node.get_metrics", lambda: metrics)
    monkeypatch.setattr("src.core.raft.node.time.perf_counter", lambda: 0.01)

    Node._become_leader(node)

    node.election_task.stop.assert_called_once()
    node.heartbeat_task.start.assert_called_once()
    node._schedule_role_label.assert_called_once()
    assert node.match_index == {2: 0, 3: 0}
    metrics.increment_counter_sync.assert_called_once_with("raft_role_changes.leader")
    metrics.record_timing_sync.assert_called_once()


def test_step_down_starts_election_and_resets_commit(monkeypatch):
    node = _build_node()
    node.role_state.role = Role.LEADER
    node.commit_index = 9
    node.election_task = SimpleNamespace(start=Mock())
    node.heartbeat_task = SimpleNamespace(stop=Mock())
    node._schedule_role_label = Mock()

    metrics = SimpleNamespace(increment_counter_sync=Mock())
    monkeypatch.setattr("src.core.raft.node.get_metrics", lambda: metrics)

    Node._step_down(node)

    node.heartbeat_task.stop.assert_called_once()
    node.election_task.start.assert_called_once()
    node._schedule_role_label.assert_called_once()
    assert node.commit_index == 0
    metrics.increment_counter_sync.assert_called_once_with("raft_role_changes.follower")


def test_schedule_role_label_noop_without_pod_name():
    node = _build_node()
    node.pod_name = ""

    Node._schedule_role_label(node, "leader")


def test_schedule_role_label_handles_runtime_error(monkeypatch):
    node = _build_node()
    node.pod_name = "pod-1"

    def raise_runtime_error(coro):
        coro.close()
        raise RuntimeError("no loop")

    monkeypatch.setattr("src.core.raft.node.asyncio.create_task", raise_runtime_error)
    Node._schedule_role_label(node, "leader")


@pytest.mark.asyncio
async def test_get_k8s_api_returns_none_without_k8s_inputs(monkeypatch):
    node = _build_node()
    node.pod_name = ""
    node.namespace = "default"
    node._k8s_api = None

    monkeypatch.setattr("src.core.raft.node.k8s_client", object())
    monkeypatch.setattr("src.core.raft.node.k8s_config", object())
    res = await Node._get_k8s_api(node)

    assert res is None


@pytest.mark.asyncio
async def test_get_k8s_api_returns_cached_instance():
    node = _build_node()
    node.pod_name = "pod-1"
    node.namespace = "default"
    cached = object()
    node._k8s_api = cached

    res = await Node._get_k8s_api(node)

    assert res is cached


@pytest.mark.asyncio
async def test_get_k8s_api_loads_config_and_creates_api(monkeypatch):
    node = _build_node()
    node.pod_name = "pod-1"
    node.namespace = "default"
    node._k8s_api = None
    node._k8s_config_loaded = False

    fake_api = object()
    monkeypatch.setattr(
        "src.core.raft.node.k8s_config",
        SimpleNamespace(load_incluster_config=lambda: None),
    )
    monkeypatch.setattr(
        "src.core.raft.node.k8s_client",
        SimpleNamespace(CoreV1Api=lambda: fake_api),
    )

    res = await Node._get_k8s_api(node)

    assert res is fake_api
    assert node._k8s_config_loaded is True


@pytest.mark.asyncio
async def test_get_k8s_api_returns_none_on_config_error(monkeypatch):
    node = _build_node()
    node.pod_name = "pod-1"
    node.namespace = "default"
    node._k8s_api = None

    def fail():
        raise RuntimeError("bad k8s config")

    monkeypatch.setattr(
        "src.core.raft.node.k8s_config",
        SimpleNamespace(load_incluster_config=fail),
    )
    monkeypatch.setattr(
        "src.core.raft.node.k8s_client",
        SimpleNamespace(CoreV1Api=lambda: object()),
    )

    res = await Node._get_k8s_api(node)

    assert res is None


@pytest.mark.asyncio
async def test_patch_role_label_success(monkeypatch):
    node = _build_node()
    node.pod_name = "pod-1"
    node.namespace = "default"

    class FakeApi:
        def __init__(self):
            self.calls = []

        def patch_namespaced_pod(self, pod_name, namespace, body):
            self.calls.append((pod_name, namespace, body))

    api = FakeApi()
    node._get_k8s_api = AsyncMock(return_value=api)
    class FakeLoop:
        async def run_in_executor(self, _executor, fn):
            fn()
            return None

    monkeypatch.setattr("src.core.raft.node.asyncio.get_running_loop", lambda: FakeLoop())

    await Node._patch_role_label(node, "leader")

    assert api.calls[0][0] == "pod-1"
    assert api.calls[0][1] == "default"
    assert api.calls[0][2]["metadata"]["labels"]["kvstore-role"] == "leader"


@pytest.mark.asyncio
async def test_patch_role_label_ignores_patch_errors(monkeypatch):
    node = _build_node()
    node.pod_name = "pod-1"
    node.namespace = "default"
    node._get_k8s_api = AsyncMock(return_value=object())

    async def fail_run_in_executor(*_args, **_kwargs):
        raise RuntimeError("patch failed")

    monkeypatch.setattr(
        "src.core.raft.node.asyncio.get_running_loop",
        lambda: SimpleNamespace(run_in_executor=fail_run_in_executor),
    )

    await Node._patch_role_label(node, "leader")
