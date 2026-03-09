import pytest
from unittest.mock import Mock, AsyncMock, MagicMock
from types import SimpleNamespace

from src.core.util.timer import TimerTask
from src.core.raft.election_strategy import ElectionStrategy
from src.core.raft.heartbeat_strategy import HeartbeatStrategy
from src.core.raft.role_state import Role, RoleState
from src.core.types import RpcResponse


class TestTimerTask:
    @pytest.mark.asyncio
    async def test_start_creates_task_when_none(self):
        mock_strategy = Mock()
        mock_coro = AsyncMock()()
        mock_strategy.start = Mock(return_value=mock_coro)
        timer_task = TimerTask(mock_strategy)

        timer_task.start()

        mock_strategy.start.assert_called_once()
        assert timer_task.task is not None

    def test_start_does_not_create_task_when_running(self):
        mock_strategy = Mock()
        mock_task = Mock()
        mock_task.done = Mock(return_value=False)
        mock_strategy.start = Mock(return_value=MagicMock())

        timer_task = TimerTask(mock_strategy)
        timer_task.task = mock_task

        timer_task.start()

        mock_strategy.start.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_creates_task_when_done(self):
        mock_strategy = Mock()
        mock_task = Mock()
        mock_task.done = Mock(return_value=True)
        mock_strategy.start = Mock(return_value=AsyncMock()())

        timer_task = TimerTask(mock_strategy)
        timer_task.task = mock_task

        timer_task.start()

        mock_strategy.start.assert_called_once()

    def test_stop_calls_strategy_stop_and_cancels_task(self):
        mock_strategy = Mock()
        mock_task = Mock()
        timer_task = TimerTask(mock_strategy)
        timer_task.task = mock_task

        timer_task.stop()

        mock_strategy.stop.assert_called_once()
        mock_task.cancel.assert_called_once()
        assert timer_task.task is None

    def test_cancel_calls_strategy_cancel_and_stops(self):
        mock_strategy = Mock()
        timer_task = TimerTask(mock_strategy)

        timer_task.cancel()

        mock_strategy.cancel.assert_called_once()

    def test_reset_calls_strategy_reset(self):
        mock_strategy = Mock()
        timer_task = TimerTask(mock_strategy)

        timer_task.reset()

        mock_strategy.reset.assert_called_once()


class TestElectionStrategy:
    def test_reset_sets_election_reset(self):
        mock_node = Mock()
        mock_node.is_leader = False
        mock_node.is_follower = True
        mock_node.role_state.election_timeout = 0.01
        strategy = ElectionStrategy(mock_node)

        strategy.reset()

        assert strategy._election_reset is True

    def test_stop_sets_running_false(self):
        mock_node = Mock()
        strategy = ElectionStrategy(mock_node)

        strategy.stop()

        assert strategy._running is False

    def test_cancel_delegates_to_stop(self):
        mock_node = Mock()
        strategy = ElectionStrategy(mock_node)

        strategy.cancel()

        assert strategy._running is False

    def test_start_sets_running_true(self):
        mock_node = Mock()
        mock_node.is_leader = True
        mock_node.role_state.election_timeout = 0.01
        strategy = ElectionStrategy(mock_node)

        # start() returns a coroutine - we don't await it in tests
        # just verify the flag is set
        result = strategy.start()
        assert strategy._running is True
        strategy.stop()
        # exhaust the coroutine to avoid warnings
        if hasattr(result, "close"):
            result.close()

    @pytest.mark.asyncio
    async def test_start_election_promotes_candidate_with_quorum_votes(self):
        class FakeNode:
            def __init__(self):
                self.id = 1
                self.peers = [SimpleNamespace(id=2), SimpleNamespace(id=3)]
                self.role_state = RoleState(role=Role.FOLLOWER, term=2)
                self.log_details = SimpleNamespace(index=4, term=2)

            @property
            def role(self):
                return self.role_state.role

            @property
            def term(self):
                return self.role_state.term

        node = FakeNode()
        node.send_to_all_peers = AsyncMock(
            return_value=[
                RpcResponse.vote_response(2, Role.FOLLOWER, True),
                RpcResponse.vote_response(3, Role.FOLLOWER, False),
            ]
        )

        strategy = ElectionStrategy(node)
        await strategy._start_election()

        assert node.role_state.role == Role.LEADER
        assert node.role_state.term == 3

    @pytest.mark.asyncio
    async def test_start_election_returns_when_no_peers_registered(self):
        class FakeNode:
            def __init__(self):
                self.id = 1
                self.peers = []
                self.role_state = RoleState(role=Role.FOLLOWER, term=1)
                self.log_details = SimpleNamespace(index=0, term=0)

            @property
            def role(self):
                return self.role_state.role

            @property
            def term(self):
                return self.role_state.term

        node = FakeNode()
        node.send_to_all_peers = AsyncMock(side_effect=ValueError("no peers"))

        strategy = ElectionStrategy(node)
        await strategy._start_election()

        assert node.role_state.role == Role.CANDIDATE

    @pytest.mark.asyncio
    async def test_election_loop_respects_reset_flag(self, monkeypatch):
        class FakeNode:
            def __init__(self):
                self.role_state = RoleState(election_timeout=0.01)
                self._is_follower = True

            @property
            def is_follower(self):
                return self._is_follower

            @property
            def is_candidate(self):
                return False

        node = FakeNode()
        strategy = ElectionStrategy(node)
        strategy._running = True
        strategy._election_reset = True
        strategy._start_election = AsyncMock()

        sleep_calls = 0

        async def fake_sleep(_):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls == 2:
                strategy._running = False

        monkeypatch.setattr("src.core.raft.election_strategy.sleep", fake_sleep)
        await strategy._election_loop()

        strategy._start_election.assert_not_called()
        assert strategy._election_reset is False

    @pytest.mark.asyncio
    async def test_election_loop_starts_election_for_follower(self, monkeypatch):
        class FakeNode:
            def __init__(self):
                self.role_state = RoleState(election_timeout=0.01)

            @property
            def is_follower(self):
                return True

            @property
            def is_candidate(self):
                return False

        node = FakeNode()
        strategy = ElectionStrategy(node)
        strategy._running = True

        async def stop_after_start():
            strategy._running = False

        strategy._start_election = AsyncMock(side_effect=stop_after_start)
        monkeypatch.setattr("src.core.raft.election_strategy.sleep", AsyncMock())

        await strategy._election_loop()

        strategy._start_election.assert_awaited_once()


class TestHeartbeatStrategy:
    def test_stop_sets_running_false(self):
        mock_node = Mock()
        strategy = HeartbeatStrategy(mock_node)

        strategy.stop()

        assert strategy._running is False

    def test_cancel_delegates_to_stop(self):
        mock_node = Mock()
        strategy = HeartbeatStrategy(mock_node)

        strategy.cancel()

        assert strategy._running is False

    def test_reset_does_nothing(self):
        mock_node = Mock()
        strategy = HeartbeatStrategy(mock_node)

        strategy.reset()

    def test_start_sets_running_true(self):
        mock_node = Mock()
        mock_node.is_leader = True
        mock_node.role_state.heartbeat_timeout = 0.01
        mock_node.peers = []
        strategy = HeartbeatStrategy(mock_node)

        result = strategy.start()
        assert strategy._running is True
        strategy.stop()
        if hasattr(result, "close"):
            result.close()

    @pytest.mark.asyncio
    async def test_heartbeat_loop_sends_heartbeat_request(self, monkeypatch):
        class FakeNode:
            def __init__(self):
                self.id = 1
                self.role_state = RoleState(role=Role.LEADER, term=7, heartbeat_timeout=0.01)
                self.log_details = SimpleNamespace(index=8, term=7)
                self.commit_index = 6

            @property
            def role(self):
                return self.role_state.role

            @property
            def term(self):
                return self.role_state.term

        node = FakeNode()
        strategy = HeartbeatStrategy(node)
        strategy._running = True

        sent_requests = []

        async def send_to_all_peers(request):
            sent_requests.append(request)
            strategy._running = False
            return []

        node.send_to_all_peers = send_to_all_peers
        monkeypatch.setattr("src.core.raft.heartbeat_strategy.sleep", AsyncMock())

        await strategy._heartbeat_loop()

        assert len(sent_requests) == 1
        request = sent_requests[0]
        assert request.type == "HEARTBEAT"
        assert request.term == 7
        assert request.commit_index == 6

    @pytest.mark.asyncio
    async def test_heartbeat_loop_handles_missing_peers(self, monkeypatch):
        class FakeNode:
            def __init__(self):
                self.id = 1
                self.role_state = RoleState(role=Role.LEADER, term=1, heartbeat_timeout=0.01)
                self.log_details = SimpleNamespace(index=0, term=0)
                self.commit_index = 0

            @property
            def role(self):
                return self.role_state.role

            @property
            def term(self):
                return self.role_state.term

        node = FakeNode()
        node.send_to_all_peers = AsyncMock(side_effect=ValueError("no peers"))
        strategy = HeartbeatStrategy(node)
        strategy._running = True

        async def fake_sleep(_):
            strategy._running = False

        monkeypatch.setattr("src.core.raft.heartbeat_strategy.sleep", fake_sleep)
        await strategy._heartbeat_loop()

        node.send_to_all_peers.assert_awaited_once()
