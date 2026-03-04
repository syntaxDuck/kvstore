import pytest
from unittest.mock import Mock, AsyncMock, MagicMock

from src.core.util.timer import TimerTask, TimerStrategy
from src.core.raft.election_strategy import ElectionStrategy
from src.core.raft.heartbeat_strategy import HeartbeatStrategy


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
