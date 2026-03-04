from typing import Coroutine, Protocol, Any
from asyncio import Task, create_task


class TimerStrategy(Protocol):
    def start(self) -> Coroutine[Any, Any, None]: ...
    def stop(self): ...
    def cancel(self): ...
    def reset(self): ...


class TimerTask:
    def __init__(self, strategy: TimerStrategy) -> None:
        self.task: Task | None = None
        self.strategy = strategy

    def start(self):
        if self.task is None or self.task.done():
            self.task = create_task(self.strategy.start())

    def stop(self):
        self.strategy.stop()
        if self.task:
            self.task.cancel()
            self.task = None

    def cancel(self):
        self.strategy.cancel()
        self.stop()

    def reset(self):
        self.strategy.reset()
