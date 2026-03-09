import asyncio
from collections import defaultdict
from typing import Any


class Histogram:
    def __init__(self) -> None:
        self._values: list[float] = []
        self._lock = asyncio.Lock()

    def observe(self, value: float) -> None:
        self._values.append(value)

    async def observe_async(self, value: float) -> None:
        async with self._lock:
            self._values.append(value)

    @property
    def avg(self) -> float:
        if not self._values:
            return 0.0
        return sum(self._values) / len(self._values)

    @property
    def count(self) -> int:
        return len(self._values)

    def percentile(self, p: float) -> float:
        if not self._values:
            return 0.0
        sorted_vals = sorted(self._values)
        idx = int(len(sorted_vals) * p / 100)
        idx = min(idx, len(sorted_vals) - 1)
        return sorted_vals[idx]

    def to_dict(self) -> dict[str, Any]:
        return {
            "avg": round(self.avg, 2),
            "p95": round(self.percentile(95), 2),
            "p99": round(self.percentile(99), 2),
            "count": self.count,
        }


class Counter:
    def __init__(self) -> None:
        self._count = 0
        self._lock = asyncio.Lock()

    def inc(self, value: int = 1) -> None:
        self._count += value

    async def inc_async(self, value: int = 1) -> None:
        async with self._lock:
            self._count += value

    @property
    def count(self) -> int:
        return self._count


class MetricsCollector:
    _instance: "MetricsCollector | None" = None
    _lock = asyncio.Lock()
    _initialized: bool

    def __new__(cls) -> "MetricsCollector":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self._histograms: dict[str, Histogram] = defaultdict(Histogram)
        self._counters: dict[str, Counter] = defaultdict(Counter)
        self._request_timings: dict[str, list[float]] = defaultdict(list)
        self._timing_lock = asyncio.Lock()

    def histogram(self, name: str) -> Histogram:
        return self._histograms[name]

    def counter(self, name: str) -> Counter:
        return self._counters[name]

    async def record_timing(self, name: str, duration_ms: float) -> None:
        async with self._timing_lock:
            self._histograms[name].observe(duration_ms)

    def record_timing_sync(self, name: str, duration_ms: float) -> None:
        self._histograms[name].observe(duration_ms)

    async def increment_counter(self, name: str, value: int = 1) -> None:
        await self._counters[name].inc_async(value)

    def increment_counter_sync(self, name: str, value: int = 1) -> None:
        self._counters[name].inc(value)

    def get_all_metrics(self) -> dict[str, Any]:
        metrics: dict[str, Any] = {}

        for name, hist in self._histograms.items():
            metrics[name] = hist.to_dict()

        for name, counter in self._counters.items():
            metrics[name] = {"count": counter.count}

        return metrics

    def reset(self) -> None:
        self._histograms.clear()
        self._counters.clear()
        self._request_timings.clear()


def get_metrics() -> MetricsCollector:
    return MetricsCollector()
