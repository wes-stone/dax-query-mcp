"""Query execution profiler for DAX queries.

Provides a ``QueryProfiler`` context manager that tracks timing for each
execution phase (connect, execute, fetch, normalize) and logs results to
stderr via loguru so MCP server stdout remains clean.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from loguru import logger


@dataclass(slots=True)
class PhaseTimer:
    """Tracks elapsed time for a single execution phase."""

    name: str
    _start: float = 0.0
    elapsed: float = 0.0

    def start(self) -> None:
        self._start = time.perf_counter()

    def stop(self) -> None:
        if self._start:
            self.elapsed = time.perf_counter() - self._start


PHASE_NAMES = ("connect", "execute", "fetch", "normalize")


@dataclass(slots=True)
class QueryProfiler:
    """Profiler that instruments DAX query execution phases.

    Usage::

        profiler = QueryProfiler("my_query", enabled=True)
        profiler.start_phase("connect")
        ...
        profiler.stop_phase("connect")
        profiler.finalize()
        print(profiler.timings)
    """

    query_name: str
    enabled: bool = True
    _phases: dict[str, PhaseTimer] = field(default_factory=dict)
    _total_start: float = 0.0
    total_elapsed: float = 0.0

    def __post_init__(self) -> None:
        if self.enabled:
            self._total_start = time.perf_counter()

    def start_phase(self, name: str) -> None:
        """Start timing a phase."""
        if not self.enabled:
            return
        if name not in self._phases:
            self._phases[name] = PhaseTimer(name=name)
        self._phases[name].start()

    def stop_phase(self, name: str) -> None:
        """Stop timing a phase."""
        if not self.enabled:
            return
        if name in self._phases:
            self._phases[name].stop()

    def finalize(self) -> None:
        """Finalize profiling and log results."""
        if not self.enabled:
            return
        self.total_elapsed = time.perf_counter() - self._total_start
        self._log()

    # ── context manager interface (for backwards compatibility) ─────────

    def __enter__(self) -> QueryProfiler:
        if self.enabled:
            self._total_start = time.perf_counter()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        if not self.enabled:
            return
        self.total_elapsed = time.perf_counter() - self._total_start
        self._log()

    class _PhaseContext:
        """Thin context manager returned by ``QueryProfiler.phase()``."""

        __slots__ = ("_timer",)

        def __init__(self, timer: PhaseTimer) -> None:
            self._timer = timer

        def __enter__(self) -> PhaseTimer:
            self._timer.start()
            return self._timer

        def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            self._timer.stop()

    def phase(self, name: str) -> _PhaseContext:
        """Return a context manager that times *name*."""
        if name not in self._phases:
            self._phases[name] = PhaseTimer(name=name)
        return self._PhaseContext(self._phases[name])

    # ── results ─────────────────────────────────────────────────────────

    @property
    def timings(self) -> dict[str, float]:
        """Return a dict of ``{phase_name: elapsed_seconds, ..., "total": ...}``."""
        result: dict[str, float] = {}
        for name in PHASE_NAMES:
            timer = self._phases.get(name)
            result[name] = round(timer.elapsed, 6) if timer else 0.0
        result["total"] = round(self.total_elapsed, 6)
        return result

    def to_response_field(self) -> dict[str, Any]:
        """Build a dict suitable for inclusion in an MCP JSON response."""
        timings = self.timings
        parts = ", ".join(
            f"{phase}: {timings[phase]:.3f}s" for phase in PHASE_NAMES if timings.get(phase)
        )
        return {
            "timings": timings,
            "summary": f"Query '{self.query_name}' completed in {timings['total']:.3f}s ({parts})",
        }

    # ── logging ─────────────────────────────────────────────────────────

    def _log(self) -> None:
        info = self.to_response_field()
        logger.info(info["summary"])
