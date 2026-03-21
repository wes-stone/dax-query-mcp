"""Tests for the profiling harness (QueryProfiler + integration)."""

from __future__ import annotations

import json
import time

import pandas as pd

from dax_query_mcp.executor import DAXExecutor, dax_to_pandas
from dax_query_mcp.models import DAXQueryConfig
from dax_query_mcp.profiling import QueryProfiler


# ── Unit tests for QueryProfiler ────────────────────────────────────────


def test_profiler_tracks_all_phases() -> None:
    """All five phase names appear in timings, plus total."""
    profiler = QueryProfiler(query_name="test")
    with profiler:
        with profiler.phase("connect"):
            time.sleep(0.01)
        with profiler.phase("execute"):
            time.sleep(0.01)
        with profiler.phase("fetch"):
            time.sleep(0.01)
        with profiler.phase("normalize"):
            time.sleep(0.01)

    timings = profiler.timings
    assert set(timings.keys()) == {"connect", "execute", "fetch", "normalize", "total"}
    assert all(v > 0 for v in timings.values())
    assert timings["total"] >= sum(timings[p] for p in ("connect", "execute", "fetch", "normalize"))


def test_profiler_disabled_records_nothing() -> None:
    """When enabled=False, no timings are recorded."""
    profiler = QueryProfiler(query_name="noop", enabled=False)
    with profiler:
        with profiler.phase("connect"):
            time.sleep(0.01)

    assert profiler.total_elapsed == 0.0
    assert profiler.timings["total"] == 0.0


def test_profiler_to_response_field_format() -> None:
    """to_response_field returns a dict with 'timings' and 'summary' keys."""
    profiler = QueryProfiler(query_name="demo")
    with profiler:
        with profiler.phase("connect"):
            pass
        with profiler.phase("execute"):
            pass

    result = profiler.to_response_field()
    assert "timings" in result
    assert "summary" in result
    assert "demo" in result["summary"]
    assert "completed in" in result["summary"]


def test_profiler_missing_phases_default_to_zero() -> None:
    """Phases not entered should report 0.0."""
    profiler = QueryProfiler(query_name="partial")
    with profiler:
        with profiler.phase("connect"):
            pass

    timings = profiler.timings
    assert timings["execute"] == 0.0
    assert timings["fetch"] == 0.0
    assert timings["normalize"] == 0.0
    assert timings["connect"] >= 0.0


# ── Integration tests with mock cube ────────────────────────────────────


def test_dax_to_pandas_with_profile_attaches_timings() -> None:
    """dax_to_pandas(profile=True) stores profiling info in df.attrs."""
    df = dax_to_pandas(
        dax_query="EVALUATE Products",
        conn_str="MOCK://contoso",
        profile=True,
    )

    assert len(df) > 0
    assert "profiling" in df.attrs
    profiling = df.attrs["profiling"]
    assert "timings" in profiling
    assert "summary" in profiling
    timings = profiling["timings"]
    assert timings["total"] > 0
    assert all(k in timings for k in ("connect", "execute", "fetch", "normalize"))


def test_dax_to_pandas_without_profile_no_attrs() -> None:
    """dax_to_pandas(profile=False) does NOT attach profiling attrs."""
    df = dax_to_pandas(
        dax_query="EVALUATE Products",
        conn_str="MOCK://contoso",
        profile=False,
    )

    assert "profiling" not in df.attrs


def test_dax_to_pandas_default_profile_is_off() -> None:
    """profile defaults to False for backward compatibility."""
    df = dax_to_pandas(
        dax_query="EVALUATE Products",
        conn_str="MOCK://contoso",
    )

    assert "profiling" not in df.attrs


def test_executor_execute_with_profile() -> None:
    """DAXExecutor.execute(profile=True) attaches profiling to DataFrame."""
    executor = DAXExecutor(connection_string="MOCK://contoso")
    config = DAXQueryConfig(
        name="profiled",
        connection_string="MOCK://contoso",
        dax_query="EVALUATE Products",
    )

    df = executor.execute(config, profile=True)

    assert len(df) > 0
    profiling = df.attrs["profiling"]
    assert profiling["timings"]["total"] > 0
    assert "profiled" in profiling["summary"]


# ── MCP tool integration ────────────────────────────────────────────────


def test_run_connection_query_with_profile(tmp_path) -> None:
    """run_connection_query(profile=True) includes profiling in response."""
    from dax_query_mcp.mcp_server import run_connection_query

    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "contoso.yaml").write_text(
        "connection_string: 'MOCK://contoso'\ndescription: 'Mock cube'\n",
        encoding="utf-8",
    )

    payload = json.loads(
        run_connection_query(
            connection_name="contoso",
            query="EVALUATE Products",
            connections_dir=str(connections_dir),
            profile=True,
        )
    )

    assert "profiling" in payload
    assert "timings" in payload["profiling"]
    assert payload["profiling"]["timings"]["total"] > 0
    assert "summary" in payload["profiling"]


def test_run_connection_query_without_profile(tmp_path) -> None:
    """run_connection_query(profile=False) omits profiling from response."""
    from dax_query_mcp.mcp_server import run_connection_query

    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "contoso.yaml").write_text(
        "connection_string: 'MOCK://contoso'\ndescription: 'Mock cube'\n",
        encoding="utf-8",
    )

    payload = json.loads(
        run_connection_query(
            connection_name="contoso",
            query="EVALUATE Products",
            connections_dir=str(connections_dir),
            profile=False,
        )
    )

    assert "profiling" not in payload


def test_run_ad_hoc_query_with_profile() -> None:
    """run_ad_hoc_query(profile=True) includes profiling in response."""
    from dax_query_mcp.mcp_server import run_ad_hoc_query

    payload = json.loads(
        run_ad_hoc_query(
            connection_string="MOCK://contoso",
            query="EVALUATE Products",
            profile=True,
        )
    )

    assert "profiling" in payload
    assert payload["profiling"]["timings"]["total"] > 0


def test_profiler_summary_format() -> None:
    """Summary string matches expected format from the task spec."""
    profiler = QueryProfiler(query_name="test")
    with profiler:
        with profiler.phase("connect"):
            time.sleep(0.005)
        with profiler.phase("execute"):
            time.sleep(0.005)
        with profiler.phase("fetch"):
            time.sleep(0.005)
        with profiler.phase("normalize"):
            time.sleep(0.005)

    summary = profiler.to_response_field()["summary"]
    assert summary.startswith("Query 'test' completed in ")
    assert "connect:" in summary
    assert "execute:" in summary
    assert "fetch:" in summary
    assert "normalize:" in summary
