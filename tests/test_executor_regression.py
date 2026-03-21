"""Regression tests for DAXExecutor.execute() resource management.

These tests prevent regressions where nested context managers cause real
ADODB connections to hang while mock tests still pass.  They verify the
*structure* of the execute() method (via AST inspection) and the *ordering*
of resource cleanup (via mock instrumentation).
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dax_query_mcp.executor import DAXExecutor, _safe_close, _release_command
from dax_query_mcp.models import DAXQueryConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXECUTOR_SOURCE = inspect.getsource(DAXExecutor.execute)
_EXECUTOR_TREE = ast.parse(textwrap.dedent(_EXECUTOR_SOURCE))


def _find_execute_func(tree: ast.Module) -> ast.FunctionDef:
    """Return the ``execute`` FunctionDef node from *tree*."""
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "execute":
            return node
    raise AssertionError("Could not find execute() in parsed AST")


_EXECUTE_NODE = _find_execute_func(_EXECUTOR_TREE)


def _max_with_depth(node: ast.AST, current: int = 0) -> int:
    """Return the maximum nesting depth of ``with`` statements in *node*."""
    max_depth = current
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.With):
            max_depth = max(max_depth, _max_with_depth(child, current + 1))
        else:
            max_depth = max(max_depth, _max_with_depth(child, current))
    return max_depth


def _make_config(**overrides) -> DAXQueryConfig:
    defaults = dict(
        name="regression",
        connection_string="Provider=MSOLAP.8;Initial Catalog=model",
        dax_query='EVALUATE ROW("Value", 1)',
    )
    defaults.update(overrides)
    return DAXQueryConfig(**defaults)


class FakeField:
    def __init__(self, name: str, value: object = None):
        self.Name = name
        self.Value = value


class FakeRecordset:
    def __init__(self, fields: list[str], rows: list[tuple]):
        self.Fields = [FakeField(n) for n in fields]
        self._rows = rows
        self._index = 0
        self.closed = False
        self._sync()

    @property
    def EOF(self) -> bool:
        return self._index >= len(self._rows)

    def MoveNext(self) -> None:
        self._index += 1
        self._sync()

    def _sync(self) -> None:
        if not self.EOF:
            for i, f in enumerate(self.Fields):
                f.Value = self._rows[self._index][i]

    def Close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.ConnectionTimeout = None
        self.CommandTimeout = None
        self.closed = False

    def Open(self, _cs: str):
        pass

    def Close(self):
        self.closed = True


class FakeCommand:
    def __init__(self, recordset: FakeRecordset):
        self.ActiveConnection = None
        self.CommandText = None
        self.CommandTimeout = None
        self._recordset = recordset

    def Execute(self):
        return (self._recordset,)


# ---------------------------------------------------------------------------
# AST-based structural tests
# ---------------------------------------------------------------------------

class TestExecuteStructure:
    """Verify the *source code* of execute() to prevent context-manager regressions."""

    def test_no_nested_with_statements(self):
        """execute() must NOT contain nested ``with`` blocks (depth > 1).

        Nested ``with`` blocks over ADODB objects caused real connections to
        hang because the inner context manager called Close() before the
        outer one finished.
        """
        depth = _max_with_depth(_EXECUTE_NODE)
        assert depth <= 1, (
            f"execute() has {depth}-level nested 'with' statements. "
            "Nested context managers break real ADODB connections."
        )

    def test_profiler_uses_explicit_start_stop(self):
        """Profiler calls must be start_phase()/stop_phase(), NOT context managers.

        Using ``with profiler.phase(...)`` inside execute() creates hidden
        nested context managers that interfere with ADODB resource lifetime.
        """
        source = _EXECUTOR_SOURCE
        assert "start_phase" in source, "execute() must call profiler.start_phase()"
        assert "stop_phase" in source, "execute() must call profiler.stop_phase()"
        assert "profiler.phase(" not in source, (
            "execute() must NOT use profiler.phase() context manager"
        )

    def test_no_with_profiler_context_manager(self):
        """execute() must not use ``with QueryProfiler(...)`` as a context manager."""
        for node in ast.walk(_EXECUTE_NODE):
            if isinstance(node, ast.With):
                for item in node.items:
                    ctx = item.context_expr
                    if isinstance(ctx, ast.Call):
                        func = ctx.func
                        name = ""
                        if isinstance(func, ast.Name):
                            name = func.id
                        elif isinstance(func, ast.Attribute):
                            name = func.attr
                        assert name != "QueryProfiler", (
                            "execute() must not use QueryProfiler as a context manager"
                        )

    def test_finally_block_exists(self):
        """execute() must have a try/finally that cleans up resources."""
        try_nodes = [n for n in ast.walk(_EXECUTE_NODE) if isinstance(n, ast.Try)]
        assert try_nodes, "execute() must contain a try block"
        has_finally = any(n.finalbody for n in try_nodes)
        assert has_finally, "execute() must have a finally block for cleanup"

    def test_finally_releases_command_and_closes(self):
        """The finally block must call _release_command and _safe_close."""
        source = _EXECUTOR_SOURCE
        # Find the finally block content in the source
        finally_idx = source.rfind("finally:")
        assert finally_idx != -1, "Could not find 'finally:' in execute()"
        finally_body = source[finally_idx:]
        assert "_release_command" in finally_body, (
            "finally block must call _release_command(cmd)"
        )
        assert "_safe_close" in finally_body, (
            "finally block must call _safe_close() for cleanup"
        )


# ---------------------------------------------------------------------------
# Resource cleanup ordering tests
# ---------------------------------------------------------------------------

class TestCleanupOrdering:
    """Verify resources are released in the correct order."""

    def _make_tracking_executor(self):
        """Build an executor that records the order of operations."""
        call_log: list[str] = []

        rs = FakeRecordset(fields=["V"], rows=[(1,)])
        conn = FakeConnection()
        cmd = FakeCommand(rs)

        orig_conn_close = conn.Close
        orig_rs_close = rs.Close

        def tracked_conn_close():
            call_log.append("conn.Close")
            orig_conn_close()

        def tracked_rs_close():
            call_log.append("rs.Close")
            orig_rs_close()

        def tracked_release_cmd():
            call_log.append("cmd.release")
            cmd.ActiveConnection = None

        conn.Close = tracked_conn_close
        rs.Close = tracked_rs_close

        def dispatcher(name: str):
            if name == "ADODB.Connection":
                call_log.append("conn.created")
                return conn
            if name == "ADODB.Command":
                call_log.append("cmd.created")
                return cmd
            raise AssertionError(name)

        return DAXExecutor(dispatcher=dispatcher), call_log, conn, rs, cmd

    def test_cleanup_happens_after_dataframe_returned(self):
        """Resources must be closed AFTER the dataframe result is constructed."""
        executor, call_log, conn, rs, cmd = self._make_tracking_executor()
        config = _make_config()

        df = executor.execute(config)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        # Connection and recordset should be closed after execute returns
        assert conn.closed is True
        assert rs.closed is True

    def test_profiler_finalize_before_resource_cleanup(self):
        """profiler.finalize() must happen BEFORE the finally block cleans up."""
        call_log: list[str] = []

        rs = FakeRecordset(fields=["V"], rows=[(1,)])
        conn = FakeConnection()
        cmd = FakeCommand(rs)

        orig_conn_close = conn.Close
        orig_rs_close = rs.Close

        def tracked_conn_close():
            call_log.append("conn.Close")
            orig_conn_close()

        def tracked_rs_close():
            call_log.append("rs.Close")
            orig_rs_close()

        conn.Close = tracked_conn_close
        rs.Close = tracked_rs_close

        def dispatcher(name: str):
            if name == "ADODB.Connection":
                return conn
            if name == "ADODB.Command":
                return cmd
            raise AssertionError(name)

        # Patch profiler.finalize to record when it's called
        with patch("dax_query_mcp.executor.QueryProfiler") as MockProfiler:
            instance = MockProfiler.return_value
            instance.start_phase = MagicMock()
            instance.stop_phase = MagicMock()
            instance.finalize = MagicMock(side_effect=lambda: call_log.append("profiler.finalize"))
            instance.to_response_field = MagicMock(return_value={"timings": {}})

            executor = DAXExecutor(dispatcher=dispatcher)
            config = _make_config()
            df = executor.execute(config, profile=True)

        assert "profiler.finalize" in call_log, "profiler.finalize() was not called"
        finalize_idx = call_log.index("profiler.finalize")
        close_indices = [i for i, c in enumerate(call_log) if "Close" in c]
        assert close_indices, "No Close calls were recorded"
        assert all(finalize_idx < ci for ci in close_indices), (
            f"profiler.finalize (index {finalize_idx}) must happen before "
            f"all Close calls (indices {close_indices}). Log: {call_log}"
        )


# ---------------------------------------------------------------------------
# Error-path cleanup tests
# ---------------------------------------------------------------------------

class TestErrorPathCleanup:
    """Verify cleanup happens even when errors occur at various stages."""

    def test_connection_closed_when_profiler_fails(self):
        """Connection must be closed even if profiler.stop_phase raises."""
        conn = FakeConnection()
        rs = FakeRecordset(fields=["V"], rows=[(1,)])
        cmd = FakeCommand(rs)

        def dispatcher(name: str):
            if name == "ADODB.Connection":
                return conn
            if name == "ADODB.Command":
                return cmd
            raise AssertionError(name)

        with patch("dax_query_mcp.executor.QueryProfiler") as MockProfiler:
            instance = MockProfiler.return_value
            call_count = 0

            def failing_stop(phase):
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    raise RuntimeError("profiler exploded")

            instance.start_phase = MagicMock()
            instance.stop_phase = MagicMock(side_effect=failing_stop)

            executor = DAXExecutor(dispatcher=dispatcher)
            config = _make_config()

            with pytest.raises(Exception):
                executor.execute(config)

        assert conn.closed is True, "Connection must be closed even if profiler fails"

    def test_command_released_when_fetch_fails(self):
        """Command.ActiveConnection must be set to None even if recordset fetch fails."""
        conn = FakeConnection()

        class ExplodingRecordset:
            """Recordset whose Fields property explodes on access."""
            def __init__(self):
                self.closed = False

            @property
            def Fields(self):
                raise RuntimeError("recordset access failed")

            def Close(self):
                self.closed = True

        bad_rs = ExplodingRecordset()
        cmd = FakeCommand.__new__(FakeCommand)
        cmd.ActiveConnection = None
        cmd.CommandText = None
        cmd.CommandTimeout = None
        cmd._recordset = None

        # Override Execute to return our exploding recordset
        cmd.Execute = lambda: (bad_rs,)

        def dispatcher(name: str):
            if name == "ADODB.Connection":
                return conn
            if name == "ADODB.Command":
                return cmd
            raise AssertionError(name)

        executor = DAXExecutor(dispatcher=dispatcher)
        config = _make_config()

        with pytest.raises(Exception):
            executor.execute(config)

        assert cmd.ActiveConnection is None, (
            "Command.ActiveConnection must be None after cleanup"
        )
        assert conn.closed is True, "Connection must be closed even after fetch failure"

    def test_connection_closed_when_execute_raises(self):
        """Connection must be closed when cmd.Execute() raises."""
        conn = FakeConnection()

        class FailingCommand:
            def __init__(self):
                self.ActiveConnection = None
                self.CommandText = None
                self.CommandTimeout = None

            def Execute(self):
                raise RuntimeError("execute failed")

        def dispatcher(name: str):
            if name == "ADODB.Connection":
                return conn
            if name == "ADODB.Command":
                return FailingCommand()
            raise AssertionError(name)

        executor = DAXExecutor(dispatcher=dispatcher)
        config = _make_config()

        with pytest.raises(Exception):
            executor.execute(config)

        assert conn.closed is True, "Connection must be closed when Execute() raises"

    def test_all_resources_closed_on_open_failure(self):
        """No resource leaks when conn.Open() itself fails."""
        class FailingConnection:
            def __init__(self):
                self.ConnectionTimeout = None
                self.CommandTimeout = None
                self.closed = False

            def Open(self, _cs):
                raise RuntimeError("cannot connect")

            def Close(self):
                self.closed = True

        failing_conn = FailingConnection()

        def dispatcher(name: str):
            if name == "ADODB.Connection":
                return failing_conn
            if name == "ADODB.Command":
                return MagicMock()
            raise AssertionError(name)

        executor = DAXExecutor(dispatcher=dispatcher)
        config = _make_config()

        with pytest.raises(Exception):
            executor.execute(config)

        assert failing_conn.closed is True, (
            "Connection must be closed even when Open() fails"
        )


# ---------------------------------------------------------------------------
# Unit tests for _safe_close and _release_command
# ---------------------------------------------------------------------------

class TestSafeCloseAndRelease:
    """Verify the cleanup helpers are robust."""

    def test_safe_close_none(self):
        """_safe_close(None) must be a no-op."""
        _safe_close(None)  # should not raise

    def test_safe_close_swallows_exceptions(self):
        """_safe_close must suppress exceptions from Close()."""
        obj = MagicMock()
        obj.Close.side_effect = RuntimeError("close failed")
        _safe_close(obj)  # should not raise
        obj.Close.assert_called_once()

    def test_release_command_none(self):
        """_release_command(None) must be a no-op."""
        _release_command(None)  # should not raise

    def test_release_command_clears_active_connection(self):
        """_release_command must set ActiveConnection = None."""
        cmd = MagicMock()
        _release_command(cmd)
        assert cmd.ActiveConnection is None
