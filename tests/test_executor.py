from datetime import datetime, timezone
import time

import pandas as pd

from dax_query_mcp.executor import DAXExecutor, MSOLAP_INSTALL_URL, dax_to_pandas, redact_connection_string
from dax_query_mcp.exceptions import DAXExecutionError
from dax_query_mcp.models import DAXQueryConfig


class FakeField:
    def __init__(self, name: str, value: object = None):
        self.Name = name
        self.Value = value


class FakeRecordset:
    def __init__(self, fields: list[str], rows: list[tuple[object, ...]]):
        self.Fields = [FakeField(name) for name in fields]
        self._rows = rows
        self._index = 0
        self.closed = False
        self._sync_field_values()

    @property
    def EOF(self) -> bool:
        return self._index >= len(self._rows)

    def MoveNext(self) -> None:
        self._index += 1
        self._sync_field_values()

    def _sync_field_values(self) -> None:
        if not self.EOF:
            for i, field in enumerate(self.Fields):
                field.Value = self._rows[self._index][i]

    def Close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, open_error: Exception | None = None):
        self.ConnectionTimeout = None
        self.CommandTimeout = None
        self.closed = False
        self.opened_with = None
        self._open_error = open_error

    def Open(self, connection_string: str):
        if self._open_error is not None:
            raise self._open_error
        self.opened_with = connection_string

    def Close(self):
        self.closed = True


class FakeCommand:
    def __init__(self, recordset: FakeRecordset | None, should_fail: bool = False):
        self.ActiveConnection = None
        self.CommandText = None
        self.CommandTimeout = None
        self._recordset = recordset
        self._should_fail = should_fail

    def Execute(self):
        if self._should_fail:
            raise RuntimeError("boom")
        return (self._recordset,)


def test_executor_builds_dataframe_and_closes_resources() -> None:
    recordset = FakeRecordset(
        fields=["Account Information[TPID]", "Amount", "Occurred"],
        rows=[
            ("123", "42.5", datetime(2026, 1, 1, tzinfo=timezone.utc)),
            ("456", "99.0", datetime(2026, 1, 2, tzinfo=timezone.utc)),
        ],
    )
    connection = FakeConnection()
    command = FakeCommand(recordset)

    def dispatcher(name: str):
        if name == "ADODB.Connection":
            return connection
        if name == "ADODB.Command":
            return command
        raise AssertionError(name)

    executor = DAXExecutor(dispatcher=dispatcher)
    config = DAXQueryConfig(
        name="sample",
        connection_string="Provider=MSOLAP.8;Password=secret;Initial Catalog=model",
        dax_query="EVALUATE ROW(\"Value\", 1)",
        command_timeout_seconds=120,
        max_rows=1,
    )

    dataframe = executor.execute(config)

    assert list(dataframe.columns) == ["TPID", "Amount", "Occurred"]
    assert dataframe.to_dict(orient="records") == [
        {"TPID": 123, "Amount": 42.5, "Occurred": pd.Timestamp("2026-01-01 00:00:00")}
    ]
    assert connection.closed is True
    assert recordset.closed is True
    assert connection.ConnectionTimeout == 300
    assert connection.CommandTimeout == 120
    assert command.CommandTimeout == 120


def test_executor_wraps_errors_and_closes_connection() -> None:
    connection = FakeConnection()
    command = FakeCommand(recordset=None, should_fail=True)

    def dispatcher(name: str):
        if name == "ADODB.Connection":
            return connection
        if name == "ADODB.Command":
            return command
        raise AssertionError(name)

    executor = DAXExecutor(dispatcher=dispatcher)
    config = DAXQueryConfig(
        name="broken",
        connection_string="Provider=MSOLAP.8;Pwd=secret",
        dax_query="EVALUATE ROW(\"Value\", 1)",
    )

    try:
        executor.execute(config)
    except DAXExecutionError as exc:
        assert "broken" in str(exc)
    else:
        raise AssertionError("Expected DAXExecutionError")

    assert connection.closed is True


def test_redact_connection_string_masks_sensitive_keys() -> None:
    redacted = redact_connection_string(
        "Provider=MSOLAP.8;User ID=me@example.com;Password=hunter2;Initial Catalog=Model"
    )

    assert "Password=***" in redacted
    assert "User ID=***" in redacted
    assert "hunter2" not in redacted


def test_executor_adds_msolap_install_hint_for_missing_provider() -> None:
    connection = FakeConnection(open_error=RuntimeError("The 'MSOLAP.8' provider is not registered on the local machine."))

    def dispatcher(name: str):
        if name == "ADODB.Connection":
            return connection
        raise AssertionError(name)

    executor = DAXExecutor(dispatcher=dispatcher)
    config = DAXQueryConfig(
        name="missing-provider",
        connection_string="Provider=MSOLAP.8;Initial Catalog=model",
        dax_query="EVALUATE ROW(\"Value\", 1)",
    )

    try:
        executor.execute(config)
    except DAXExecutionError as exc:
        assert MSOLAP_INSTALL_URL in str(exc)
        assert "MSOLAP / Analysis Services client libraries" in str(exc)
    else:
        raise AssertionError("Expected DAXExecutionError")


def test_dax_to_pandas_uses_executor_defaults(monkeypatch) -> None:
    captured = {}

    class StubExecutor:
        def execute(self, query):
            captured["query"] = query
            return pd.DataFrame({"Value": [1]})

    monkeypatch.setattr("dax_query_mcp.executor.DAXExecutor", lambda: StubExecutor())

    dataframe = dax_to_pandas("EVALUATE ROW(\"Value\", 1)", "Provider=MSOLAP.8;Initial Catalog=model")

    assert list(dataframe.columns) == ["Value"]
    assert captured["query"].command_timeout_seconds == 1800


def _make_executor_and_config(recordset: FakeRecordset, name: str = "test", max_rows: int | None = None):
    """Helper that wires a FakeRecordset into a DAXExecutor."""
    connection = FakeConnection()
    command = FakeCommand(recordset)

    def dispatcher(prog_id: str):
        if prog_id == "ADODB.Connection":
            return connection
        if prog_id == "ADODB.Command":
            return command
        raise AssertionError(prog_id)

    executor = DAXExecutor(dispatcher=dispatcher)
    config = DAXQueryConfig(
        name=name,
        connection_string="Provider=MSOLAP.8;Initial Catalog=model",
        dax_query='EVALUATE ROW("Value", 1)',
        max_rows=max_rows,
    )
    return executor, config


def test_streaming_fetch_yields_all_rows() -> None:
    """Incremental MoveNext iteration must return every row."""
    num_rows = 50
    recordset = FakeRecordset(
        fields=["ID", "Value"],
        rows=[(i, float(i)) for i in range(num_rows)],
    )
    executor, config = _make_executor_and_config(recordset, name="all-rows")

    dataframe = executor.execute(config)

    assert len(dataframe) == num_rows
    assert list(dataframe.columns) == ["ID", "Value"]
    assert dataframe["ID"].iloc[0] == 0
    assert dataframe["ID"].iloc[-1] == num_rows - 1


def test_streaming_respects_max_rows() -> None:
    """max_rows must stop iteration after the requested number of rows."""
    recordset = FakeRecordset(
        fields=["ID"],
        rows=[(i,) for i in range(10)],
    )
    executor, config = _make_executor_and_config(recordset, name="max-rows", max_rows=3)

    dataframe = executor.execute(config)

    assert len(dataframe) == 3
    assert list(dataframe["ID"]) == [0, 1, 2]


def test_streaming_empty_recordset_returns_empty_dataframe() -> None:
    """An empty recordset (EOF at start) must produce an empty DataFrame with correct columns."""
    recordset = FakeRecordset(fields=["ID", "Label"], rows=[])
    executor, config = _make_executor_and_config(recordset, name="empty")

    dataframe = executor.execute(config)

    assert len(dataframe) == 0
    assert list(dataframe.columns) == ["ID", "Label"]


def test_streaming_benchmark_mock_recordset() -> None:
    """Benchmark: incremental MoveNext fetch must complete quickly for a large mock recordset."""
    num_rows = 10_000
    recordset = FakeRecordset(
        fields=["ID", "Amount", "Label"],
        rows=[(i, float(i) * 1.5, f"row_{i}") for i in range(num_rows)],
    )
    executor, config = _make_executor_and_config(recordset, name="benchmark")

    start = time.perf_counter()
    dataframe = executor.execute(config)
    elapsed = time.perf_counter() - start

    assert len(dataframe) == num_rows
    assert elapsed < 2.0, f"Streaming fetch took {elapsed:.2f}s – too slow"

