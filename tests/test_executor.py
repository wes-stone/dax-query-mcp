from datetime import datetime, timezone

import pandas as pd

from dax_query_mcp.executor import DAXExecutor, MSOLAP_INSTALL_URL, dax_to_pandas, redact_connection_string
from dax_query_mcp.exceptions import DAXExecutionError
from dax_query_mcp.models import DAXQueryConfig


class FakeField:
    def __init__(self, name: str):
        self.Name = name


class FakeRecordset:
    def __init__(self, fields: list[str], rows: list[tuple[object, ...]]):
        self.Fields = [FakeField(name) for name in fields]
        self._rows = rows
        self.closed = False

    def GetRows(self, max_rows=None):
        rows = self._rows if max_rows is None else self._rows[:max_rows]
        if not rows:
            return []
        return [tuple(row[index] for row in rows) for index in range(len(self.Fields))]

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

