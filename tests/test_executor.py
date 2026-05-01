from datetime import datetime, timezone
import time

import pandas as pd
import pytest

import dax_query_mcp.executor as executor_module
from dax_query_mcp.executor import (
    DAXExecutor,
    MSOLAP_INSTALL_URL,
    PowerBIRestExecutor,
    _resolve_azure_cli_executable,
    dax_to_pandas,
    redact_connection_string,
)
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


class FakeHttpResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def read(self) -> bytes:
        import json

        return json.dumps(self.payload).encode("utf-8")


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
        def __init__(self, **kwargs):
            pass  # Accept any kwargs from the new signature

        def execute(self, query, *, profile=False):
            captured["query"] = query
            return pd.DataFrame({"Value": [1]})

    monkeypatch.setattr("dax_query_mcp.executor.DAXExecutor", StubExecutor)

    dataframe = dax_to_pandas("EVALUATE ROW(\"Value\", 1)", "Provider=MSOLAP.8;Initial Catalog=model")

    assert list(dataframe.columns) == ["Value"]
    assert captured["query"].command_timeout_seconds == 1800


def test_powerbi_rest_executor_posts_execute_queries() -> None:
    captured = {}

    def opener(request, timeout):
        captured["url"] = request.full_url
        captured["authorization"] = request.get_header("Authorization")
        captured["content_type"] = request.get_header("Content-type")
        captured["timeout"] = timeout
        captured["body"] = request.data.decode("utf-8")
        return FakeHttpResponse(
            {
                "results": [
                    {
                        "tables": [
                            {
                                "rows": [
                                    {"Sales[Amount]": 42, "[Label]": "Actual"},
                                ]
                            }
                        ]
                    }
                ]
            }
        )

    config = DAXQueryConfig(
        name="rest",
        transport="powerbi_rest",
        dataset_id="00000000-0000-0000-0000-000000000000",
        dax_query='EVALUATE ROW("Amount", 42)',
        command_timeout_seconds=45,
    )
    executor = PowerBIRestExecutor(token_getter=lambda _query: "fake-token", opener=opener)

    dataframe = executor.execute(config)

    assert captured["url"] == (
        "https://api.powerbi.com/v1.0/myorg/datasets/"
        "00000000-0000-0000-0000-000000000000/executeQueries"
    )
    assert captured["authorization"] == "Bearer fake-token"
    assert captured["content_type"] == "application/json"
    assert captured["timeout"] == 45
    assert '"includeNulls": true' in captured["body"]
    assert list(dataframe.columns) == ["Amount", "Label"]
    assert dataframe.to_dict(orient="records") == [{"Amount": 42, "Label": "Actual"}]


def test_powerbi_rest_executor_rejects_workspace_scoped_base_url() -> None:
    config = DAXQueryConfig(
        name="rest",
        transport="powerbi_rest",
        dataset_id="00000000-0000-0000-0000-000000000000",
        dax_query='EVALUATE ROW("Amount", 42)',
        api_base_url="https://api.powerbi.com/v1.0/myorg/groups/00000000-0000-0000-0000-000000000000",
    )
    executor = PowerBIRestExecutor(token_getter=lambda _query: "fake-token")

    with pytest.raises(DAXExecutionError, match="dataset-only executeQueries endpoint"):
        executor.execute(config)


def test_powerbi_rest_executor_uses_env_token(monkeypatch) -> None:
    monkeypatch.setenv("TEST_POWERBI_TOKEN", "env-token")
    captured = {}

    def opener(request, timeout):
        captured["authorization"] = request.get_header("Authorization")
        return FakeHttpResponse({"results": [{"tables": [{"rows": [{"[Value]": 1}]}]}]})

    config = DAXQueryConfig(
        name="rest-env",
        transport="powerbi_rest",
        dataset_id="00000000-0000-0000-0000-000000000000",
        auth_mode="env",
        access_token_env="TEST_POWERBI_TOKEN",
        dax_query='EVALUATE ROW("Value", 1)',
    )

    dataframe = PowerBIRestExecutor(opener=opener).execute(config)

    assert captured["authorization"] == "Bearer env-token"
    assert dataframe["Value"].iloc[0] == 1


def test_resolve_azure_cli_executable_uses_env_override(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_CLI_PATH", r"C:\custom\az.cmd")

    assert _resolve_azure_cli_executable() == r"C:\custom\az.cmd"


def test_resolve_azure_cli_executable_uses_standard_windows_path(monkeypatch) -> None:
    standard_path = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
    monkeypatch.delenv("AZURE_CLI_PATH", raising=False)
    monkeypatch.setattr(executor_module.sys, "platform", "win32")
    monkeypatch.setattr("dax_query_mcp.executor.shutil.which", lambda executable: None)
    monkeypatch.setattr("dax_query_mcp.executor.os.path.isfile", lambda path: path == standard_path)

    assert _resolve_azure_cli_executable() == standard_path


def test_resolve_azure_cli_executable_prefers_az_cmd_on_windows(monkeypatch) -> None:
    monkeypatch.delenv("AZURE_CLI_PATH", raising=False)
    monkeypatch.setattr(executor_module.sys, "platform", "win32")

    def fake_which(executable: str) -> str | None:
        if executable == "az":
            return r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az"
        if executable == "az.cmd":
            return r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
        return None

    monkeypatch.setattr("dax_query_mcp.executor.shutil.which", fake_which)

    assert _resolve_azure_cli_executable() == r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"


def test_resolve_azure_cli_executable_raises_clear_error(monkeypatch) -> None:
    monkeypatch.delenv("AZURE_CLI_PATH", raising=False)
    monkeypatch.setattr("dax_query_mcp.executor.shutil.which", lambda executable: None)
    monkeypatch.setattr("dax_query_mcp.executor.os.path.isfile", lambda path: False)

    try:
        _resolve_azure_cli_executable()
    except DAXExecutionError as exc:
        assert "Azure CLI executable not found" in str(exc)
        assert "AZURE_CLI_PATH" in str(exc)
    else:
        raise AssertionError("Expected DAXExecutionError")


def test_powerbi_rest_executor_raises_payload_errors() -> None:
    config = DAXQueryConfig(
        name="rest-error",
        transport="powerbi_rest",
        dataset_id="00000000-0000-0000-0000-000000000000",
        dax_query="EVALUATE Bad",
    )
    executor = PowerBIRestExecutor(
        token_getter=lambda _query: "fake-token",
        opener=lambda request, timeout: FakeHttpResponse(
            {"results": [{"error": {"code": "DAXQueryFailure", "message": "Bad DAX"}}]}
        ),
    )

    try:
        executor.execute(config)
    except DAXExecutionError as exc:
        assert "DAXQueryFailure" in str(exc)
        assert "Bad DAX" in str(exc)
    else:
        raise AssertionError("Expected DAXExecutionError")


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

