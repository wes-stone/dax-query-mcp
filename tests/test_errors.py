"""Tests for structured error responses in MCP tools."""

from __future__ import annotations

import json

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from dax_query_mcp.errors import (
    ADMIN_QUERY_BLOCKED,
    CONNECTION_NOT_FOUND,
    EXECUTION_FAILED,
    INVALID_PARAMS,
    QUERY_TIMEOUT,
    admin_query_blocked,
    connection_not_found,
    execution_failed,
    invalid_params,
    query_timeout,
    structured_error,
    structured_tool_error,
)
from dax_query_mcp.exceptions import DAXExecutionError
from dax_query_mcp.mcp_server import (
    _get_connection,
    save_query_builder,
    validate_dax_query,
    run_connection_query,
    run_ad_hoc_query,
    copy_to_clipboard,
)


# ---------------------------------------------------------------------------
# errors.py — structured_error helpers
# ---------------------------------------------------------------------------


class TestStructuredErrorPayload:
    """The base structured_error dict has the required schema."""

    def test_schema_fields(self):
        payload = structured_error("CODE", "msg", "hint", {"k": "v"})
        assert payload == {
            "error_code": "CODE",
            "message": "msg",
            "suggestion": "hint",
            "details": {"k": "v"},
        }

    def test_details_default_to_empty_dict(self):
        payload = structured_error("CODE", "msg", "hint")
        assert payload["details"] == {}

    def test_structured_tool_error_is_tool_error(self):
        err = structured_tool_error("CODE", "msg", "hint")
        assert isinstance(err, ToolError)

    def test_structured_tool_error_json_roundtrip(self):
        err = structured_tool_error("CODE", "msg", "hint", {"x": 1})
        payload = json.loads(str(err))
        assert payload["error_code"] == "CODE"
        assert payload["details"]["x"] == 1


# ---------------------------------------------------------------------------
# errors.py — factory helpers
# ---------------------------------------------------------------------------


class TestAdminQueryBlockedError:
    def test_error_code_and_pattern(self):
        err = admin_query_blocked("INFO.")
        payload = json.loads(str(err))
        assert payload["error_code"] == ADMIN_QUERY_BLOCKED
        assert "INFO." in payload["message"]
        assert payload["details"]["blocked_pattern"] == "INFO."
        assert "get_connection_context" in payload["suggestion"]


class TestConnectionNotFoundError:
    def test_includes_available_connections(self):
        err = connection_not_found("bad", "Connections", ["sales", "finance"])
        payload = json.loads(str(err))
        assert payload["error_code"] == CONNECTION_NOT_FOUND
        assert payload["details"]["connection_name"] == "bad"
        assert "sales" in payload["details"]["available_connections"]
        assert "sales" in payload["suggestion"]

    def test_no_available_connections(self):
        err = connection_not_found("bad", "Connections")
        payload = json.loads(str(err))
        assert payload["details"].get("available_connections") is None
        assert "list_connections" in payload["suggestion"]


class TestQueryTimeoutError:
    def test_timeout_details(self):
        err = query_timeout("EVALUATE ...", 300, TimeoutError("timed out"))
        payload = json.loads(str(err))
        assert payload["error_code"] == QUERY_TIMEOUT
        assert payload["details"]["timeout_seconds"] == 300
        assert "timed out" in payload["details"]["original_error"]
        assert "simplify" in payload["suggestion"].lower()


class TestExecutionFailedError:
    def test_execution_details(self):
        err = execution_failed("EVALUATE X", RuntimeError("bad column"))
        payload = json.loads(str(err))
        assert payload["error_code"] == EXECUTION_FAILED
        assert "bad column" in payload["message"]
        assert "EVALUATE X" in payload["details"]["query_preview"]
        assert "get_connection_context" in payload["suggestion"]


class TestInvalidParamsError:
    def test_custom_details(self):
        err = invalid_params("missing field", "provide the field", field="name")
        payload = json.loads(str(err))
        assert payload["error_code"] == INVALID_PARAMS
        assert payload["details"]["field"] == "name"


# ---------------------------------------------------------------------------
# mcp_server.py — validate_dax_query
# ---------------------------------------------------------------------------


class TestValidateDaxQueryStructuredErrors:
    """validate_dax_query raises ToolError with structured JSON."""

    @pytest.mark.parametrize(
        "query,expected_pattern",
        [
            ("SELECT * FROM INFO.TABLES()", "INFO"),
            ("SELECT * FROM $SYSTEM.DISCOVER_XML_METADATA", "$SYSTEM.DISCOVER_"),
            ("DBCC CLEARMETRICS", "DBCC"),
            ("ALTER TABLE Foo ...", "ALTER"),
            ("CREATE MEMBER ...", "CREATE"),
            ("DELETE FROM Foo", "DELETE"),
            ("DROP MEMBER ...", "DROP"),
        ],
    )
    def test_admin_query_returns_structured_error(self, query, expected_pattern):
        with pytest.raises(ToolError) as exc_info:
            validate_dax_query(query)
        payload = json.loads(str(exc_info.value))
        assert payload["error_code"] == ADMIN_QUERY_BLOCKED
        assert "blocked_pattern" in payload["details"]
        assert "get_connection_context" in payload["suggestion"]

    def test_safe_mdschema_allowed(self):
        validate_dax_query("SELECT * FROM $SYSTEM.MDSCHEMA_CUBES")

    def test_normal_evaluate_allowed(self):
        validate_dax_query("EVALUATE ROW(\"Revenue\", 42)")


# ---------------------------------------------------------------------------
# mcp_server.py — _get_connection
# ---------------------------------------------------------------------------


class TestGetConnectionStructuredErrors:
    def test_missing_connection_returns_structured_error(self, tmp_path):
        connections_dir = tmp_path / "Connections"
        connections_dir.mkdir()
        (connections_dir / "sales.yaml").write_text(
            "connection_string: 'Provider=MSOLAP;'\ndescription: 'Sales'\n",
            encoding="utf-8",
        )

        with pytest.raises(ToolError) as exc_info:
            _get_connection("nonexistent", str(connections_dir))

        payload = json.loads(str(exc_info.value))
        assert payload["error_code"] == CONNECTION_NOT_FOUND
        assert payload["details"]["connection_name"] == "nonexistent"
        assert "sales" in payload["details"]["available_connections"]
        assert "list_connections" in payload["suggestion"]


# ---------------------------------------------------------------------------
# mcp_server.py — save_query_builder
# ---------------------------------------------------------------------------


class TestSaveQueryBuilderStructuredErrors:
    def test_empty_queries_dir(self):
        with pytest.raises(ToolError) as exc_info:
            save_query_builder('{"name":"test"}', queries_dir="")

        payload = json.loads(str(exc_info.value))
        assert payload["error_code"] == INVALID_PARAMS
        assert payload["details"]["parameter"] == "queries_dir"

    def test_invalid_payload(self):
        with pytest.raises(ToolError) as exc_info:
            save_query_builder(
                '{"name":"bad","connection_name":"x","columns":[""]}',
                queries_dir="test_queries",
            )

        payload = json.loads(str(exc_info.value))
        assert payload["error_code"] == INVALID_PARAMS
        assert "get_query_builder_schema" in payload["suggestion"]


# ---------------------------------------------------------------------------
# mcp_server.py — execution error wrapping
# ---------------------------------------------------------------------------


class TestExecutionErrorWrapping:
    """Execution failures are wrapped as structured errors."""

    def test_run_connection_query_execution_failure(self, monkeypatch, tmp_path):
        connections_dir = tmp_path / "Connections"
        connections_dir.mkdir()
        (connections_dir / "sales.yaml").write_text(
            "connection_string: 'Provider=MSOLAP;'\ndescription: 'Sales'\n",
            encoding="utf-8",
        )

        def _fail(**kwargs):
            raise DAXExecutionError("Column 'Foo' not found")

        monkeypatch.setattr("dax_query_mcp.mcp_server.dax_to_pandas", _fail)

        with pytest.raises(ToolError) as exc_info:
            run_connection_query(
                connection_name="sales",
                query="EVALUATE Foo",
                connections_dir=str(connections_dir),
            )

        payload = json.loads(str(exc_info.value))
        assert payload["error_code"] == EXECUTION_FAILED
        assert "Foo" in payload["message"]
        assert "get_connection_context" in payload["suggestion"]

    def test_run_connection_query_timeout(self, monkeypatch, tmp_path):
        connections_dir = tmp_path / "Connections"
        connections_dir.mkdir()
        (connections_dir / "sales.yaml").write_text(
            "connection_string: 'Provider=MSOLAP;'\ndescription: 'Sales'\n",
            encoding="utf-8",
        )

        def _timeout(**kwargs):
            raise DAXExecutionError("query timed out after 300s")

        monkeypatch.setattr("dax_query_mcp.mcp_server.dax_to_pandas", _timeout)

        with pytest.raises(ToolError) as exc_info:
            run_connection_query(
                connection_name="sales",
                query="EVALUATE BigTable",
                connections_dir=str(connections_dir),
            )

        payload = json.loads(str(exc_info.value))
        assert payload["error_code"] == QUERY_TIMEOUT
        assert "simplify" in payload["suggestion"].lower()

    def test_run_ad_hoc_query_execution_failure(self, monkeypatch):
        def _fail(**kwargs):
            raise DAXExecutionError("syntax error")

        monkeypatch.setattr("dax_query_mcp.mcp_server.dax_to_pandas", _fail)

        with pytest.raises(ToolError) as exc_info:
            run_ad_hoc_query(
                connection_string="Provider=MSOLAP;Data Source=localhost",
                query="EVALUATE BadSyntax(",
            )

        payload = json.loads(str(exc_info.value))
        assert payload["error_code"] == EXECUTION_FAILED

    def test_copy_to_clipboard_invalid_format(self):
        with pytest.raises(ToolError) as exc_info:
            copy_to_clipboard(
                connection_name="sales",
                query="EVALUATE ROW(1,1)",
                format="csv",
            )

        payload = json.loads(str(exc_info.value))
        assert payload["error_code"] == INVALID_PARAMS
        assert payload["details"]["provided"] == "csv"
        assert "tsv" in payload["suggestion"]
