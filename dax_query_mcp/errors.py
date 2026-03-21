"""Structured error responses for MCP tools.

Each error is a JSON-serializable dict with fields that help LLMs
self-correct without additional user guidance:

    {
        "error_code": str,      # machine-readable error code
        "message": str,         # human-readable description
        "suggestion": str,      # actionable fix for the LLM
        "details": dict,        # extra context (patterns, names, etc.)
    }
"""

from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp.exceptions import ToolError

# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------

ADMIN_QUERY_BLOCKED = "ADMIN_QUERY_BLOCKED"
CONNECTION_NOT_FOUND = "CONNECTION_NOT_FOUND"
QUERY_TIMEOUT = "QUERY_TIMEOUT"
EXECUTION_FAILED = "EXECUTION_FAILED"
INVALID_PARAMS = "INVALID_PARAMS"


def structured_error(
    error_code: str,
    message: str,
    suggestion: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a structured error payload."""
    return {
        "error_code": error_code,
        "message": message,
        "suggestion": suggestion,
        "details": details or {},
    }


def structured_tool_error(
    error_code: str,
    message: str,
    suggestion: str,
    details: dict[str, Any] | None = None,
) -> ToolError:
    """Return a ``ToolError`` whose text is a structured JSON error payload."""
    payload = structured_error(error_code, message, suggestion, details)
    return ToolError(json.dumps(payload, indent=2))


# ---------------------------------------------------------------------------
# Pre-built factory helpers
# ---------------------------------------------------------------------------


def admin_query_blocked(blocked_pattern: str) -> ToolError:
    """Query uses admin-only syntax that the server rejects."""
    return structured_tool_error(
        error_code=ADMIN_QUERY_BLOCKED,
        message=f"Query uses admin-required syntax ({blocked_pattern})",
        suggestion=(
            "Use get_connection_context to discover tables and measures instead. "
            "Avoid INFO.*(), $SYSTEM.DISCOVER_*, DBCC, ALTER, CREATE, DELETE, and DROP."
        ),
        details={"blocked_pattern": blocked_pattern},
    )


def connection_not_found(
    connection_name: str,
    connections_dir: str,
    available: list[str] | None = None,
) -> ToolError:
    """The requested connection name does not exist."""
    details: dict[str, Any] = {
        "connection_name": connection_name,
        "connections_dir": connections_dir,
    }
    if available is not None:
        details["available_connections"] = available

    suggestion = "Call list_connections to see available connection names."
    if available:
        suggestion += f" Available: {', '.join(available)}"

    return structured_tool_error(
        error_code=CONNECTION_NOT_FOUND,
        message=f"Connection '{connection_name}' was not found in '{connections_dir}'.",
        suggestion=suggestion,
        details=details,
    )


def query_timeout(query: str, timeout_seconds: int, exc: Exception) -> ToolError:
    """Query execution exceeded the configured timeout."""
    return structured_tool_error(
        error_code=QUERY_TIMEOUT,
        message=f"Query timed out after {timeout_seconds}s.",
        suggestion=(
            "Simplify the query, add filters to reduce the result set, "
            "or increase command_timeout_seconds."
        ),
        details={
            "timeout_seconds": timeout_seconds,
            "query_preview": query[:200],
            "original_error": str(exc),
        },
    )


def execution_failed(query: str, exc: Exception) -> ToolError:
    """General DAX execution failure."""
    return structured_tool_error(
        error_code=EXECUTION_FAILED,
        message=f"DAX query execution failed: {exc}",
        suggestion=(
            "Check the query syntax. Call get_connection_context to verify "
            "table and column names, then retry with a corrected query."
        ),
        details={
            "query_preview": query[:200],
            "original_error": str(exc),
        },
    )


def invalid_params(message: str, suggestion: str, **details: Any) -> ToolError:
    """Invalid or missing tool parameters."""
    return structured_tool_error(
        error_code=INVALID_PARAMS,
        message=message,
        suggestion=suggestion,
        details=details,
    )
