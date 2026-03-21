from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from .connections import load_connections, resolve_connections_dir
from .executor import dax_to_pandas
from .formatting import DEFAULT_DATE_FORMAT, dataframe_to_markdown, preview_records
from .query_builder import (
    load_query_builder_artifacts,
    query_builder_from_dict,
    query_builder_schema_payload,
    query_builder_to_payload,
    save_query_builder_artifacts,
)
from .scaffold import scaffold_workspace

DEFAULT_CONNECTIONS_DIR = str(resolve_connections_dir(os.getenv("DAX_QUERY_MCP_CONNECTIONS_DIR")))
DEFAULT_PREVIEW_ROWS = 10
_MARKDOWN_PRESENTATION_HINT = "When presenting these results to the user, render preview data as a markdown table."

_ROWSET_COLUMNS: dict[str, list[str]] = {
    "cubes": ["CUBE_NAME", "DESCRIPTION"],
    "dimensions": ["CUBE_NAME", "DIMENSION_NAME", "DESCRIPTION"],
    "hierarchies": ["CUBE_NAME", "HIERARCHY_NAME", "DESCRIPTION"],
    "levels": ["CUBE_NAME", "LEVEL_NAME", "DESCRIPTION"],
    "measures": ["CUBE_NAME", "MEASURE_NAME", "DESCRIPTION"],
}

_ADMIN_QUERY_PATTERNS = re.compile(
    r"""
      \bINFO\s*\.               # INFO.*() DMV functions
    | \$SYSTEM\.DISCOVER_       # $SYSTEM.DISCOVER_* DMV rowsets
    | \bDBCC\b                  # DBCC commands
    | \bALTER\b                 # DDL: ALTER
    | \bCREATE\b                # DDL: CREATE
    | \bDELETE\b                # DDL: DELETE
    | \bDROP\b                  # DDL: DROP
    """,
    re.IGNORECASE | re.VERBOSE,
)

_SAFE_SYSTEM_PREFIXES = (
    "$SYSTEM.MDSCHEMA_",
)

_NEXT_STEPS = [
    "Filter / refine — narrow to a specific account, TPID, or time range",
    "Aggregate — total by month, by account, etc.",
    "Export as CSV — save results to a CSV file",
    "Save to DAX Studio — save as a .dax query builder file (I will ask you where to save)",
    "Scaffold Python workspace — export to a standalone Python project (I will ask you where to save)",
]

_SERVER_INSTRUCTIONS = """\
You are connected to the dax-query-server, which runs DAX queries against \
Power BI / Analysis Services semantic models.

RULES — follow these every time:

1. ALWAYS EXECUTE queries — when the user asks for a DAX query or example, \
do NOT just show the query text and ask if they want to run it. Build the \
query AND run it with run_connection_query in the same turn so the user \
sees both the query and the resulting data table.

2. After EVERY query result, you MUST render the markdown_table field as an \
actual markdown table for the user. Do NOT summarize, paraphrase, or describe \
the data in words — SHOW THE TABLE. Then render the next_steps list as a \
numbered markdown list. This is mandatory on every single query response.

3. NEVER generate admin-required queries: INFO.*(), $SYSTEM.DISCOVER_*, \
DBCC, ALTER, CREATE, DELETE, or DROP. They will be rejected. Use \
get_connection_context or inspect_connection for metadata instead.

4. Before writing any DAX query, call get_connection_context to learn the \
available tables, columns, measures, and filters for the connection.
"""

mcp = FastMCP("dax-query-server", instructions=_SERVER_INSTRUCTIONS)


def validate_dax_query(query: str) -> None:
    """Reject queries that require admin privileges or perform DDL.

    Allows safe $SYSTEM.MDSCHEMA_* rowsets used by inspect_connection.
    Raises ToolError with a helpful message so the LLM can self-correct.
    """
    upper = query.strip().upper()
    for prefix in _SAFE_SYSTEM_PREFIXES:
        if prefix.upper() in upper:
            return

    if _ADMIN_QUERY_PATTERNS.search(query):
        raise ToolError(
            "This query uses admin-required syntax (INFO.*(), $SYSTEM.DISCOVER_*, "
            "DBCC, ALTER, CREATE, DELETE, or DROP) which will fail without admin "
            "privileges. Use get_connection_context to discover tables, columns, "
            "and measures instead."
        )


@mcp.tool()
def list_connections(connections_dir: str = DEFAULT_CONNECTIONS_DIR) -> str:
    """List configured connections. Call get_connection_context on any connection
    with has_context_markdown=true before writing DAX queries.
    """
    connections = load_connections(connections_dir)
    payload = {
        "connections_dir": str(resolve_connections_dir(connections_dir)),
        "connection_count": len(connections),
        "connections": [
            {
                "name": connection.name,
                "description": connection.description,
                "suggested_skill": connection.suggested_skill,
                "suggested_skill_reason": connection.suggested_skill_reason,
                "has_context_markdown": connection.context_markdown is not None,
                "context_path": connection.context_path,
            }
            for connection in connections.values()
        ],
    }
    return _to_json(payload)


@mcp.tool()
def get_connection_context(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Return metadata and curated markdown context for a named connection.

    This is the PRIMARY way to discover tables, columns, measures, and filters.
    Always call this FIRST before writing any DAX query.
    """
    connection = _get_connection(connection_name, connections_dir)
    payload = {
        "connection_name": connection.name,
        "description": connection.description,
        "suggested_skill": connection.suggested_skill,
        "suggested_skill_reason": connection.suggested_skill_reason,
        "context_path": connection.context_path,
        "context_markdown": connection.context_markdown,
        "NEXT_ACTION": (
            "You now have the schema. Compose a DAX query AND immediately "
            "execute it using run_connection_query in the SAME turn. "
            "Do NOT just display the query text to the user — run it so "
            "they see the actual data table."
        ),
    }
    return _to_json(payload)


@mcp.tool()
def run_connection_query(
    connection_name: str,
    query: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    max_rows: int | None = None,
) -> str:
    """Run a DAX query against a named connection and return a preview.

    Present the markdown_table as a table and the next_steps list as a
    numbered list after every result.
    """
    validate_dax_query(query)
    connection = _get_connection(connection_name, connections_dir)
    dataframe = dax_to_pandas(
        dax_query=query,
        conn_str=connection.connection_string,
        connection_timeout_seconds=connection.connection_timeout_seconds,
        command_timeout_seconds=connection.command_timeout_seconds,
        max_rows=max_rows or connection.max_rows,
    )

    summary = summarize_dataframe(dataframe, preview_rows=preview_rows)
    payload = {
        "connection_name": connection_name,
        "connections_dir": str(resolve_connections_dir(connections_dir)),
        "presentation_hint": _MARKDOWN_PRESENTATION_HINT,
        "markdown_table": summary["markdown_table"],
        "response_markdown": _build_query_response_markdown(
            title=f"Query preview for `{connection_name}`",
            summary=summary,
        ),
        "next_steps": _NEXT_STEPS,
        "summary": summary,
    }
    return _to_json(payload)


@mcp.tool()
def run_connection_query_markdown(
    connection_name: str,
    query: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    max_rows: int | None = None,
) -> str:
    """Run a DAX query and return a ready-to-present markdown preview.

    Present the returned markdown EXACTLY as-is, including the next_steps list.
    """
    validate_dax_query(query)
    payload = json.loads(
        run_connection_query(
            connection_name=connection_name,
            query=query,
            connections_dir=connections_dir,
            preview_rows=preview_rows,
            max_rows=max_rows,
        )
    )
    return str(payload["response_markdown"])


@mcp.tool()
def get_query_builder_schema(connection_name: str = "your_connection") -> str:
    """Return the expected JSON shape and a copyable example payload for save_query_builder."""
    return _to_json(query_builder_schema_payload(connection_name=connection_name))


@mcp.tool()
def save_query_builder(
    query_builder_json: str,
    queries_dir: str = "",
    overwrite: bool = False,
) -> str:
    """Save .dax and .dax.queryBuilder artifacts from a structured query builder JSON payload.

    STOP — before calling this tool you MUST:
    1. Ask the user: "Where should I save the query files?" and wait for their answer.
    2. Call get_query_builder_schema to see the required JSON shape.
    3. Use the user's answer as queries_dir. If queries_dir is empty this tool will error.

    The JSON payload requires these fields:
       - "name": a slug for the query (e.g. "copilot_acr")
       - "connection_name": the connection to use
       - "columns": list of column expressions like "'Calendar'[Fiscal Month]"
       - "measures": list of {caption, expression} objects
       - "filters": list of filter definitions
       - "order_by": list of sort definitions
    """
    if not queries_dir.strip():
        raise ValueError(
            "queries_dir is required — ask the user where to save before calling this tool."
        )
    try:
        definition = query_builder_from_dict(json.loads(query_builder_json))
    except ValueError as exc:
        raise ValueError(
            f"{exc}. Call get_query_builder_schema first for a valid payload template."
        ) from exc
    payload = save_query_builder_artifacts(definition, queries_dir=queries_dir, overwrite=overwrite)
    return _to_json(payload)


@mcp.tool()
def get_query_builder(
    query_name: str,
    queries_dir: str = "queries",
) -> str:
    """Load a saved query builder definition and generated DAX text by query name."""
    definition, dax_query = load_query_builder_artifacts(query_name, queries_dir=queries_dir)
    payload = {
        "query_name": query_name,
        "queries_dir": str(Path(queries_dir)),
        "query_builder": query_builder_to_payload(definition),
        "dax_query": dax_query,
    }
    return _to_json(payload)


@mcp.tool()
def inspect_connection(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    command_timeout_seconds: int | None = None,
) -> str:
    """Inspect model metadata using non-admin MDSCHEMA rowsets.

    Prefer get_connection_context first — it returns curated documentation
    without hitting the server. Use this only for live schema discovery.
    """
    return _to_json(
        inspect_connection_metadata(
            connection_name=connection_name,
            connections_dir=connections_dir,
            preview_rows=preview_rows,
            command_timeout_seconds=command_timeout_seconds,
        )
    )


def inspect_connection_metadata(
    connection_name: str,
    *,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    command_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Return connection metadata as a Python payload for CLI and MCP callers."""
    connection = _get_connection(connection_name, connections_dir)
    effective_timeout = (
        connection.command_timeout_seconds if command_timeout_seconds is None else command_timeout_seconds
    )
    rowsets = {
        "cubes": "SELECT * FROM $SYSTEM.MDSCHEMA_CUBES",
        "dimensions": "SELECT * FROM $SYSTEM.MDSCHEMA_DIMENSIONS",
        "hierarchies": "SELECT * FROM $SYSTEM.MDSCHEMA_HIERARCHIES",
        "levels": "SELECT * FROM $SYSTEM.MDSCHEMA_LEVELS",
        "measures": "SELECT * FROM $SYSTEM.MDSCHEMA_MEASURES",
    }
    results: dict[str, Any] = {
        "connection_name": connection_name,
        "presentation_hint": _MARKDOWN_PRESENTATION_HINT,
    }

    for name, rowset_query in rowsets.items():
        try:
            dataframe = dax_to_pandas(
                dax_query=rowset_query,
                conn_str=connection.connection_string,
                connection_timeout_seconds=connection.connection_timeout_seconds,
                command_timeout_seconds=effective_timeout,
            )
            results[name] = summarize_rowset(
                dataframe,
                preview_rows=preview_rows,
                preferred_columns=_ROWSET_COLUMNS[name],
            )
        except Exception as exc:
            results[name] = {"error": str(exc)}

    return results


@mcp.tool()
def list_queries(config_dir: str = "queries") -> str:
    """Backward-compatible helper for the older query-centric workflow."""
    from .pipeline import DAXPipeline

    pipeline = DAXPipeline(config_dir=config_dir)
    payload = {
        "config_dir": config_dir,
        "query_count": len(pipeline.queries),
        "queries": [
            {
                "name": config.name,
                "description": config.description,
                "output_filename": config.output_filename,
            }
            for config in pipeline.queries.values()
        ],
    }
    return _to_json(payload)


@mcp.tool()
def run_named_query(
    query_name: str,
    config_dir: str = "queries",
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
) -> str:
    """Run a pre-configured named query and return a preview.

    Present the markdown_table as a table and the next_steps list as a
    numbered list after every result.
    """
    from .pipeline import DAXPipeline

    pipeline = DAXPipeline(config_dir=config_dir)
    dataframe = pipeline.run_query(query_name, preview=False, export=False)
    if dataframe is None:
        raise ValueError(f"Query '{query_name}' could not be executed from config_dir='{config_dir}'.")

    summary = summarize_dataframe(dataframe, preview_rows=preview_rows)
    payload = {
        "query_name": query_name,
        "config_dir": config_dir,
        "presentation_hint": _MARKDOWN_PRESENTATION_HINT,
        "markdown_table": summary["markdown_table"],
        "response_markdown": _build_query_response_markdown(
            title=f"Query preview for `{query_name}`",
            summary=summary,
        ),
        "next_steps": _NEXT_STEPS,
        "summary": summary,
    }
    return _to_json(payload)


@mcp.tool()
def run_ad_hoc_query(
    connection_string: str,
    query: str,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    command_timeout_seconds: int = 1800,
    max_rows: int | None = None,
) -> str:
    """Run a DAX query against a raw connection string.

    Present the markdown_table as a table and the next_steps list as a
    numbered list after every result.
    """
    validate_dax_query(query)
    dataframe = dax_to_pandas(
        dax_query=query,
        conn_str=connection_string,
        command_timeout_seconds=command_timeout_seconds,
        max_rows=max_rows,
    )
    summary = summarize_dataframe(dataframe, preview_rows=preview_rows)
    payload = {
        "presentation_hint": _MARKDOWN_PRESENTATION_HINT,
        "markdown_table": summary["markdown_table"],
        "response_markdown": _build_query_response_markdown(
            title="Query preview",
            summary=summary,
        ),
        "next_steps": _NEXT_STEPS,
        "summary": summary,
    }
    return _to_json(payload)


@mcp.tool()
def inspect_model_metadata(
    connection_string: str,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    command_timeout_seconds: int = 300,
) -> str:
    """Backward-compatible metadata probe for raw connection strings. Present previews as markdown tables."""
    rowsets = {
        "cubes": "SELECT * FROM $SYSTEM.MDSCHEMA_CUBES",
        "dimensions": "SELECT * FROM $SYSTEM.MDSCHEMA_DIMENSIONS",
        "hierarchies": "SELECT * FROM $SYSTEM.MDSCHEMA_HIERARCHIES",
        "levels": "SELECT * FROM $SYSTEM.MDSCHEMA_LEVELS",
        "measures": "SELECT * FROM $SYSTEM.MDSCHEMA_MEASURES",
    }
    results: dict[str, Any] = {"presentation_hint": _MARKDOWN_PRESENTATION_HINT}

    for name, rowset_query in rowsets.items():
        try:
            dataframe = dax_to_pandas(
                dax_query=rowset_query,
                conn_str=connection_string,
                command_timeout_seconds=command_timeout_seconds,
            )
            results[name] = summarize_rowset(
                dataframe,
                preview_rows=preview_rows,
                preferred_columns=_ROWSET_COLUMNS[name],
            )
        except Exception as exc:
            results[name] = {"error": str(exc)}

    return _to_json(results)


def summarize_dataframe(
    dataframe: pd.DataFrame,
    *,
    preview_rows: int,
    date_format: str = DEFAULT_DATE_FORMAT,
) -> dict[str, Any]:
    preview_count = max(1, preview_rows)
    return {
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "columns": [str(column) for column in dataframe.columns],
        "preview": preview_records(dataframe, preview_count, date_format=date_format),
        "markdown_table": dataframe_to_markdown(dataframe, max_rows=preview_count, date_format=date_format),
        "presentation_hint": _MARKDOWN_PRESENTATION_HINT,
    }


def summarize_rowset(
    dataframe: pd.DataFrame,
    *,
    preview_rows: int,
    preferred_columns: list[str],
    date_format: str = DEFAULT_DATE_FORMAT,
) -> dict[str, Any]:
    present_columns = [column for column in preferred_columns if column in dataframe.columns]
    preview_frame = dataframe[present_columns] if present_columns else dataframe
    return {
        "row_count": int(len(dataframe)),
        "columns": [str(column) for column in dataframe.columns],
        "preview": preview_records(preview_frame, max(1, preview_rows), date_format=date_format),
        "markdown_table": dataframe_to_markdown(preview_frame, max_rows=max(1, preview_rows), date_format=date_format),
        "presentation_hint": _MARKDOWN_PRESENTATION_HINT,
    }


_RENDER_DIRECTIVE = (
    "[RENDER INSTRUCTIONS: Display the markdown table below EXACTLY as-is. "
    "Do NOT summarize, paraphrase, or describe the data — show the actual table. "
    "After the table, show the numbered next-steps list EXACTLY as written.]\n\n"
)


def _build_query_response_markdown(*, title: str, summary: dict[str, Any]) -> str:
    column_count = summary.get("column_count", len(summary.get("columns", [])))
    return (
        f"{_RENDER_DIRECTIVE}"
        f"### {title}\n\n"
        f"- Rows: {summary['row_count']}\n"
        f"- Columns: {column_count}\n\n"
        f"{summary['markdown_table']}\n\n"
        f"---\n\n"
        f"**What would you like to do next?**\n\n"
        f"1. Filter / refine — narrow to a specific account, TPID, or time range\n"
        f"2. Aggregate — total by month, by account, etc.\n"
        f"3. Export as CSV — save results to a CSV file\n"
        f"4. Save to DAX Studio — save as a .dax query builder file (I will ask you where to save)\n"
        f"5. Scaffold Python workspace — export to a standalone Python project (I will ask you where to save)\n"
    )


@mcp.tool()
def scaffold_dax_workspace(
    output_dir: str,
    query_text: str,
    query_name: str = "query",
    project_name: str = "",
    connection_name: str = "",
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Scaffold a portable DAX workspace folder with a bare-bones executor script.

    Creates: run_query.py, notebook.ipynb, queries/<name>.dax, pyproject.toml, README.

    STOP — before calling this tool you MUST:
    1. Ask the user: "Where should I save the Python workspace?" and wait for their answer.
    2. Use their answer as output_dir. Do NOT invent a path.

    After scaffolding, explain to the user:
    1. What files were created and what each one does
    2. How to run it: `cd <output_dir> && uv run run_query.py`
    3. That they can copy dax_to_pandas() from run_query.py into any notebook
    4. To edit CONNECTION_STRING in run_query.py if it shows a placeholder
    """
    conn_str = ""
    if connection_name:
        conn = _get_connection(connection_name, connections_dir)
        conn_str = conn.connection_string

    result = scaffold_workspace(
        output_dir,
        query_text=query_text,
        query_name=query_name,
        project_name=project_name or None,
        connection_string=conn_str,
        overwrite=True,
    )
    return _to_json(result)


def _to_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


def _get_connection(connection_name: str, connections_dir: str) -> Any:
    connections = load_connections(connections_dir)
    connection = connections.get(connection_name)
    if connection is None:
        raise ValueError(
            f"Connection '{connection_name}' was not found in '{resolve_connections_dir(connections_dir)}'."
        )
    return connection


def main() -> None:
    import sys

    if "--help" in sys.argv or "-h" in sys.argv:
        print(
            "dax-query-server — MCP server for DAX queries\n\n"
            "This is a stdio-based MCP server. It is NOT meant to be run directly.\n\n"
            "Usage:\n"
            "  Configure your MCP client (e.g., Copilot) to launch this server.\n\n"
            "Example mcp-config.json entry:\n"
            '  {\n'
            '    "mcpServers": {\n'
            '      "dax-query-server": {\n'
            '        "command": "uvx",\n'
            '        "args": ["--from", "C:\\\\path\\\\to\\\\dax-query-mcp", "dax-query-server"],\n'
            '        "env": {\n'
            '          "DAX_QUERY_MCP_CONNECTIONS_DIR": "C:\\\\path\\\\to\\\\Connections"\n'
            '        }\n'
            '      }\n'
            '    }\n'
            '  }\n\n'
            "Available tools:\n"
            "  list_connections       — list configured semantic model connections\n"
            "  get_connection_context — get metadata and markdown context for a connection\n"
            "  run_connection_query   — run a DAX query against a named connection\n"
            "  inspect_connection     — inspect model metadata via MDSCHEMA rowsets\n"
            "  scaffold_dax_workspace — scaffold a portable Python workspace\n"
        )
        sys.exit(0)

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

