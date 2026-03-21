from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from mcp.server.fastmcp import FastMCP

from .connections import load_connections, resolve_connections_dir
from .data_dictionary import find_data_dictionary
from .errors import (
    admin_query_blocked,
    connection_not_found,
    execution_failed,
    invalid_params,
    query_timeout,
)
from .exceptions import DAXExecutionError
from .executor import dax_to_pandas
from .formatting import DEFAULT_DATE_FORMAT, dataframe_to_markdown, preview_records
from .query_builder import (
    load_query_builder_artifacts,
    query_builder_from_dict,
    query_builder_schema_payload,
    query_builder_to_payload,
    save_query_builder_artifacts,
)
from .data_dictionary import DataDictionary, load_data_dictionary, save_data_dictionary, TableDef, MeasureDef
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
    "Copy to clipboard — copy as TSV (paste into Excel) or markdown",
    "Export as CSV — save results to a CSV file",
    "Quick chart — generate a bar, line, or pie chart",
    "Scaffold Power Query — generate Excel Power Query M code",
    "Scaffold Streamlit — generate a Streamlit dashboard app",
    "Save to DAX Studio — save as a .dax query builder file",
    "Scaffold Python — export to a standalone Python project",
    "Re-run last query — execute the same query again",
]

_SERVER_INSTRUCTIONS = """\
You are connected to the dax-query-server, which runs DAX queries against \
Power BI / Analysis Services semantic models.

## Rules — follow these every time

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

## Tool overview — when to use each tool

| Tool | Purpose |
|---|---|
| list_connections | Discover available connections. Call first if the user hasn't specified one. |
| get_connection_context | Get curated schema (tables, columns, measures, filters) for a connection. Call BEFORE writing DAX. |
| run_connection_query | Execute a DAX query against a named connection and return structured results. Primary query tool. |
| run_connection_query_markdown | Same as run_connection_query but returns ready-to-render markdown. Use when you only need display output. |
| run_ad_hoc_query | Execute DAX against a raw connection string (no named connection required). |
| inspect_connection | Live schema discovery via MDSCHEMA rowsets. Use only when get_connection_context is unavailable or stale. |
| export_to_csv | Save query results to a timestamped CSV file. |
| copy_to_clipboard | Copy query results to the system clipboard (TSV for Excel, markdown for docs). |
| save_query_builder | Persist a structured query as .dax + .dax.queryBuilder artifacts. Always call get_query_builder_schema first. |
| get_query_builder_schema | Get the expected JSON shape for save_query_builder. |
| get_query_builder | Load a previously saved query builder definition. |
| scaffold_dax_workspace | Create a standalone Python project with run_query.py, notebook, and pyproject.toml. |

## DAX best practices

- Always start queries with EVALUATE. Every DAX query must return a table.
- Use SUMMARIZECOLUMNS or SUMMARIZE for aggregations instead of raw table scans.
- Use TOPN to limit large result sets: `EVALUATE TOPN(100, 'Table')`.
- Wrap scalar expressions in ROW: `EVALUATE ROW("Label", [Measure])`.
- Quote table names with single quotes and column names with square brackets: `'Sales'[Revenue]`.
- Prefer measures already defined in the model over writing inline CALCULATE expressions.

Good query examples:
  EVALUATE SUMMARIZECOLUMNS('Calendar'[Month], "Revenue", SUM('Sales'[Amount]))
  EVALUATE TOPN(10, 'Products', 'Products'[Sales], DESC)
  EVALUATE ROW("Total Revenue", [Total Revenue])

Bad query examples (avoid these):
  SELECT * FROM Sales           -- SQL, not DAX
  EVALUATE Sales                -- returns entire table; add TOPN or filters
  INFO.STORAGETABLECOLUMNS()    -- admin query; use get_connection_context instead
  EVALUATE FILTER(ALL('Huge'), …) -- scanning all rows; add specific filters first

## Common patterns

1. Schema-first workflow: call get_connection_context → read tables/columns/measures → compose query → run_connection_query.
2. Data dictionary: if has_context_markdown=true on a connection, get_connection_context returns curated docs. Prefer this over inspect_connection.
3. Iterative refinement: run a broad query first, then add filters/aggregations based on results.
4. Profiling: set profile=true on run_connection_query or run_ad_hoc_query to get per-phase timing. Use this to identify slow queries before optimizing.

## Error codes and recovery

| Error code | Meaning | Recovery action |
|---|---|---|
| ADMIN_QUERY_BLOCKED | Query uses forbidden admin syntax (INFO, DBCC, ALTER, etc.) | Rewrite using EVALUATE or call get_connection_context for metadata. |
| CONNECTION_NOT_FOUND | The named connection does not exist in the connections directory. | Call list_connections to see available names and retry. |
| QUERY_TIMEOUT | Query exceeded the configured timeout. | Simplify the query, add filters, reduce the result set, or increase command_timeout_seconds. |
| EXECUTION_FAILED | DAX syntax error or server-side failure. | Check table/column names with get_connection_context, fix syntax, and retry. |
| INVALID_PARAMS | Missing or invalid tool parameters. | Read the suggestion field in the error payload and correct the call. |

## Follow-up options after query results

After every successful query, offer these follow-up actions:
1. Filter / refine — narrow to specific accounts, dates, or segments.
2. Aggregate — total by month, by account, by product, etc.
3. Export as CSV — use export_to_csv to save results to a file.
4. Copy to clipboard — use copy_to_clipboard (TSV for Excel, markdown for docs).
5. Save to DAX Studio — use save_query_builder to persist as .dax artifacts.
6. Scaffold Python workspace — use scaffold_dax_workspace to create a standalone project.
7. Generate charts — suggest the user can visualize the exported data.

## Performance tips

- Always filter before aggregating to minimize data scanned.
- Use TOPN to preview large tables before pulling full results.
- Enable profiling (profile=true) to see connection, execution, and serialization timings.
- If a query times out, break it into smaller queries or add date/category filters.
- Prefer pre-defined measures over complex inline CALCULATE expressions; the engine optimizes them better.
"""

mcp = FastMCP("dax-query-server", instructions=_SERVER_INSTRUCTIONS)


# ── Follow-up menu resource ─────────────────────────────────────────────

_FOLLOWUP_MENU: list[dict[str, Any]] = [
    {
        "name": "export_to_csv",
        "description": "Export query results to a timestamped CSV file.",
        "required_params": ["connection_name", "query", "output_dir"],
        "example_usage": 'export_to_csv(connection_name="sales", query="EVALUATE ...", output_dir="./export")',
    },
    {
        "name": "copy_to_clipboard",
        "description": "Copy query results to the system clipboard as TSV (for Excel) or markdown.",
        "required_params": ["connection_name", "query"],
        "example_usage": 'copy_to_clipboard(connection_name="sales", query="EVALUATE ...", format="tsv")',
    },
    {
        "name": "quick_chart",
        "description": "Generate a chart (bar, line, or pie) from query results.",
        "required_params": ["connection_name", "query", "chart_type", "x_column", "y_column"],
        "example_usage": 'quick_chart(connection_name="sales", query="EVALUATE ...", chart_type="bar", x_column="Month", y_column="Revenue")',
    },
    {
        "name": "scaffold_power_query",
        "description": "Generate Excel Power Query M code to import DAX query results.",
        "required_params": ["connection_name", "query"],
        "example_usage": 'scaffold_power_query(connection_name="sales", query="EVALUATE ...", table_name="DAXResults")',
    },
    {
        "name": "scaffold_streamlit_app",
        "description": "Generate a Streamlit dashboard app for visualizing query results.",
        "required_params": ["connection_name", "query"],
        "example_usage": 'scaffold_streamlit_app(connection_name="sales", query="EVALUATE ...", title="Sales Dashboard")',
    },
    {
        "name": "scaffold_python",
        "description": "Generate a standalone Python project with run_query.py, notebook, and pyproject.toml. Uses save_query_builder under the hood.",
        "required_params": ["output_dir", "query_text"],
        "example_usage": 'scaffold_dax_workspace(output_dir="./my_project", query_text="EVALUATE ...", connection_name="sales")',
    },
    {
        "name": "scaffold_dax_studio",
        "description": "Save the query as .dax and .dax.queryBuilder artifacts for DAX Studio. Uses save_query_builder under the hood.",
        "required_params": ["query_builder_json", "queries_dir"],
        "example_usage": 'save_query_builder(query_builder_json="...", queries_dir="./queries")',
    },
]


@mcp.resource("followup://menu")
def followup_menu() -> str:
    """Return a structured menu of available follow-up actions after running a query.

    Each item includes name, description, required_params, and example_usage
    so an LLM can suggest appropriate next actions to the user.
    """
    return _to_json({"actions": _FOLLOWUP_MENU})


def validate_dax_query(query: str) -> None:
    """Reject queries that require admin privileges or perform DDL.

    Allows safe $SYSTEM.MDSCHEMA_* rowsets used by inspect_connection.
    Raises ToolError with a structured JSON payload so the LLM can self-correct.
    """
    upper = query.strip().upper()
    for prefix in _SAFE_SYSTEM_PREFIXES:
        if prefix.upper() in upper:
            return

    match = _ADMIN_QUERY_PATTERNS.search(query)
    if match:
        raise admin_query_blocked(blocked_pattern=match.group().strip())


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
def get_data_dictionary(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Return the data dictionary for a named connection as JSON.

    The data dictionary describes tables, columns, measures, and filters
    with human-readable descriptions and sample values.
    """
    dd = find_data_dictionary(connection_name, connections_dir)
    if dd is None:
        return _to_json({
            "connection_name": connection_name,
            "found": False,
            "message": (
                f"No data dictionary found for '{connection_name}'. "
                f"Create a file named '{connection_name}.data_dictionary.yaml' "
                f"in the connections directory to add one."
            ),
        })
    return _to_json({
        "connection_name": connection_name,
        "found": True,
        "data_dictionary": dd.model_dump(),
    })


@mcp.tool()
def get_schema(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Return schema information for a connection, enriched with data
    dictionary descriptions when available.

    If a data dictionary file exists for the connection, the response
    includes table, column, measure, and filter descriptions.  Otherwise
    a basic payload is returned suggesting ``inspect_connection`` for
    live schema discovery.
    """
    connection = _get_connection(connection_name, connections_dir)
    dd = find_data_dictionary(connection_name, connections_dir)

    payload: dict[str, Any] = {
        "connection_name": connection_name,
        "description": connection.description,
        "has_data_dictionary": dd is not None,
    }

    if dd is not None:
        payload["tables"] = [
            {
                "name": t.name,
                "description": t.description,
                "columns": [
                    {
                        "name": c.name,
                        "data_type": c.data_type,
                        "description": c.description,
                        "sample_values": c.sample_values,
                    }
                    for c in t.columns
                ],
            }
            for t in dd.tables
        ]
        payload["measures"] = [
            {
                "name": m.name,
                "expression": m.expression,
                "description": m.description,
                "format_string": m.format_string,
            }
            for m in dd.measures
        ]
        payload["filters"] = [
            {
                "name": f.name,
                "column": f.column,
                "description": f.description,
                "suggested_values": f.suggested_values,
            }
            for f in dd.filters
        ]
    else:
        payload["message"] = (
            "No data dictionary found. Use inspect_connection for live schema "
            "discovery, or create a data dictionary file."
        )

    return _to_json(payload)


@mcp.tool()
def run_connection_query(
    connection_name: str,
    query: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    max_rows: int | None = None,
    profile: bool = False,
) -> str:
    """Run a DAX query against a named connection and return a preview.

    Present the markdown_table as a table and the next_steps list as a
    numbered list after every result.
    Set profile=true to include per-phase timing information in the response.
    """
    validate_dax_query(query)
    connection = _get_connection(connection_name, connections_dir)
    try:
        dataframe = dax_to_pandas(
            dax_query=query,
            conn_str=connection.connection_string,
            connection_timeout_seconds=connection.connection_timeout_seconds,
            command_timeout_seconds=connection.command_timeout_seconds,
            max_rows=max_rows or connection.max_rows,
            profile=profile,
        )
    except DAXExecutionError as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise query_timeout(query, connection.command_timeout_seconds, exc) from exc
        raise execution_failed(query, exc) from exc

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
    if profile and "profiling" in dataframe.attrs:
        payload["profiling"] = dataframe.attrs["profiling"]
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
        raise invalid_params(
            message="queries_dir is required.",
            suggestion="Ask the user where to save before calling this tool.",
            parameter="queries_dir",
        )
    try:
        definition = query_builder_from_dict(json.loads(query_builder_json))
    except ValueError as exc:
        raise invalid_params(
            message=str(exc),
            suggestion="Call get_query_builder_schema first for a valid payload template.",
            parameter="query_builder_json",
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
    try:
        dataframe = pipeline.run_query(query_name, preview=False, export=False)
    except Exception as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise query_timeout(query_name, 0, exc) from exc
        raise execution_failed(query_name, exc) from exc
    if dataframe is None:
        raise execution_failed(
            query_name,
            ValueError(f"Query '{query_name}' could not be executed from config_dir='{config_dir}'."),
        )

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
    profile: bool = False,
) -> str:
    """Run a DAX query against a raw connection string.

    Present the markdown_table as a table and the next_steps list as a
    numbered list after every result.
    Set profile=true to include per-phase timing information in the response.
    """
    validate_dax_query(query)
    try:
        dataframe = dax_to_pandas(
            dax_query=query,
            conn_str=connection_string,
            command_timeout_seconds=command_timeout_seconds,
            max_rows=max_rows,
            profile=profile,
        )
    except DAXExecutionError as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise query_timeout(query, command_timeout_seconds, exc) from exc
        raise execution_failed(query, exc) from exc

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
    if profile and "profiling" in dataframe.attrs:
        payload["profiling"] = dataframe.attrs["profiling"]
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


@mcp.tool()
def search_columns(
    connection_name: str,
    search_term: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    max_results: int = 20,
) -> str:
    """Fuzzy-search columns across all tables for a connection.

    Searches column names (case-insensitive substring match) and also
    column descriptions when a data dictionary exists.  Returns a JSON
    array sorted by relevance: exact match > starts-with > contains.
    """
    connection = _get_connection(connection_name, connections_dir)

    # Try to load data dictionary (connection_name.data_dictionary.yaml)
    dd: DataDictionary | None = None
    dd_path = Path(resolve_connections_dir(connections_dir)) / f"{connection_name}.data_dictionary.yaml"
    if dd_path.exists():
        dd = load_data_dictionary(dd_path)

    # Build description lookup from data dictionary
    desc_lookup: dict[tuple[str, str], str] = {}
    dd_type_lookup: dict[tuple[str, str], str] = {}
    if dd is not None:
        for table in dd.tables:
            for col in table.columns:
                desc_lookup[(table.name, col.name)] = col.description
                dd_type_lookup[(table.name, col.name)] = col.data_type

    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    term_lower = search_term.lower()

    # 1. Search data dictionary columns (primary source when available)
    if dd is not None:
        for table in dd.tables:
            for col in table.columns:
                col_lower = col.name.lower()
                desc_lower = col.description.lower() if col.description else ""
                if term_lower in col_lower or term_lower in desc_lower:
                    matches.append({
                        "table": table.name,
                        "column": col.name,
                        "data_type": col.data_type,
                        "description": col.description or "",
                    })
                    seen.add((table.name, col.name))

    # 2. Supplement with live MDSCHEMA schema (columns not already found)
    try:
        dataframe = dax_to_pandas(
            dax_query=(
                "SELECT DIMENSION_UNIQUE_NAME, HIERARCHY_UNIQUE_NAME, "
                "LEVEL_NAME, DESCRIPTION "
                "FROM $SYSTEM.MDSCHEMA_LEVELS"
            ),
            conn_str=connection.connection_string,
            connection_timeout_seconds=connection.connection_timeout_seconds,
            command_timeout_seconds=connection.command_timeout_seconds,
        )
        for _, row in dataframe.iterrows():
            dim_name = str(row.get("DIMENSION_UNIQUE_NAME", ""))
            col_name = str(row.get("LEVEL_NAME", ""))
            schema_desc = str(row.get("DESCRIPTION", "") or "")
            table_name = dim_name.strip("[]")

            if (table_name, col_name) in seen:
                continue

            col_lower = col_name.lower()
            desc_lower = schema_desc.lower()
            dd_desc = desc_lookup.get((table_name, col_name), "")
            dd_desc_lower = dd_desc.lower()

            if term_lower in col_lower or term_lower in desc_lower or term_lower in dd_desc_lower:
                data_type = dd_type_lookup.get((table_name, col_name), "")
                description = dd_desc or schema_desc
                matches.append({
                    "table": table_name,
                    "column": col_name,
                    "data_type": data_type,
                    "description": description,
                })
                seen.add((table_name, col_name))
    except Exception:
        pass

    # Sort by relevance: exact > starts-with > contains
    def _relevance(m: dict[str, Any]) -> tuple[int, str]:
        col_lower = m["column"].lower()
        if col_lower == term_lower:
            return (0, col_lower)
        if col_lower.startswith(term_lower):
            return (1, col_lower)
        return (2, col_lower)

    matches.sort(key=_relevance)
    return _to_json(matches[:max_results])


@mcp.tool()
def search_measures(
    connection_name: str,
    search_term: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    max_results: int = 20,
) -> str:
    """Search measures for a connection by name, description, or expression.

    Performs a case-insensitive substring match on measure names and, when a
    data dictionary exists, measure descriptions and DAX expressions.  Returns
    a JSON array sorted by relevance: exact match > starts-with > contains.
    Expressions are truncated to 100 characters.

    Required parameters: connection_name, search_term.
    Optional parameters: connections_dir, max_results (default 20).

    Use this when the user asks "which measure calculates revenue?" or similar
    discovery questions.
    """
    _get_connection(connection_name, connections_dir)

    dd: DataDictionary | None = None
    dd_path = Path(resolve_connections_dir(connections_dir)) / f"{connection_name}.data_dictionary.yaml"
    if dd_path.exists():
        dd = load_data_dictionary(dd_path)

    matches: list[dict[str, Any]] = []
    term_lower = search_term.lower()

    if dd is not None:
        for measure in dd.measures:
            name_lower = measure.name.lower()
            expr_lower = measure.expression.lower() if measure.expression else ""
            desc_lower = measure.description.lower() if measure.description else ""
            if term_lower in name_lower or term_lower in desc_lower or term_lower in expr_lower:
                expression = measure.expression or ""
                matches.append({
                    "name": measure.name,
                    "expression": expression[:100] + ("..." if len(expression) > 100 else ""),
                    "description": measure.description or "",
                })

    def _relevance(m: dict[str, Any]) -> tuple[int, str]:
        name_lower = m["name"].lower()
        if name_lower == term_lower:
            return (0, name_lower)
        if name_lower.startswith(term_lower):
            return (1, name_lower)
        return (2, name_lower)

    matches.sort(key=_relevance)
    return _to_json(matches[:max_results])


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
def copy_to_clipboard(
    connection_name: str,
    query: str,
    format: str = "tsv",
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    max_rows: int | None = None,
) -> str:
    """Run a DAX query and copy the full result to the system clipboard.

    Use format="tsv" (default) to paste into Excel, or format="markdown" for
    a markdown table. Returns a JSON summary with row_count and a short preview.
    """
    import pyperclip

    if format not in ("tsv", "markdown"):
        raise invalid_params(
            message=f"Unsupported format '{format}'.",
            suggestion="Use format='tsv' (for Excel paste) or format='markdown' (for markdown table).",
            parameter="format",
            provided=format,
            allowed=["tsv", "markdown"],
        )

    validate_dax_query(query)
    connection = _get_connection(connection_name, connections_dir)
    try:
        dataframe = dax_to_pandas(
            dax_query=query,
            conn_str=connection.connection_string,
            connection_timeout_seconds=connection.connection_timeout_seconds,
            command_timeout_seconds=connection.command_timeout_seconds,
            max_rows=max_rows or connection.max_rows,
        )
    except DAXExecutionError as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise query_timeout(query, connection.command_timeout_seconds, exc) from exc
        raise execution_failed(query, exc) from exc

    if format == "tsv":
        clipboard_text = dataframe.to_csv(sep="\t", index=False)
    else:
        clipboard_text = dataframe_to_markdown(dataframe, max_rows=len(dataframe))

    pyperclip.copy(clipboard_text)

    preview_rows = min(5, len(dataframe))
    preview = preview_records(dataframe, preview_rows)
    payload = {
        "format": format,
        "row_count": len(dataframe),
        "preview": preview,
        "message": f"Copied {len(dataframe)} rows as {format.upper()} to clipboard.",
    }
    return _to_json(payload)


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


@mcp.tool()
def scaffold_streamlit_app(
    connection_name: str,
    query: str,
    title: str = "DAX Query Results",
    output_path: str = "",
) -> str:
    """Generate a Streamlit Python app for visualizing DAX query results.

    Creates a standalone .py file that uses Streamlit to display a data table
    and an optional bar chart for numeric columns.

    Parameters:
        connection_name: Name of the DAX connection (embedded in the generated app).
        query: The DAX query to embed in the generated app.
        title: Page title shown in the Streamlit app.
        output_path: If provided, write the generated code to this file path.
    """
    import textwrap

    code = textwrap.dedent(f'''\
        """Streamlit app for visualizing DAX query results.

        Run with: streamlit run {output_path or "app.py"}
        """

        import streamlit as st
        import pandas as pd

        st.set_page_config(page_title={title!r}, layout="wide")
        st.title({title!r})

        CONNECTION_NAME = {connection_name!r}
        DAX_QUERY = {query!r}

        st.subheader("DAX Query")
        st.code(DAX_QUERY, language="dax")

        # -- Replace this section with live query execution if desired --
        # from dax_query_mcp.executor import dax_to_pandas
        # df = dax_to_pandas(dax_query=DAX_QUERY, conn_str="YOUR_CONNECTION_STRING")
        st.info(
            f"Connection: **{{CONNECTION_NAME}}** — "
            "replace the sample DataFrame below with a live call to dax_to_pandas()."
        )
        df = pd.DataFrame({{"Column": ["sample"], "Value": [0]}})
        # -- End sample section --

        st.subheader("Data")
        st.dataframe(df, use_container_width=True)

        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if numeric_cols:
            st.subheader("Chart")
            st.bar_chart(df[numeric_cols])
    ''')

    payload: dict[str, Any] = {
        "code": code,
        "instructions": (
            f"Run the app with: streamlit run "
            f"{output_path or 'app.py'}"
        ),
    }

    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(code, encoding="utf-8")
        payload["file_path"] = str(out)

    return _to_json(payload)


@mcp.tool()
def export_to_csv(
    connection_name: str,
    query: str,
    output_dir: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    filename_prefix: str = "export",
    max_rows: int | None = None,
) -> str:
    """Export DAX query results to a timestamped CSV file.

    Returns JSON with file_path, row_count, and column_count.
    """
    validate_dax_query(query)
    connection = _get_connection(connection_name, connections_dir)
    try:
        dataframe = dax_to_pandas(
            dax_query=query,
            conn_str=connection.connection_string,
            connection_timeout_seconds=connection.connection_timeout_seconds,
            command_timeout_seconds=connection.command_timeout_seconds,
            max_rows=max_rows or connection.max_rows,
        )
    except DAXExecutionError as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise query_timeout(query, connection.command_timeout_seconds, exc) from exc
        raise execution_failed(query, exc) from exc

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"
    file_path = out_path / filename

    dataframe.to_csv(file_path, index=False)

    payload = {
        "file_path": str(file_path),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
    }
    return _to_json(payload)


@mcp.tool()
def scaffold_power_query(
    connection_name: str,
    query: str,
    table_name: str = "DAXResults",
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Generate Power Query M code for importing DAX query results into Excel.

    Returns JSON with the generated M code, table name, and paste instructions.
    """
    validate_dax_query(query)
    connection = _get_connection(connection_name, connections_dir)
    conn_str = connection.connection_string.strip()

    escaped_query = query.replace('"', '""')

    m_code = (
        "let\n"
        "    // Step 1 — Connect to Analysis Services\n"
        f'    Source = AnalysisServices.Database("{conn_str}"),\n'
        "\n"
        "    // Step 2 — Run the DAX query\n"
        f'    Result = Value.NativeQuery(Source, "{escaped_query}")\n'
        "in\n"
        "    // Step 3 — Return the result table\n"
        "    Result"
    )

    payload = {
        "m_code": m_code,
        "table_name": table_name,
        "instructions": (
            "1. Open Excel and go to the Data tab.\n"
            "2. Click 'Get Data' > 'From Other Sources' > 'Blank Query'.\n"
            "3. In the Power Query Editor, click 'Advanced Editor'.\n"
            "4. Replace the contents with the M code above.\n"
            "5. Click 'Done', then 'Close & Load'.\n"
            f"6. Rename the resulting table to '{table_name}'."
        ),
    }
    return _to_json(payload)


@mcp.tool()
def quick_chart(
    connection_name: str,
    query: str,
    chart_type: str,
    x_column: str,
    y_column: str,
    output_path: str = "",
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    max_rows: int | None = None,
) -> str:
    """Generate a chart (bar, line, or pie) from DAX query results.

    Returns JSON with file_path, chart_type, and row_count.
    If output_path is not provided, the chart is saved to a temp file.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    valid_types = ("bar", "line", "pie")
    if chart_type not in valid_types:
        raise invalid_params(f"chart_type must be one of {valid_types}, got '{chart_type}'")

    validate_dax_query(query)
    connection = _get_connection(connection_name, connections_dir)
    try:
        dataframe = dax_to_pandas(
            dax_query=query,
            conn_str=connection.connection_string,
            connection_timeout_seconds=connection.connection_timeout_seconds,
            command_timeout_seconds=connection.command_timeout_seconds,
            max_rows=max_rows or connection.max_rows,
        )
    except DAXExecutionError as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise query_timeout(query, connection.command_timeout_seconds, exc) from exc
        raise execution_failed(query, exc) from exc

    for col in (x_column, y_column):
        if col not in dataframe.columns:
            raise invalid_params(
                f"Column '{col}' not found in query results. "
                f"Available columns: {list(dataframe.columns)}"
            )

    fig, ax = plt.subplots()
    if chart_type == "bar":
        ax.bar(dataframe[x_column].astype(str), dataframe[y_column])
    elif chart_type == "line":
        ax.plot(dataframe[x_column], dataframe[y_column])
    elif chart_type == "pie":
        ax.pie(dataframe[y_column], labels=dataframe[x_column].astype(str), autopct="%1.1f%%")
    ax.set_title(f"{y_column} by {x_column}")

    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        file_path = str(out)
    else:
        file_path = tempfile.mktemp(suffix=".png", prefix="quick_chart_")

    plt.savefig(file_path)
    plt.close(fig)

    payload = {
        "file_path": file_path,
        "chart_type": chart_type,
        "row_count": int(len(dataframe)),
    }
    return _to_json(payload)


@mcp.tool()
def generate_data_dictionary(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    output_path: str = "",
) -> str:
    """Generate a data dictionary YAML from live schema inspection.

    Queries MDSCHEMA_MEASUREGROUPS, MDSCHEMA_MEASURES, and
    MDSCHEMA_DIMENSIONS to discover tables and measures, then builds a
    DataDictionary scaffold with empty descriptions for the user to fill in.

    Required parameters: connection_name.
    Optional parameters: connections_dir, output_path — when provided the
    YAML is written to disk.

    Returns JSON with yaml_content, table_count, measure_count, and
    file_path (when output_path is given).
    """
    connection = _get_connection(connection_name, connections_dir)
    conn_str = connection.connection_string

    schema_queries = {
        "measuregroups": "SELECT * FROM $SYSTEM.MDSCHEMA_MEASUREGROUPS",
        "measures": "SELECT * FROM $SYSTEM.MDSCHEMA_MEASURES",
        "dimensions": "SELECT * FROM $SYSTEM.MDSCHEMA_DIMENSIONS",
    }

    raw: dict[str, pd.DataFrame] = {}
    for key, query in schema_queries.items():
        try:
            raw[key] = dax_to_pandas(
                dax_query=query,
                conn_str=conn_str,
                connection_timeout_seconds=connection.connection_timeout_seconds,
                command_timeout_seconds=connection.command_timeout_seconds,
            )
        except DAXExecutionError as exc:
            raise execution_failed(query, exc) from exc

    # Build tables from MDSCHEMA_DIMENSIONS
    tables: list[TableDef] = []
    if "dimensions" in raw:
        dim_df = raw["dimensions"]
        dim_name_col = "DIMENSION_NAME"
        if dim_name_col in dim_df.columns:
            for dim_name in dim_df[dim_name_col].unique():
                tables.append(TableDef(name=str(dim_name), description=""))

    # Build measures from MDSCHEMA_MEASURES
    measures: list[MeasureDef] = []
    if "measures" in raw:
        meas_df = raw["measures"]
        name_col = "MEASURE_NAME"
        unique_col = "MEASURE_UNIQUE_NAME"
        if name_col in meas_df.columns:
            for _, row in meas_df.iterrows():
                expression = str(row.get(unique_col, "")) if unique_col in meas_df.columns else ""
                measures.append(
                    MeasureDef(name=str(row[name_col]), expression=expression, description="")
                )

    dd = DataDictionary(version="1.0", tables=tables, measures=measures, filters=[])

    yaml_content = yaml.dump(
        dd.model_dump(exclude_defaults=False),
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )

    payload: dict[str, Any] = {
        "yaml_content": yaml_content,
        "table_count": len(tables),
        "measure_count": len(measures),
    }

    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        save_data_dictionary(dd, out)
        payload["file_path"] = str(out)

    return _to_json(payload)


def _to_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


def _get_connection(connection_name: str, connections_dir: str) -> Any:
    connections = load_connections(connections_dir)
    connection = connections.get(connection_name)
    if connection is None:
        raise connection_not_found(
            connection_name=connection_name,
            connections_dir=str(resolve_connections_dir(connections_dir)),
            available=list(connections.keys()),
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

