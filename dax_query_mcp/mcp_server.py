from __future__ import annotations

import json
import os
import re
import tempfile
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from fastmcp import FastMCP

from .connections import load_connections, resolve_connections_dir
from .data_dictionary import (
    ColumnDef,
    DataDictionary,
    MeasureDef,
    RelationshipDef,
    TableDef,
    find_data_dictionary,
    load_data_dictionary,
    save_data_dictionary,
)
from .errors import (
    admin_query_blocked,
    connection_not_found,
    execution_failed,
    invalid_params,
    query_timeout,
)
from .exceptions import DAXExecutionError
from .executor import dax_to_pandas
from .followups import catalog_actions, recommend_actions, rendered_next_steps
from .formatting import DEFAULT_DATE_FORMAT, dataframe_to_markdown, preview_records
from .models import TRANSPORT_POWERBI_REST
from .query_builder import (
    load_query_builder_artifacts,
    query_builder_from_dict,
    query_builder_schema_payload,
    query_builder_to_payload,
    save_query_builder_artifacts,
)
from .scaffold import (
    build_scaffold_connection_config,
    render_run_queries_script,
    scaffold_workspace,
)

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
    | \bDBCC\b                  # DBCC commands
    | \bALTER\b                 # DDL: ALTER
    | \bCREATE\b                # DDL: CREATE
    | \bDELETE\b                # DDL: DELETE
    | \bDROP\b                  # DDL: DROP
    """,
    re.IGNORECASE | re.VERBOSE,
)
_SYSTEM_ROWSET_PATTERN = re.compile(r"\$SYSTEM\.[A-Z0-9_]+", re.IGNORECASE)

_SAFE_SYSTEM_PREFIXES = (
    "$SYSTEM.MDSCHEMA_",
)

_NEXT_STEPS = rendered_next_steps()

_SERVER_INSTRUCTIONS = """\
You are connected to the dax-query-server, which runs DAX queries against \
Power BI / Analysis Services semantic models.

## Rules

1. ALWAYS EXECUTE queries — build the DAX AND call run_connection_query in \
the same turn. Never just show query text without running it.
2. Query tools return a COMPLETE, pre-formatted markdown response. \
Output the ENTIRE string verbatim as your response — including the data \
table AND the numbered "What would you like to do next?" list. \
Do NOT summarize, convert the table to bullet points, or omit the menu.
3. NEVER generate admin queries (INFO.*(), $SYSTEM.DISCOVER_*, DBCC, \
ALTER, CREATE, DELETE, DROP). Use get_connection_context for metadata.

## Tool overview

| Tool | Purpose |
|---|---|
| list_connections | Discover available connections. |
| get_connection_context | Schema overview — call BEFORE writing DAX. |
| run_connection_query | Execute DAX against a named connection. |
| run_ad_hoc_query | Execute DAX against a raw connection string. |
| search_connection_context | Search context docs for tables, columns, filters. |
| search_columns | Fuzzy search columns by name or description. |
| search_measures | Fuzzy search measures by name or expression. |
| export_to_csv | Save results to a timestamped CSV file. |
| copy_to_clipboard | Copy results as TSV (Excel) or markdown. |
| save_to_workstation | Save a query for iterative exploration. |

## DAX best practices

- Every query must start with EVALUATE and return a table.
- Use SUMMARIZECOLUMNS (or SUMMARIZE) for grouped aggregations.
- Use TREATAS for cross-table filtering instead of CALCULATETABLE.
- Use TOPN to limit large result sets: `EVALUATE TOPN(100, 'Table')`.
- Quote table names with single quotes, columns with brackets: `'Sales'[Revenue]`.

## Error codes and recovery

| Code | Recovery |
|---|---|
| ADMIN_QUERY_BLOCKED | Rewrite with EVALUATE or use get_connection_context. |
| CONNECTION_NOT_FOUND | Call list_connections and retry. |
| QUERY_TIMEOUT | Simplify query, add filters, or increase timeout. |
| EXECUTION_FAILED | Check names via get_connection_context, fix syntax. |
| INVALID_PARAMS | Read the suggestion field and correct the call. |

## Follow-up options after query results

The query tool output already includes the numbered "What would you like \
to do next?" list. Do NOT generate your own version — the list is baked \
into the tool response and must appear exactly as returned.
"""

mcp = FastMCP("dax-query-server", instructions=_SERVER_INSTRUCTIONS)


# ── Follow-up menu resource ─────────────────────────────────────────────

_FOLLOWUP_MENU: list[dict[str, Any]] = catalog_actions()
_last_query_context: dict[str, Any] | None = None


@mcp.resource("followup://menu")
def followup_menu() -> str:
    """Return a structured menu of available follow-up actions after running a query.

    Each item includes name, description, required_params, and example_usage
    so an LLM can suggest appropriate next actions to the user.
    """
    return _to_json({"actions": catalog_actions()})


@mcp.resource("followup://recommendations")
def followup_recommendations() -> str:
    """Return server-ranked follow-up actions for the latest query result."""
    return _to_json(_followup_recommendation_payload())


@mcp.prompt
def explore_connection(connection_name: str = "your_connection") -> str:
    """Guide an agent through progressive discovery for a new DAX connection."""
    return (
        f"Explore the `{connection_name}` semantic model progressively. "
        "First call get_connection_context(detail='overview'), then use "
        "get_context_bundle(detail='overview') for structured counts. Only fetch "
        "get_table_detail, get_measure_detail, get_relationships, or "
        "get_filter_suggestions when the question needs that scoped context. "
        "After writing DAX, run it with run_connection_query in the same turn and "
        "return the complete tool response verbatim."
    )


@mcp.prompt
def find_measure_for_metric(connection_name: str = "your_connection", metric: str = "revenue") -> str:
    """Guide measure discovery before writing a metric-focused DAX query."""
    return (
        f"Find the best DAX measure for `{metric}` in `{connection_name}`. "
        "Call search_measures with the metric term, then get_measure_detail for "
        "the most likely measure. If ambiguity remains, inspect related tables "
        "with get_context_bundle(detail='schema') or get_table_detail before "
        "running a DAX query."
    )


@mcp.prompt
def write_filtered_dax_query(
    connection_name: str = "your_connection",
    business_question: str = "show the metric by month",
    filters: str = "",
) -> str:
    """Guide filtered DAX query authoring with context and execution steps."""
    return (
        f"Answer this question against `{connection_name}`: {business_question}. "
        f"Requested filters: {filters or 'none specified'}. Start with "
        "get_connection_context(detail='overview'), then use search_columns, "
        "search_measures, and get_filter_suggestions to resolve exact names. "
        "Write a safe EVALUATE query, execute it with run_connection_query in "
        "the same turn, and output the complete returned markdown verbatim."
    )


@mcp.prompt
def build_period_comparison_query(
    connection_name: str = "your_connection",
    metric: str = "Total Sales",
    period: str = "month",
) -> str:
    """Guide a period comparison or time-intelligence DAX workflow."""
    return (
        f"Build a `{period}` comparison for `{metric}` on `{connection_name}`. "
        "Use search_measures to confirm the metric, search_columns for calendar "
        "fields, and get_relationships to verify the fact table filters through "
        "the calendar table. Then execute the DAX with run_connection_query and "
        "preserve the returned follow-up menu."
    )


@mcp.prompt
def export_query_results(connection_name: str = "your_connection", query: str = "EVALUATE ...") -> str:
    """Guide an end-to-end query-to-artifact follow-up workflow."""
    return (
        f"Run this query on `{connection_name}` and turn it into a reusable "
        f"artifact: {query}. First execute run_connection_query and output the "
        "complete response. Then use the server-authored follow-up options: "
        "save_to_workstation for iterative work, quick_chart when numeric columns "
        "are present, export_to_csv for a file, or scaffold_power_query/"
        "scaffold_streamlit_app/scaffold_dax_workspace when the user wants a "
        "refreshable asset."
    )


def _followup_recommendation_payload() -> dict[str, Any]:
    """Return the latest query context plus ranked follow-up actions."""
    if _last_query_context is None:
        return {
            "has_query_context": False,
            "message": "No query has been run in this server session yet.",
            "recommended_actions": [],
        }

    return {
        "has_query_context": True,
        "latest_query": _last_query_context,
        "recommended_actions": recommend_actions(_last_query_context),
    }


def _capture_last_query_context(
    *,
    connection_name: str | None,
    query: str,
    summary: dict[str, Any],
    dataframe: pd.DataFrame,
    profile: dict[str, Any] | None = None,
) -> None:
    """Capture result metadata used by server-authored follow-up recommendations."""
    global _last_query_context
    _last_query_context = {
        "connection_name": connection_name,
        "query": query,
        "row_count": summary["row_count"],
        "column_count": summary.get("column_count", len(summary.get("columns", []))),
        "columns": list(summary.get("columns", [])),
        "numeric_columns": _numeric_columns(dataframe),
        "profile": profile or {},
        "workstation_count": len(_workstation),
    }


def _numeric_columns(dataframe: pd.DataFrame) -> list[str]:
    return [
        str(column)
        for column in dataframe.columns
        if pd.api.types.is_numeric_dtype(dataframe[column])
    ]


def validate_dax_query(query: str) -> None:
    """Reject queries that require admin privileges or perform DDL.

    Allows safe $SYSTEM.MDSCHEMA_* rowsets used by inspect_connection.
    Raises ToolError with a structured JSON payload so the LLM can self-correct.
    """
    upper = query.strip().upper()
    for rowset_ref in _SYSTEM_ROWSET_PATTERN.findall(upper):
        if not any(rowset_ref.startswith(prefix) for prefix in _SAFE_SYSTEM_PREFIXES):
            raise admin_query_blocked(blocked_pattern=rowset_ref)

    match = _ADMIN_QUERY_PATTERNS.search(query)
    if match:
        raise admin_query_blocked(blocked_pattern=match.group().strip())


@mcp.tool(annotations={"readOnlyHint": True})
def list_connections(
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    output_format: str = "markdown",
) -> str:
    """List configured connections as a markdown table by default.

    Call get_connection_context on a connection with overview/full context before
    writing DAX queries. Set output_format="json" for machine-readable output.
    """
    payload = _list_connections_payload(connections_dir)
    if output_format.lower() == "json":
        return _to_json(payload)
    if output_format.lower() != "markdown":
        raise invalid_params(
            message=f"Unsupported output_format '{output_format}'.",
            suggestion='Use output_format="markdown" or output_format="json".',
            parameter="output_format",
        )
    return _connections_to_markdown(payload)


def _connections_to_markdown(payload: dict[str, Any]) -> str:
    connections = payload["connections"]
    connection_count = payload["connection_count"]
    connections_dir = payload["connections_dir"]
    if connection_count == 0:
        return (
            f"No DAX connections found in `{connections_dir}`.\n\n"
            "Add a connection YAML file, then call `list_connections` again."
        )

    rows = [
        "| Connection | Description | Type | Transport | Overview | Full context |",
        "|---|---|---|---|---|---|",
    ]
    for connection in connections:
        rows.append(
            "| "
            + " | ".join(
                [
                    f"`{_markdown_escape_cell(connection['name'])}`",
                    _markdown_escape_cell(connection.get("description") or ""),
                    _markdown_escape_cell(connection.get("connection_type") or ""),
                    _markdown_escape_cell(connection.get("transport") or ""),
                    "Yes" if connection.get("has_overview") else "No",
                    "Yes" if connection.get("has_full_context") else "No",
                ]
            )
            + " |"
        )

    heading = f"Found {connection_count} DAX connection"
    if connection_count != 1:
        heading += "s"
    heading += f" in `{connections_dir}`."
    return heading + "\n\n" + "\n".join(rows)


def _markdown_escape_cell(value: object) -> str:
    text = str(value)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _list_connections_payload(connections_dir: str = DEFAULT_CONNECTIONS_DIR) -> dict[str, Any]:
    connections = load_connections(connections_dir)
    return {
        "connections_dir": str(resolve_connections_dir(connections_dir)),
        "connection_count": len(connections),
        "connections": [
            {
                "name": connection.name,
                "description": connection.description,
                "transport": connection.transport,
                "connection_type": _connection_type(connection),
                "has_dataset_id": connection.dataset_id is not None,
                "suggested_skill": connection.suggested_skill,
                "suggested_skill_reason": connection.suggested_skill_reason,
                "has_overview": connection.overview_markdown is not None,
                "has_full_context": connection.context_markdown is not None,
            }
            for connection in connections.values()
        ],
    }


@mcp.tool(annotations={"readOnlyHint": True})
def get_connection_context(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    detail: str = "overview",
) -> str:
    """Return metadata and context for a named connection.

    This is the PRIMARY way to discover tables, columns, measures, and filters.
    Always call this FIRST before writing any DAX query.

    detail levels:
    - "overview" (default): Returns the compact overview with key tables, measures,
      and a few example queries. Fast and concise — use this first.
    - "full": Returns the complete context markdown. Only use if overview is
      insufficient and you need deep detail on filters, column values, etc.
    """
    connection = _get_connection(connection_name, connections_dir)

    if detail == "full":
        context = connection.context_markdown
    else:
        context = connection.overview_markdown or connection.context_markdown

    payload = {
        "connection_name": connection.name,
        "description": connection.description,
        "transport": connection.transport,
        "connection_type": _connection_type(connection),
        "has_dataset_id": connection.dataset_id is not None,
        "suggested_skill": connection.suggested_skill,
        "suggested_skill_reason": connection.suggested_skill_reason,
        "detail_level": detail,
        "has_full_context": connection.context_markdown is not None,
        "has_overview": connection.overview_markdown is not None,
        "context_markdown": context,
        "NEXT_ACTION": (
            "You now have the schema. Compose a DAX query AND immediately "
            "execute it using run_connection_query in the SAME turn. "
            "Do NOT just display the query text to the user — run it so "
            "they see the actual data table."
        ),
    }
    if detail == "overview" and connection.context_markdown is not None:
        payload["NOTE"] = (
            "This is the compact overview. If you need more detail on "
            "specific tables, columns, or filter values, call "
            "get_connection_context with detail='full' or use "
            "search_connection_context to search for specific terms."
        )
    return _to_json(payload)


@mcp.tool(annotations={"readOnlyHint": True})
def search_connection_context(
    connection_name: str,
    search_term: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    max_lines: int = 50,
) -> str:
    """Search the full connection context markdown for specific terms.

    Use this instead of loading the full context when you need to find
    specific information about tables, columns, filters, or query patterns.
    Returns matching lines with surrounding context.
    """
    connection = _get_connection(connection_name, connections_dir)
    if not connection.context_markdown:
        return _to_json({
            "connection_name": connection_name,
            "search_term": search_term,
            "match_count": 0,
            "matches": [],
            "message": "No context markdown found for this connection.",
        })

    lines = connection.context_markdown.splitlines()
    search_lower = search_term.lower()
    matches: list[dict[str, Any]] = []

    for i, line in enumerate(lines):
        if search_lower in line.lower():
            start = max(0, i - 2)
            end = min(len(lines), i + 3)
            context_lines = lines[start:end]
            matches.append({
                "line_number": i + 1,
                "match_line": line.strip(),
                "context": "\n".join(context_lines),
            })
            if len(matches) >= max_lines:
                break

    return _to_json({
        "connection_name": connection_name,
        "search_term": search_term,
        "match_count": len(matches),
        "matches": matches,
    })


@mcp.tool(annotations={"readOnlyHint": True})
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


@mcp.tool(annotations={"readOnlyHint": True})
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
        payload["relationships"] = [
            _relationship_payload(relationship)
            for relationship in dd.relationships
        ]
    else:
        if connection.transport == TRANSPORT_POWERBI_REST:
            payload["message"] = (
                "No data dictionary found. Power BI REST connections cannot use "
                "MDSCHEMA/DMV live schema discovery through executeQueries, so create "
                "a data dictionary file or add markdown context for this connection."
            )
        else:
            payload["message"] = (
                "No data dictionary found. Use inspect_connection for live schema "
                "discovery, or create a data dictionary file."
            )

    return _to_json(payload)


@mcp.tool(annotations={"readOnlyHint": True})
def get_context_bundle(
    connection_name: str,
    detail: str = "overview",
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    table_names: str = "",
) -> str:
    """Return a progressive, structured context bundle for a connection.

    detail="overview" returns compact counts and key names; "schema" returns
    structured tables, measures, filters, and relationships; "full" also
    includes available markdown context. Use table_names as a comma-separated
    allowlist to scope table-heavy schema output.
    """
    return _to_json(
        _context_bundle_payload(
            connection_name=connection_name,
            detail=detail,
            connections_dir=connections_dir,
            table_names=_split_csv(table_names),
        )
    )


@mcp.resource("context://{connection_name}/schema")
def connection_schema_resource(connection_name: str) -> str:
    """Return schema-level structured context for a connection."""
    return _to_json(
        _context_bundle_payload(
            connection_name=connection_name,
            detail="schema",
            connections_dir=DEFAULT_CONNECTIONS_DIR,
            table_names=[],
        )
    )


@mcp.resource("context://{connection_name}/relationships")
def connection_relationships_resource(connection_name: str) -> str:
    """Return relationship topology from the connection data dictionary."""
    dd = find_data_dictionary(connection_name, DEFAULT_CONNECTIONS_DIR)
    relationships = [_relationship_payload(item) for item in dd.relationships] if dd is not None else []
    return _to_json({
        "connection_name": connection_name,
        "found": dd is not None,
        "relationship_count": len(relationships),
        "relationships": relationships,
    })


@mcp.resource("context://{connection_name}/data-dictionary")
def connection_data_dictionary_resource(connection_name: str) -> str:
    """Return the structured data dictionary resource for a connection."""
    return get_data_dictionary(connection_name, DEFAULT_CONNECTIONS_DIR)


@mcp.tool(annotations={"readOnlyHint": True})
def get_table_detail(
    connection_name: str,
    table_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Return detailed structured context for a single table."""
    dd = find_data_dictionary(connection_name, connections_dir)
    if dd is None:
        return _to_json({
            "connection_name": connection_name,
            "found": False,
            "message": "No data dictionary found for this connection.",
        })

    table = _find_table(dd, table_name)
    if table is None:
        raise invalid_params(
            message=f"Table '{table_name}' not found in data dictionary.",
            suggestion="Call get_schema or search_columns to discover available tables.",
            parameter="table_name",
        )

    relationships = [
        _relationship_payload(item)
        for item in dd.relationships
        if item.from_table == table.name or item.to_table == table.name
    ]
    return _to_json({
        "connection_name": connection_name,
        "found": True,
        "table": table.model_dump(),
        "relationships": relationships,
    })


@mcp.tool(annotations={"readOnlyHint": True})
def get_measure_detail(
    connection_name: str,
    measure_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Return detailed structured context for a single measure."""
    dd = find_data_dictionary(connection_name, connections_dir)
    if dd is None:
        return _to_json({
            "connection_name": connection_name,
            "found": False,
            "message": "No data dictionary found for this connection.",
        })

    measure = _find_measure(dd, measure_name)
    if measure is None:
        raise invalid_params(
            message=f"Measure '{measure_name}' not found in data dictionary.",
            suggestion="Call search_measures to discover available measures.",
            parameter="measure_name",
        )
    return _to_json({
        "connection_name": connection_name,
        "found": True,
        "measure": measure.model_dump(),
    })


@mcp.tool(annotations={"readOnlyHint": True})
def get_relationships(
    connection_name: str,
    table_name: str = "",
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Return relationship topology, optionally filtered to one table."""
    dd = find_data_dictionary(connection_name, connections_dir)
    if dd is None:
        return _to_json({
            "connection_name": connection_name,
            "found": False,
            "relationship_count": 0,
            "relationships": [],
            "message": "No data dictionary found for this connection.",
        })

    relationships = dd.relationships
    if table_name.strip():
        table = _find_table(dd, table_name)
        if table is None:
            raise invalid_params(
                message=f"Table '{table_name}' not found in data dictionary.",
                suggestion="Call get_schema to discover available tables.",
                parameter="table_name",
            )
        relationships = [
            item for item in relationships
            if item.from_table == table.name or item.to_table == table.name
        ]

    return _to_json({
        "connection_name": connection_name,
        "found": True,
        "relationship_count": len(relationships),
        "relationships": [_relationship_payload(item) for item in relationships],
    })


@mcp.tool(annotations={"readOnlyHint": True})
def get_filter_suggestions(
    connection_name: str,
    filter_name: str = "",
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Return suggested filters and values from the data dictionary."""
    dd = find_data_dictionary(connection_name, connections_dir)
    if dd is None:
        return _to_json({
            "connection_name": connection_name,
            "found": False,
            "filters": [],
            "message": "No data dictionary found for this connection.",
        })

    filters = dd.filters
    if filter_name.strip():
        term = filter_name.lower()
        filters = [
            item for item in filters
            if item.name.lower() == term or item.column.lower() == term
        ]

    return _to_json({
        "connection_name": connection_name,
        "found": True,
        "filter_count": len(filters),
        "filters": [item.model_dump() for item in filters],
    })


@mcp.tool(annotations={"readOnlyHint": True})
def check_context_staleness(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    command_timeout_seconds: int | None = None,
) -> str:
    """Compare live model metadata against the data dictionary for drift."""
    connection = _get_connection(connection_name, connections_dir)
    dd = find_data_dictionary(connection_name, connections_dir)
    payload: dict[str, Any] = {
        "connection_name": connection_name,
        "has_data_dictionary": dd is not None,
        "live_metadata_supported": connection.transport != TRANSPORT_POWERBI_REST,
    }
    if dd is None:
        payload["status"] = "missing_data_dictionary"
        payload["message"] = "Create or generate a data dictionary before checking staleness."
        return _to_json(payload)
    if connection.transport == TRANSPORT_POWERBI_REST:
        payload["status"] = "not_checked"
        payload["message"] = (
            "Power BI REST executeQueries cannot inspect live MDSCHEMA metadata. "
            "Use an MSOLAP connection to the same model for live staleness checks."
        )
        payload["dictionary"] = _dictionary_metadata(dd)
        return _to_json(payload)

    live = _live_mdschema_metadata(connection, command_timeout_seconds=command_timeout_seconds)
    dictionary = _dictionary_metadata(dd)
    comparisons = {
        key: _compare_name_sets(dictionary[key], live[key])
        for key in ("tables", "columns", "measures")
    }

    relationship_probe = _load_tmschema_relationships(connection)
    if relationship_probe["supported"]:
        live_relationships = {
            _relationship_key(RelationshipDef.model_validate(item))
            for item in relationship_probe["relationships"]
        }
        comparisons["relationships"] = _compare_name_sets(dictionary["relationships"], live_relationships)
    else:
        comparisons["relationships"] = {
            "checked": False,
            "reason": relationship_probe["message"],
            "missing_in_dictionary": [],
            "missing_in_live": [],
        }

    stale = any(
        comparison.get("missing_in_dictionary") or comparison.get("missing_in_live")
        for comparison in comparisons.values()
        if comparison.get("checked", True)
    )
    payload.update({
        "status": "stale" if stale else "current",
        "dictionary": dictionary,
        "live": live,
        "comparisons": comparisons,
    })
    return _to_json(payload)


@mcp.tool(annotations={"readOnlyHint": True})
def check_ai_readiness(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Assess whether context is descriptive enough for reliable DAX generation."""
    dd = find_data_dictionary(connection_name, connections_dir)
    if dd is None:
        return _to_json({
            "connection_name": connection_name,
            "status": "not_ready",
            "score": 0,
            "issues": [{"category": "data_dictionary", "message": "No data dictionary found."}],
        })

    issues: list[dict[str, str]] = []
    duplicate_columns = _duplicate_column_names(dd)
    for name, tables in duplicate_columns.items():
        issues.append({
            "category": "ambiguous_column",
            "message": f"Column '{name}' appears in multiple tables: {', '.join(tables)}.",
        })
    for table in dd.tables:
        if not table.description.strip():
            issues.append({"category": "table_description", "message": f"Table '{table.name}' has no description."})
        for column in table.columns:
            if not column.description.strip():
                issues.append({
                    "category": "column_description",
                    "message": f"Column '{table.name}[{column.name}]' has no description.",
                })
    for measure in dd.measures:
        if not measure.expression.strip():
            issues.append({"category": "measure_expression", "message": f"Measure '{measure.name}' has no expression."})
        if not measure.description.strip():
            issues.append({"category": "measure_description", "message": f"Measure '{measure.name}' has no description."})
    for filter_def in dd.filters:
        if not filter_def.suggested_values:
            issues.append({
                "category": "filter_values",
                "message": f"Filter '{filter_def.name}' has no suggested values.",
            })
    if not dd.relationships:
        issues.append({
            "category": "relationships",
            "message": "No relationships are documented; joins/filter propagation may be ambiguous.",
        })
    else:
        for relationship in dd.relationships:
            if not relationship.description.strip():
                issues.append({
                    "category": "relationship_description",
                    "message": (
                        f"Relationship {relationship.from_table}[{relationship.from_column}] -> "
                        f"{relationship.to_table}[{relationship.to_column}] has no description."
                    ),
                })

    score = max(0, 100 - (len(issues) * 5))
    if score >= 90:
        status = "ready"
    elif score >= 70:
        status = "usable_with_warnings"
    else:
        status = "needs_context"
    return _to_json({
        "connection_name": connection_name,
        "status": status,
        "score": score,
        "issue_count": len(issues),
        "issues": issues,
    })


@mcp.tool(annotations={"readOnlyHint": True})
def probe_tmschema_capabilities(
    connection_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Probe optional TMSCHEMA metadata access without requiring it."""
    connection = _get_connection(connection_name, connections_dir)
    if connection.transport == TRANSPORT_POWERBI_REST:
        return _to_json({
            "connection_name": connection_name,
            "supported": False,
            "message": "Power BI REST executeQueries does not expose TMSCHEMA rowsets.",
        })

    probe = _load_tmschema_relationships(connection)
    return _to_json({
        "connection_name": connection_name,
        "supported": probe["supported"],
        "message": probe["message"],
        "relationship_count": len(probe["relationships"]),
        "relationships": probe["relationships"],
    })


@mcp.tool(annotations={"readOnlyHint": True})
def run_connection_query(
    connection_name: str,
    query: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    max_rows: int | None = None,
    profile: bool = False,
) -> str:
    """Run a DAX query against a named connection.

    IMPORTANT: This tool returns a COMPLETE, pre-formatted response for the
    user. Output the returned string as your ENTIRE response — do NOT
    summarize, truncate, or convert the table to bullet points. The output
    includes a data table and a numbered follow-up menu that must both appear.
    """
    validate_dax_query(query)
    connection = _get_connection(connection_name, connections_dir)
    try:
        dataframe = _execute_connection_dataframe(
            connection,
            query,
            max_rows=max_rows,
            profile=profile,
        )
    except DAXExecutionError as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise query_timeout(query, connection.command_timeout_seconds, exc) from exc
        raise execution_failed(query, exc) from exc

    summary = summarize_dataframe(dataframe, preview_rows=preview_rows)
    _capture_last_query_context(
        connection_name=connection_name,
        query=query,
        summary=summary,
        dataframe=dataframe,
        profile=dataframe.attrs.get("profiling") if profile else None,
    )
    md = _build_query_response_markdown(
        title=f"Query preview for `{connection_name}`",
        summary=summary,
    )
    if profile and "profiling" in dataframe.attrs:
        md += _format_profiling_markdown(dataframe.attrs["profiling"])
    return md


@mcp.tool(annotations={"readOnlyHint": True})
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


@mcp.tool(annotations={"readOnlyHint": True})
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


@mcp.tool(annotations={"readOnlyHint": True})
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
    if connection.transport == TRANSPORT_POWERBI_REST:
        return {
            "connection_name": connection_name,
            "transport": connection.transport,
            "live_metadata_supported": False,
            "has_context_markdown": connection.context_markdown is not None,
            "has_overview": connection.overview_markdown is not None,
            "presentation_hint": _MARKDOWN_PRESENTATION_HINT,
            "message": (
                "Power BI REST executeQueries supports DAX query execution but not "
                "MDSCHEMA/DMV live metadata inspection. Use the connection context "
                "or data dictionary files, or use an MSOLAP connection for live inspection."
            ),
        }
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
            dataframe = _execute_connection_dataframe(
                connection,
                rowset_query,
                max_rows=None,
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


@mcp.tool(annotations={"readOnlyHint": True})
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


@mcp.tool(annotations={"readOnlyHint": True})
def run_named_query(
    query_name: str,
    config_dir: str = "queries",
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
) -> str:
    """Run a pre-configured named query and return a preview.

    IMPORTANT: This tool returns a COMPLETE, pre-formatted response for the
    user. Output the returned string as your ENTIRE response — do NOT
    summarize, truncate, or convert the table to bullet points. The output
    includes a data table and a numbered follow-up menu that must both appear.
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
    query_config = pipeline.queries.get(query_name)
    _capture_last_query_context(
        connection_name=None,
        query=query_config.dax_query if query_config is not None else query_name,
        summary=summary,
        dataframe=dataframe,
    )
    return _build_query_response_markdown(
        title=f"Query preview for `{query_name}`",
        summary=summary,
    )


@mcp.tool(annotations={"readOnlyHint": True})
def run_ad_hoc_query(
    connection_string: str,
    query: str,
    preview_rows: int = DEFAULT_PREVIEW_ROWS,
    command_timeout_seconds: int = 1800,
    max_rows: int | None = None,
    profile: bool = False,
) -> str:
    """Run a DAX query against a raw connection string.

    IMPORTANT: This tool returns a COMPLETE, pre-formatted response for the
    user. Output the returned string as your ENTIRE response — do NOT
    summarize, truncate, or convert the table to bullet points. The output
    includes a data table and a numbered follow-up menu that must both appear.
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
    _capture_last_query_context(
        connection_name=None,
        query=query,
        summary=summary,
        dataframe=dataframe,
        profile=dataframe.attrs.get("profiling") if profile else None,
    )
    md = _build_query_response_markdown(
        title="Query preview",
        summary=summary,
    )
    if profile and "profiling" in dataframe.attrs:
        md += _format_profiling_markdown(dataframe.attrs["profiling"])
    return md


@mcp.tool(annotations={"readOnlyHint": True})
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


@mcp.tool(annotations={"readOnlyHint": True})
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
        if connection.transport != TRANSPORT_POWERBI_REST:
            dataframe = _execute_connection_dataframe(
                connection,
                (
                    "SELECT DIMENSION_UNIQUE_NAME, HIERARCHY_UNIQUE_NAME, "
                    "LEVEL_NAME, DESCRIPTION "
                    "FROM $SYSTEM.MDSCHEMA_LEVELS"
                ),
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
    except DAXExecutionError:
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


@mcp.tool(annotations={"readOnlyHint": True})
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
    connection = _get_connection(connection_name, connections_dir)

    dd: DataDictionary | None = None
    dd_path = Path(resolve_connections_dir(connections_dir)) / f"{connection_name}.data_dictionary.yaml"
    if dd_path.exists():
        dd = load_data_dictionary(dd_path)

    matches: list[dict[str, Any]] = []
    seen: set[str] = set()
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
                    "source": "data_dictionary",
                })
                seen.add(measure.name)

    try:
        if connection.transport != TRANSPORT_POWERBI_REST:
            dataframe = _execute_connection_dataframe(
                connection,
                (
                    "SELECT MEASURE_NAME, MEASURE_UNIQUE_NAME, DESCRIPTION "
                    "FROM $SYSTEM.MDSCHEMA_MEASURES"
                ),
            )
            for _, row in dataframe.iterrows():
                measure_name = str(row.get("MEASURE_NAME", "") or "")
                if not measure_name or measure_name in seen:
                    continue

                expression = str(row.get("MEASURE_UNIQUE_NAME", "") or "")
                description = str(row.get("DESCRIPTION", "") or "")
                name_lower = measure_name.lower()
                expr_lower = expression.lower()
                desc_lower = description.lower()
                if term_lower in name_lower or term_lower in desc_lower or term_lower in expr_lower:
                    matches.append({
                        "name": measure_name,
                        "expression": expression[:100] + ("..." if len(expression) > 100 else ""),
                        "description": description,
                        "source": "live_mdschema",
                    })
                    seen.add(measure_name)
    except DAXExecutionError:
        pass

    def _relevance(m: dict[str, Any]) -> tuple[int, str]:
        name_lower = m["name"].lower()
        if name_lower == term_lower:
            return (0, name_lower)
        if name_lower.startswith(term_lower):
            return (1, name_lower)
        return (2, name_lower)

    matches.sort(key=_relevance)
    return _to_json(matches[:max_results])


def _split_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _relationship_payload(relationship: RelationshipDef) -> dict[str, Any]:
    payload = relationship.model_dump()
    payload["from"] = f"{relationship.from_table}[{relationship.from_column}]"
    payload["to"] = f"{relationship.to_table}[{relationship.to_column}]"
    return payload


def _relationship_key(relationship: RelationshipDef) -> str:
    return (
        f"{relationship.from_table}[{relationship.from_column}]"
        f"->{relationship.to_table}[{relationship.to_column}]"
    )


def _context_bundle_payload(
    *,
    connection_name: str,
    detail: str,
    connections_dir: str,
    table_names: list[str],
) -> dict[str, Any]:
    normalized_detail = detail.lower()
    if normalized_detail not in {"overview", "schema", "full"}:
        raise invalid_params(
            message=f"Unsupported detail '{detail}'.",
            suggestion='Use detail="overview", "schema", or "full".',
            parameter="detail",
        )

    connection = _get_connection(connection_name, connections_dir)
    dd = find_data_dictionary(connection_name, connections_dir)
    payload: dict[str, Any] = {
        "connection_name": connection_name,
        "description": connection.description,
        "context_level": normalized_detail,
        "has_data_dictionary": dd is not None,
        "sources": {
            "overview_markdown": connection.overview_path,
            "full_context_markdown": connection.context_path,
            "data_dictionary": str(Path(resolve_connections_dir(connections_dir)) / f"{connection_name}.data_dictionary.yaml")
            if dd is not None
            else None,
        },
        "next_levels": _next_context_levels(normalized_detail),
    }
    if dd is None:
        payload["message"] = "No data dictionary found; use get_connection_context or generate_data_dictionary next."
        return payload

    table_filter = {name.lower() for name in table_names}
    tables = [table for table in dd.tables if not table_filter or table.name.lower() in table_filter]
    payload["counts"] = {
        "tables": len(dd.tables),
        "measures": len(dd.measures),
        "filters": len(dd.filters),
        "relationships": len(dd.relationships),
    }

    if normalized_detail == "overview":
        payload["tables"] = [
            {"name": table.name, "description": table.description, "column_count": len(table.columns)}
            for table in tables
        ]
        payload["measures"] = [
            {"name": measure.name, "description": measure.description}
            for measure in dd.measures
        ]
        payload["filters"] = [
            {"name": filter_def.name, "column": filter_def.column, "description": filter_def.description}
            for filter_def in dd.filters
        ]
        payload["relationships"] = [_relationship_payload(item) for item in dd.relationships]
        return payload

    payload["tables"] = [table.model_dump() for table in tables]
    payload["measures"] = [measure.model_dump() for measure in dd.measures]
    payload["filters"] = [filter_def.model_dump() for filter_def in dd.filters]
    payload["relationships"] = [_relationship_payload(item) for item in dd.relationships]
    if normalized_detail == "full":
        payload["overview_markdown"] = connection.overview_markdown or ""
        payload["context_markdown"] = connection.context_markdown or ""
    return payload


def _next_context_levels(detail: str) -> list[str]:
    if detail == "overview":
        return ["schema", "full"]
    if detail == "schema":
        return ["full"]
    return []


def _find_table(dd: DataDictionary, table_name: str) -> TableDef | None:
    term = table_name.lower()
    return next((table for table in dd.tables if table.name.lower() == term), None)


def _find_measure(dd: DataDictionary, measure_name: str) -> MeasureDef | None:
    term = measure_name.lower()
    return next((measure for measure in dd.measures if measure.name.lower() == term), None)


def _dictionary_metadata(dd: DataDictionary) -> dict[str, list[str]]:
    return {
        "tables": sorted(table.name for table in dd.tables),
        "columns": sorted(
            f"{table.name}[{column.name}]"
            for table in dd.tables
            for column in table.columns
        ),
        "measures": sorted(measure.name for measure in dd.measures),
        "relationships": sorted(_relationship_key(item) for item in dd.relationships),
    }


def _live_mdschema_metadata(
    connection: Any,
    *,
    command_timeout_seconds: int | None,
) -> dict[str, list[str]]:
    queries = {
        "dimensions": "SELECT * FROM $SYSTEM.MDSCHEMA_DIMENSIONS",
        "levels": "SELECT * FROM $SYSTEM.MDSCHEMA_LEVELS",
        "measures": "SELECT * FROM $SYSTEM.MDSCHEMA_MEASURES",
    }
    raw: dict[str, pd.DataFrame] = {}
    for key, query in queries.items():
        try:
            raw[key] = _execute_connection_dataframe(
                connection,
                query,
                command_timeout_seconds=command_timeout_seconds,
            )
        except DAXExecutionError as exc:
            raise execution_failed(query, exc) from exc
    return {
        "tables": sorted(_table_names_from_dimensions(raw["dimensions"])),
        "columns": sorted(
            f"{table_name}[{column.name}]"
            for table_name, column in _columns_from_levels(raw["levels"])
        ),
        "measures": sorted(
            str(row.get("MEASURE_NAME", "") or "")
            for _, row in raw["measures"].iterrows()
            if str(row.get("MEASURE_NAME", "") or "").strip()
        ),
    }


def _table_names_from_dimensions(dataframe: pd.DataFrame) -> set[str]:
    if "DIMENSION_NAME" not in dataframe.columns:
        return set()
    return {
        str(value)
        for value in dataframe["DIMENSION_NAME"].dropna().unique()
        if str(value).strip()
    }


def _columns_from_levels(dataframe: pd.DataFrame) -> list[tuple[str, ColumnDef]]:
    columns: list[tuple[str, ColumnDef]] = []
    for _, row in dataframe.iterrows():
        column_name = str(row.get("LEVEL_NAME", "") or "").strip()
        if not column_name or column_name.lower() in {"(all)", "all"}:
            continue
        table_name = _table_name_from_level_row(row)
        if not table_name:
            continue
        data_type = _mdschema_level_data_type(row)
        columns.append((
            table_name,
            ColumnDef(
                name=column_name,
                data_type=data_type,
                description=str(row.get("DESCRIPTION", "") or ""),
            ),
        ))
    return columns


def _table_name_from_level_row(row: Any) -> str:
    value = str(row.get("DIMENSION_UNIQUE_NAME", "") or "").strip()
    if value:
        return _clean_mdx_name(value)
    hierarchy = str(row.get("HIERARCHY_UNIQUE_NAME", "") or "").strip()
    if hierarchy:
        first_part = hierarchy.split(".", maxsplit=1)[0]
        return _clean_mdx_name(first_part)
    return ""


def _clean_mdx_name(value: str) -> str:
    text = value.strip()
    if text.startswith("[") and "]" in text:
        text = text[1:text.index("]")]
    return text.strip("[]")


def _mdschema_level_data_type(row: Any) -> str:
    for key in ("DATA_TYPE", "LEVEL_DBTYPE", "LEVEL_TYPE"):
        value = row.get(key, None)
        if value is not None and str(value).strip() and str(value) != "nan":
            return str(value)
    return "string"


def _compare_name_sets(dictionary_values: Any, live_values: Any) -> dict[str, Any]:
    dictionary_set = {str(value) for value in dictionary_values}
    live_set = {str(value) for value in live_values}
    return {
        "checked": True,
        "dictionary_count": len(dictionary_set),
        "live_count": len(live_set),
        "missing_in_dictionary": sorted(live_set - dictionary_set),
        "missing_in_live": sorted(dictionary_set - live_set),
    }


def _duplicate_column_names(dd: DataDictionary) -> dict[str, list[str]]:
    locations: dict[str, list[str]] = {}
    display_names: dict[str, str] = {}
    for table in dd.tables:
        for column in table.columns:
            key = column.name.lower()
            display_names.setdefault(key, column.name)
            locations.setdefault(key, []).append(table.name)
    return {
        display_names[key]: sorted(set(tables))
        for key, tables in locations.items()
        if len(set(tables)) > 1
    }


def _load_tmschema_relationships(connection: Any) -> dict[str, Any]:
    try:
        relationships_df = _execute_connection_dataframe(
            connection,
            "SELECT * FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS",
        )
    except DAXExecutionError as exc:
        return {
            "supported": False,
            "message": f"TMSCHEMA relationship metadata unavailable: {exc}",
            "relationships": [],
        }

    try:
        tables_df = _execute_connection_dataframe(connection, "SELECT * FROM $SYSTEM.TMSCHEMA_TABLES")
        columns_df = _execute_connection_dataframe(connection, "SELECT * FROM $SYSTEM.TMSCHEMA_COLUMNS")
    except DAXExecutionError:
        tables_df = pd.DataFrame()
        columns_df = pd.DataFrame()

    relationships = _relationships_from_tmschema(relationships_df, tables_df, columns_df)
    message = (
        "TMSCHEMA relationship metadata is available."
        if relationships
        else "TMSCHEMA_RELATIONSHIPS returned no usable relationship rows."
    )
    return {
        "supported": True,
        "message": message,
        "relationships": [relationship.model_dump() for relationship in relationships],
    }


def _relationships_from_tmschema(
    relationships_df: pd.DataFrame,
    tables_df: pd.DataFrame,
    columns_df: pd.DataFrame,
) -> list[RelationshipDef]:
    table_names = _tmschema_table_name_map(tables_df)
    column_lookup = _tmschema_column_lookup(columns_df, table_names)
    relationships: list[RelationshipDef] = []
    for _, row in relationships_df.iterrows():
        from_column_info = column_lookup.get(str(_first_existing(row, ("FromColumnID", "FROM_COLUMN_ID"))))
        to_column_info = column_lookup.get(str(_first_existing(row, ("ToColumnID", "TO_COLUMN_ID"))))
        from_table = str(_first_existing(row, ("FromTable", "FromTableName", "FROM_TABLE", "FROM_TABLE_NAME")) or "")
        from_column = str(_first_existing(row, ("FromColumn", "FromColumnName", "FROM_COLUMN", "FROM_COLUMN_NAME")) or "")
        to_table = str(_first_existing(row, ("ToTable", "ToTableName", "TO_TABLE", "TO_TABLE_NAME")) or "")
        to_column = str(_first_existing(row, ("ToColumn", "ToColumnName", "TO_COLUMN", "TO_COLUMN_NAME")) or "")

        if from_column_info is not None:
            from_table = from_table or from_column_info["table"]
            from_column = from_column or from_column_info["column"]
        if to_column_info is not None:
            to_table = to_table or to_column_info["table"]
            to_column = to_column or to_column_info["column"]

        from_table_id = _first_existing(row, ("FromTableID", "FROM_TABLE_ID"))
        to_table_id = _first_existing(row, ("ToTableID", "TO_TABLE_ID"))
        from_table = from_table or table_names.get(str(from_table_id), "")
        to_table = to_table or table_names.get(str(to_table_id), "")

        if not all((from_table, from_column, to_table, to_column)):
            continue
        relationships.append(
            RelationshipDef(
                from_table=from_table,
                from_column=from_column,
                to_table=to_table,
                to_column=to_column,
                cardinality=_cardinality_from_tmschema(row),
                cross_filter_direction=_cross_filter_from_tmschema(row),
                is_active=_coerce_bool(_first_existing(row, ("IsActive", "IS_ACTIVE")), default=True),
                description=str(_first_existing(row, ("Description", "DESCRIPTION")) or ""),
                source="tmschema",
                confidence="high",
            )
        )
    return relationships


def _tmschema_table_name_map(dataframe: pd.DataFrame) -> dict[str, str]:
    if dataframe.empty or "ID" not in dataframe.columns:
        return {}
    return {
        str(row.get("ID")): str(_first_existing(row, ("Name", "ExplicitName", "InferredName")) or "")
        for _, row in dataframe.iterrows()
    }


def _tmschema_column_lookup(dataframe: pd.DataFrame, table_names: dict[str, str]) -> dict[str, dict[str, str]]:
    if dataframe.empty or "ID" not in dataframe.columns:
        return {}
    lookup: dict[str, dict[str, str]] = {}
    for _, row in dataframe.iterrows():
        column_id = str(row.get("ID"))
        table_id = str(_first_existing(row, ("TableID", "TABLE_ID")) or "")
        column_name = str(_first_existing(row, ("ExplicitName", "InferredName", "Name")) or "")
        if column_id and column_name:
            lookup[column_id] = {"table": table_names.get(table_id, ""), "column": column_name}
    return lookup


def _first_existing(row: Any, names: tuple[str, ...]) -> Any:
    for name in names:
        if name in row:
            value = row.get(name)
            if value is not None and not pd.isna(value) and str(value).strip():
                return value
    return None


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "active"}:
        return True
    if text in {"false", "0", "no", "inactive"}:
        return False
    return default


def _cardinality_from_tmschema(row: Any) -> str:
    from_cardinality = str(_first_existing(row, ("FromCardinality", "FROM_CARDINALITY")) or "").lower()
    to_cardinality = str(_first_existing(row, ("ToCardinality", "TO_CARDINALITY")) or "").lower()
    if "many" in from_cardinality and "one" in to_cardinality:
        return "many-to-one"
    if "one" in from_cardinality and "many" in to_cardinality:
        return "one-to-many"
    if "one" in from_cardinality and "one" in to_cardinality:
        return "one-to-one"
    if "many" in from_cardinality and "many" in to_cardinality:
        return "many-to-many"
    return "many-to-one"


def _cross_filter_from_tmschema(row: Any) -> str:
    value = str(_first_existing(row, ("CrossFilteringBehavior", "CROSS_FILTERING_BEHAVIOR")) or "").lower()
    return "both" if "both" in value else "single"


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



def _build_query_response_markdown(*, title: str, summary: dict[str, Any]) -> str:
    column_count = summary.get("column_count", len(summary.get("columns", [])))
    next_steps_md = "\n".join(f"{i+1}. {step}" for i, step in enumerate(_NEXT_STEPS))
    return (
        f"### {title}\n\n"
        f"- Rows: {summary['row_count']}\n"
        f"- Columns: {column_count}\n\n"
        f"{summary['markdown_table']}\n\n"
        f"---\n\n"
        f"**What would you like to do next?**\n\n"
        f"{next_steps_md}\n"
    )


def _format_profiling_markdown(profiling: dict[str, Any]) -> str:
    """Append profiling details as a markdown section."""
    lines = ["\n---\n", "**Profiling**\n"]
    for key, value in profiling.items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


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
        dataframe = _execute_connection_dataframe(
            connection,
            query,
            max_rows=max_rows,
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
    3. That they can copy execute_dax() / dax_to_pandas() from run_query.py into any notebook
    4. To edit CONNECTION in run_query.py if it shows placeholder connection values
    """
    connection_kwargs: dict[str, Any] = {}
    if connection_name:
        conn = _get_connection(connection_name, connections_dir)
        connection_kwargs = {
            "connection_string": conn.connection_string,
            "transport": conn.transport,
            "dataset_id": conn.dataset_id,
            "auth_mode": conn.auth_mode,
            "access_token_env": conn.access_token_env,
            "api_base_url": conn.api_base_url,
            "impersonated_user_name": conn.impersonated_user_name,
            "connection_timeout_seconds": conn.connection_timeout_seconds,
            "command_timeout_seconds": conn.command_timeout_seconds,
            "max_rows": conn.max_rows,
        }

    result = scaffold_workspace(
        output_dir,
        query_text=query_text,
        query_name=query_name,
        project_name=project_name or None,
        **connection_kwargs,
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
        dataframe = _execute_connection_dataframe(
            connection,
            query,
            max_rows=max_rows,
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
    if connection.transport == TRANSPORT_POWERBI_REST:
        raise invalid_params(
            message="scaffold_power_query currently supports MSOLAP connections only.",
            suggestion=(
                "Use an MSOLAP connection for Excel Power Query scaffolding, or run the REST-backed query "
                "with export_to_csv/copy_to_clipboard and load that output into Excel."
            ),
            connection_name=connection_name,
            transport=connection.transport,
        )
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
        dataframe = _execute_connection_dataframe(
            connection,
            query,
            max_rows=max_rows,
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

    Queries MDSCHEMA_MEASUREGROUPS, MDSCHEMA_MEASURES, MDSCHEMA_DIMENSIONS,
    and MDSCHEMA_LEVELS to discover tables, columns, and measures, then builds
    a DataDictionary scaffold. If optional TMSCHEMA rowsets are available, it
    also includes high-confidence relationship metadata.

    Required parameters: connection_name.
    Optional parameters: connections_dir, output_path — when provided the
    YAML is written to disk.

    Returns JSON with yaml_content, table_count, measure_count, and
    file_path (when output_path is given).
    """
    connection = _get_connection(connection_name, connections_dir)
    if connection.transport == TRANSPORT_POWERBI_REST:
        dd = find_data_dictionary(connection_name, connections_dir)
        payload: dict[str, Any] = {
            "connection_name": connection_name,
            "transport": connection.transport,
            "generated": False,
            "table_count": len(dd.tables) if dd is not None else 0,
            "measure_count": len(dd.measures) if dd is not None else 0,
            "message": (
                "Power BI REST executeQueries does not expose MDSCHEMA/DMV live metadata. "
                "Use an existing data dictionary file, fill one manually from model docs, "
                "or run generate_data_dictionary against an MSOLAP connection to the same model."
            ),
        }
        if dd is not None:
            payload["data_dictionary"] = dd.model_dump()
        return _to_json(payload)

    schema_queries = {
        "measuregroups": "SELECT * FROM $SYSTEM.MDSCHEMA_MEASUREGROUPS",
        "measures": "SELECT * FROM $SYSTEM.MDSCHEMA_MEASURES",
        "dimensions": "SELECT * FROM $SYSTEM.MDSCHEMA_DIMENSIONS",
        "levels": "SELECT * FROM $SYSTEM.MDSCHEMA_LEVELS",
    }

    raw: dict[str, pd.DataFrame] = {}
    for key, query in schema_queries.items():
        try:
            raw[key] = _execute_connection_dataframe(connection, query)
        except DAXExecutionError as exc:
            raise execution_failed(query, exc) from exc

    # Build tables from MDSCHEMA_DIMENSIONS and enrich columns from MDSCHEMA_LEVELS.
    table_map: dict[str, TableDef] = {}
    if "dimensions" in raw:
        dim_df = raw["dimensions"]
        dim_name_col = "DIMENSION_NAME"
        if dim_name_col in dim_df.columns:
            for _, row in dim_df.iterrows():
                dim_name = str(row[dim_name_col])
                description = str(row.get("DESCRIPTION", "") or "")
                table_map.setdefault(dim_name, TableDef(name=dim_name, description=description))

    if "levels" in raw:
        seen_columns: set[tuple[str, str]] = set()
        for table_name, column in _columns_from_levels(raw["levels"]):
            key = (table_name, column.name)
            if key in seen_columns:
                continue
            seen_columns.add(key)
            table = table_map.setdefault(table_name, TableDef(name=table_name, description=""))
            table.columns.append(column)
    tables = list(table_map.values())

    # Build measures from MDSCHEMA_MEASURES
    measures: list[MeasureDef] = []
    if "measures" in raw:
        meas_df = raw["measures"]
        name_col = "MEASURE_NAME"
        unique_col = "MEASURE_UNIQUE_NAME"
        if name_col in meas_df.columns:
            for _, row in meas_df.iterrows():
                expression = str(row.get(unique_col, "")) if unique_col in meas_df.columns else ""
                description = str(row.get("DESCRIPTION", "") or "")
                measures.append(
                    MeasureDef(name=str(row[name_col]), expression=expression, description=description)
                )

    relationship_probe = _load_tmschema_relationships(connection)
    relationships = [
        RelationshipDef.model_validate(item)
        for item in relationship_probe["relationships"]
    ] if relationship_probe["supported"] else []

    dd = DataDictionary(
        version="1.0",
        tables=tables,
        measures=measures,
        filters=[],
        relationships=relationships,
    )

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
        "relationship_count": len(relationships),
        "relationship_source": "tmschema" if relationships else "unavailable",
        "relationship_message": relationship_probe["message"],
    }

    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        save_data_dictionary(dd, out)
        payload["file_path"] = str(out)

    return _to_json(payload)


# ── Workstation helpers ──────────────────────────────────────────────

# ── In-memory workstation (ephemeral per server session) ─────────────
_workstation: dict[str, dict[str, Any]] = {}


def _slugify(text: str) -> str:
    """Convert a description to a filesystem-safe slug."""
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[\s_-]+", "_", slug).strip("_")
    return slug or "query"


@mcp.tool()
def save_to_workstation(
    connection_name: str,
    query: str,
    description: str,
    query_name: str = "",
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Save a DAX query to the session workstation for iterative exploration.

    The workstation is ephemeral — it resets when the server restarts
    (i.e. each new chat session starts fresh). Use export_workstation to
    persist queries permanently as a scaffold project or .dax files.

    Parameters:
        connection_name: Name of the connection this query targets.
        query: The DAX query text.
        description: Human-readable description of what the query does.
        query_name: Optional slug name; auto-generated from description if blank.
        connections_dir: Connections directory (unused for storage, kept for API compat).
    """
    if not query_name.strip():
        query_name = _slugify(description)

    _workstation[query_name] = {
        "query_name": query_name,
        "connection_name": connection_name,
        "query": query,
        "description": description,
        "saved_at": datetime.now().isoformat(),
    }

    return _to_json({
        "message": f"Query '{query_name}' saved to workstation (session-only).",
        "query_name": query_name,
    })


@mcp.tool(annotations={"readOnlyHint": True})
def list_workstation(
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """List all queries saved in the current session workstation.

    Returns a JSON array of saved queries with their names, descriptions,
    connection names, and timestamps.  Shows a helpful message if the
    workstation is empty.
    """
    if not _workstation:
        return _to_json({
            "message": "Workstation is empty. Use save_to_workstation to add queries.",
            "count": 0,
            "queries": [],
        })

    queries = [
        {
            "query_name": e["query_name"],
            "description": e.get("description", ""),
            "connection_name": e.get("connection_name", ""),
            "saved_at": e.get("saved_at", ""),
        }
        for e in _workstation.values()
    ]

    return _to_json({
        "message": f"{len(queries)} query(ies) in workstation.",
        "count": len(queries),
        "queries": queries,
    })


@mcp.tool(annotations={"destructiveHint": True})
def remove_from_workstation(
    query_name: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Remove a saved query from the workstation by name.

    Parameters:
        query_name: The slug name of the query to remove.
        connections_dir: Unused (kept for API compat).
    """
    if query_name not in _workstation:
        raise invalid_params(
            message=f"Query '{query_name}' not found in workstation.",
            suggestion="Call list_workstation to see available queries.",
            parameter="query_name",
        )

    del _workstation[query_name]
    return _to_json({
        "message": f"Query '{query_name}' removed from workstation.",
        "query_name": query_name,
    })


@mcp.tool(annotations={"destructiveHint": True})
def clear_workstation(
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
) -> str:
    """Clear all queries from the workstation.

    Removes every saved query from the in-memory workstation and
    returns the count of removed items.
    """
    count = len(_workstation)
    _workstation.clear()
    return _to_json({
        "message": f"Cleared {count} query(ies) from workstation.",
        "removed_count": count,
    })


_MULTI_PYPROJECT_TEMPLATE = textwrap.dedent("""\
    [project]
    name = "{project_name}"
    version = "0.1.0"
    description = "DAX workstation — exported queries"
    requires-python = ">=3.12"
    dependencies = [
        "ipykernel>=6.29.0",
        "pandas>=2.3.0",
        "pywin32>=310",
        "rich>=13.0.0",
    ]
""")


@mcp.tool()
def export_workstation(
    output_dir: str,
    connections_dir: str = DEFAULT_CONNECTIONS_DIR,
    format: str = "scaffold",
) -> str:
    """Export all workstation queries as a scaffold workspace or .dax files.

    This is the "make it permanent" step — writes the in-memory workstation
    to disk as a portable project.

    Parameters:
        output_dir: Directory to write the exported files.
        connections_dir: Unused (kept for API compat).
        format: "scaffold" creates a full project (run_queries.py, pyproject.toml,
                README, and queries/ dir).  "dax" writes only .dax files.
    """
    if not _workstation:
        return _to_json({
            "message": "Workstation is empty — nothing to export.",
            "files_created": [],
        })

    entries = list(_workstation.values())
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    queries_dir = out / "queries"
    queries_dir.mkdir(exist_ok=True)
    created: list[str] = []

    # Write .dax files
    for entry in entries:
        dax_path = queries_dir / f"{entry['query_name']}.dax"
        dax_path.write_text(entry["query"], encoding="utf-8")
        created.append(str(dax_path))

    if format == "dax":
        return _to_json({
            "message": f"Exported {len(entries)} .dax file(s).",
            "files_created": created,
            "output_dir": str(out),
        })

    # ── scaffold format ──────────────────────────────────────────────
    connection_names = sorted({e["connection_name"] for e in entries})
    available_connections = load_connections(connections_dir)
    connections_config: dict[str, dict[str, Any]] = {}
    for connection_name in connection_names:
        connection = available_connections.get(connection_name)
        if connection is None:
            connections_config[connection_name] = build_scaffold_connection_config()
            continue

        connections_config[connection_name] = build_scaffold_connection_config(
            connection_string=connection.connection_string,
            transport=connection.transport,
            dataset_id=connection.dataset_id,
            auth_mode=connection.auth_mode,
            access_token_env=connection.access_token_env,
            api_base_url=connection.api_base_url,
            impersonated_user_name=connection.impersonated_user_name,
            connection_timeout_seconds=connection.connection_timeout_seconds,
            command_timeout_seconds=connection.command_timeout_seconds,
            max_rows=connection.max_rows,
        )

    queries_payload = [
        {
            "name": e["query_name"],
            "file": f"queries/{e['query_name']}.dax",
            "connection": e["connection_name"],
            "description": e["description"],
        }
        for e in entries
    ]

    safe_project = out.name.replace(" ", "-").lower()

    run_script = out / "run_queries.py"
    run_script.write_text(
        render_run_queries_script(
            connections_config=connections_config,
            queries=queries_payload,
        ),
        encoding="utf-8",
    )
    created.append(str(run_script))

    pyproject = out / "pyproject.toml"
    pyproject.write_text(
        _MULTI_PYPROJECT_TEMPLATE.format(project_name=safe_project),
        encoding="utf-8",
    )
    created.append(str(pyproject))

    # README listing all queries
    readme_lines = [
        f"# {safe_project}\n",
        "\nDAX workstation exported by **dax-query-mcp**.\n",
        "\n## Quick start\n",
        "\n```bash\ncd " + str(out) + " && uv run run_queries.py\n```\n",
        "\n## Queries\n",
        "\n| Name | Connection | Description |",
        "\n|------|------------|-------------|",
    ]
    for e in entries:
        readme_lines.append(f"\n| {e['query_name']} | {e['connection_name']} | {e['description']} |")
    readme_lines.append("\n")

    readme = out / "README.md"
    readme.write_text("".join(readme_lines), encoding="utf-8")
    created.append(str(readme))

    return _to_json({
        "message": f"Exported {len(entries)} query(ies) as scaffold workspace.",
        "files_created": created,
        "output_dir": str(out),
        "project_name": safe_project,
    })


def _to_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


def _connection_type(connection: Any) -> str:
    if str(getattr(connection, "connection_string", "")).strip().upper().startswith("MOCK://"):
        return "mock"
    return str(connection.transport)


def _execute_connection_dataframe(
    connection: Any,
    query: str,
    *,
    max_rows: int | None = None,
    command_timeout_seconds: int | None = None,
    profile: bool = False,
) -> pd.DataFrame:
    return dax_to_pandas(
        dax_query=query,
        conn_str=connection.connection_string,
        transport=connection.transport,
        dataset_id=connection.dataset_id,
        auth_mode=connection.auth_mode,
        access_token_env=connection.access_token_env,
        api_base_url=connection.api_base_url,
        impersonated_user_name=connection.impersonated_user_name,
        connection_timeout_seconds=connection.connection_timeout_seconds,
        command_timeout_seconds=(
            connection.command_timeout_seconds
            if command_timeout_seconds is None
            else command_timeout_seconds
        ),
        max_rows=max_rows if max_rows is not None else connection.max_rows,
        profile=profile,
    )


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

