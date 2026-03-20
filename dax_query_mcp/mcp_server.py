from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from mcp.server.fastmcp import FastMCP

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

mcp = FastMCP("dax-query-server")


@mcp.tool()
def list_connections(connections_dir: str = DEFAULT_CONNECTIONS_DIR) -> str:
    """List the configured semantic model connections."""
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
    """Return metadata and markdown context for a named connection."""
    connection = _get_connection(connection_name, connections_dir)
    payload = {
        "connection_name": connection.name,
        "description": connection.description,
        "suggested_skill": connection.suggested_skill,
        "suggested_skill_reason": connection.suggested_skill_reason,
        "context_path": connection.context_path,
        "context_markdown": connection.context_markdown,
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
    """Run an ad hoc query against a named connection and return a preview. Use response_markdown when answering users."""
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
    """Run an ad hoc query against a named connection and return a ready-to-present markdown preview table."""
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
    queries_dir: str = "queries",
    overwrite: bool = False,
) -> str:
    """Save .dax and .dax.queryBuilder artifacts from a structured query builder JSON payload."""
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
    """Inspect model metadata for a named connection using non-admin MDSCHEMA rowsets. Present previews as markdown tables."""
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
    """Backward-compatible helper for the older query-centric workflow. Use response_markdown when answering users."""
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
    """Run an ad hoc DAX or rowset query against a semantic model connection. Use response_markdown when answering users."""
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


def _build_query_response_markdown(*, title: str, summary: dict[str, Any]) -> str:
    column_count = summary.get("column_count", len(summary.get("columns", [])))
    return (
        f"### {title}\n\n"
        f"- Rows: {summary['row_count']}\n"
        f"- Columns: {column_count}\n\n"
        f"{summary['markdown_table']}\n\n"
        f"---\n"
        f"**What would you like to do next?**\n\n"
        f"1. 🔍 **Filter / refine** — narrow to a specific account, TPID, or time range\n"
        f"2. 📊 **Aggregate** — total by month, by account, etc.\n"
        f"3. 📋 **Export as CSV** — save results to a CSV file\n"
        f"4. 🛠️ **Save to DAX Studio** — save as a `.dax` query builder file you can open in DAX Studio\n"
        f"5. 📦 **Scaffold Python workspace** — export to a standalone Python project with `uv run` support\n"
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

    Creates a standalone folder the user can share, run with `uv run`, or paste
    into a notebook.  Contains: run_query.py, queries/<name>.dax, pyproject.toml,
    and a README.

    IMPORTANT: Always ask the user where they want to save the workspace before
    calling this tool. Do NOT pick a directory on their behalf.

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
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

