"""Follow-up action registry for query-result workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class FollowupAction:
    """A server-authored action that can follow a DAX query result."""

    name: str
    label: str
    description: str
    required_params: tuple[str, ...]
    example_usage: str
    category: str
    optional_params: tuple[str, ...] = ()
    consumes: tuple[str, ...] = ("query_result",)
    produces: tuple[str, ...] = ()
    rendered: bool = True
    catalog_visible: bool = True
    grouped_visible: bool = True
    scope: str = "this_query"
    base_rank: int = 50
    rank_hints: tuple[str, ...] = field(default_factory=tuple)

    def rendered_label(self) -> str:
        """Return the human-readable menu line for query-result markdown."""
        return f"{self.label} - {self.description}"

    def to_catalog_item(self) -> dict[str, Any]:
        """Return the stable machine-readable catalog representation."""
        return {
            "name": self.name,
            "description": self.description,
            "required_params": list(self.required_params),
            "example_usage": self.example_usage,
            "optional_params": list(self.optional_params),
            "category": self.category,
            "scope": self.scope,
            "consumes": list(self.consumes),
            "produces": list(self.produces),
        }


FOLLOWUP_ACTIONS: tuple[FollowupAction, ...] = (
    FollowupAction(
        name="refine_query",
        label="Filter / refine",
        description="Narrow to a specific account, TPID, product, or time range.",
        required_params=("connection_name", "query"),
        example_usage='run_connection_query(connection_name="sales", query="EVALUATE ...")',
        category="query_iteration",
        consumes=("query", "query_result"),
        produces=("query", "query_result"),
        catalog_visible=False,
        base_rank=65,
        rank_hints=("large_result", "many_columns"),
        scope="this_query",
    ),
    FollowupAction(
        name="aggregate_query",
        label="Aggregate",
        description="Summarize results by month, account, product, or another useful grain.",
        required_params=("connection_name", "query"),
        example_usage='run_connection_query(connection_name="sales", query="EVALUATE SUMMARIZECOLUMNS(...)")',
        category="query_iteration",
        consumes=("query", "query_result"),
        produces=("query", "query_result"),
        catalog_visible=False,
        base_rank=60,
        rank_hints=("large_result", "numeric_columns"),
        scope="this_query",
    ),
    FollowupAction(
        name="save_to_workstation",
        label="Save to workstation",
        description="Save the query to the session workstation for iterative exploration and later export.",
        required_params=("connection_name", "query", "description"),
        example_usage='save_to_workstation(connection_name="sales", query="EVALUATE ...", description="Monthly revenue")',
        category="workflow_state",
        produces=("workstation_item",),
        base_rank=30,
        scope="this_query",
    ),
    FollowupAction(
        name="copy_to_clipboard",
        label="Copy to clipboard",
        description="Copy query results to the system clipboard as TSV for Excel or as markdown.",
        required_params=("connection_name", "query"),
        optional_params=("format", "max_rows"),
        example_usage='copy_to_clipboard(connection_name="sales", query="EVALUATE ...", format="tsv")',
        category="export",
        produces=("clipboard_data",),
        base_rank=40,
        scope="this_query",
    ),
    FollowupAction(
        name="export_to_csv",
        label="Export as CSV",
        description="Export query results to a timestamped CSV file.",
        required_params=("connection_name", "query", "output_dir"),
        optional_params=("filename_prefix", "max_rows"),
        example_usage='export_to_csv(connection_name="sales", query="EVALUATE ...", output_dir="./export")',
        category="export",
        produces=("csv_path",),
        base_rank=45,
        scope="this_query",
    ),
    FollowupAction(
        name="quick_chart",
        label="Quick chart",
        description="Generate a bar, line, or pie chart from query results.",
        required_params=("connection_name", "query", "chart_type", "x_column", "y_column"),
        optional_params=("output_path", "max_rows"),
        example_usage='quick_chart(connection_name="sales", query="EVALUATE ...", chart_type="bar", x_column="Month", y_column="Revenue")',
        category="visualization",
        produces=("chart_path",),
        base_rank=35,
        rank_hints=("numeric_columns",),
        scope="this_query",
    ),
    FollowupAction(
        name="scaffold_power_query",
        label="Scaffold Power Query",
        description="Generate Excel Power Query M code to import DAX query results.",
        required_params=("connection_name", "query"),
        optional_params=("table_name",),
        example_usage='scaffold_power_query(connection_name="sales", query="EVALUATE ...", table_name="DAXResults")',
        category="scaffold",
        produces=("power_query_m",),
        base_rank=60,
        scope="this_query",
    ),
    FollowupAction(
        name="scaffold_streamlit_app",
        label="Scaffold Streamlit",
        description="Generate a Streamlit dashboard app for visualizing query results.",
        required_params=("connection_name", "query"),
        optional_params=("title", "output_path"),
        example_usage='scaffold_streamlit_app(connection_name="sales", query="EVALUATE ...", title="Sales Dashboard")',
        category="scaffold",
        produces=("streamlit_app",),
        base_rank=70,
        scope="this_query",
    ),
    FollowupAction(
        name="scaffold_dax_studio",
        label="Save to DAX Studio",
        description="Save the query as .dax and .dax.queryBuilder artifacts for DAX Studio.",
        required_params=("query_builder_json", "queries_dir"),
        optional_params=("overwrite",),
        example_usage='save_query_builder(query_builder_json="...", queries_dir="./queries")',
        category="scaffold",
        produces=("dax_studio_files",),
        base_rank=80,
        scope="this_query",
    ),
    FollowupAction(
        name="scaffold_python",
        label="Scaffold Python",
        description="Generate a standalone Python project with run_query.py, notebook, and pyproject.toml.",
        required_params=("output_dir", "query_text"),
        optional_params=("query_name", "project_name", "connection_name"),
        example_usage='scaffold_dax_workspace(output_dir="./my_project", query_text="EVALUATE ...", connection_name="sales")',
        category="scaffold",
        produces=("python_workspace",),
        base_rank=90,
        scope="this_query",
    ),
    FollowupAction(
        name="rerun_last_query",
        label="Re-run last query",
        description="Execute the same query again after changing limits, profiling, or context.",
        required_params=("connection_name", "query"),
        example_usage='run_connection_query(connection_name="sales", query="EVALUATE ...", profile=True)',
        category="query_iteration",
        consumes=("query",),
        produces=("query_result",),
        catalog_visible=False,
        base_rank=95,
        scope="this_query",
    ),
    FollowupAction(
        name="create_query_pack",
        label="Create query pack",
        description="Start a durable pack.yaml workspace for reusable DAX queries.",
        required_params=("output_dir",),
        optional_params=("name", "description", "overwrite"),
        example_usage='create_query_pack(output_dir="./pack", name="Revenue Pack")',
        category="query_pack",
        consumes=(),
        produces=("query_pack",),
        rendered=False,
        catalog_visible=False,
        scope="current_pack",
        base_rank=25,
    ),
    FollowupAction(
        name="save_query_to_pack",
        label="Save query to pack",
        description="Persist the current query into a durable query pack.",
        required_params=("pack_path", "connection_name", "query", "description"),
        optional_params=("query_id", "display_name", "tags", "parameters_json", "table_name", "overwrite"),
        example_usage='save_query_to_pack(pack_path="./pack", connection_name="sales", query="EVALUATE ...", description="Monthly sales")',
        category="query_pack",
        consumes=("query",),
        produces=("query_pack_entry",),
        rendered=False,
        catalog_visible=False,
        scope="current_pack",
        base_rank=28,
    ),
    FollowupAction(
        name="validate_query_pack",
        label="Validate query pack",
        description="Check pack structure, safe DAX, connection references, and optional live smoke results.",
        required_params=("pack_path",),
        optional_params=("connections_dir", "dry_run", "max_rows", "continue_on_error"),
        example_usage='validate_query_pack(pack_path="./pack", connections_dir="./Connections", dry_run=True, max_rows=1)',
        category="query_pack",
        consumes=("query_pack",),
        produces=("validation_report",),
        rendered=False,
        catalog_visible=False,
        scope="current_pack",
        base_rank=32,
    ),
    FollowupAction(
        name="describe_query_pack",
        label="Describe query pack",
        description="Generate a markdown summary for sharing the pack with another analyst or agent.",
        required_params=("pack_path",),
        optional_params=("connections_dir", "include_validation"),
        example_usage='describe_query_pack(pack_path="./pack", connections_dir="./Connections")',
        category="query_pack",
        consumes=("query_pack",),
        produces=("markdown_summary",),
        rendered=False,
        catalog_visible=False,
        scope="current_pack",
        base_rank=34,
    ),
    FollowupAction(
        name="export_query_pack",
        label="Export query pack",
        description="Generate multi-query Python, Streamlit, Power Query, and README artifacts.",
        required_params=("pack_path",),
        optional_params=("output_dir", "connections_dir", "include_power_query", "include_streamlit", "overwrite"),
        example_usage='export_query_pack(pack_path="./pack", output_dir="./workspace", connections_dir="./Connections")',
        category="query_pack",
        consumes=("query_pack",),
        produces=("query_pack_workspace",),
        rendered=False,
        catalog_visible=False,
        scope="current_pack",
        base_rank=36,
    ),
    FollowupAction(
        name="save_validated_query",
        label="Save validated query",
        description="Save the current query as a reusable connection-scoped DAX example.",
        required_params=("connection_name", "query", "description"),
        optional_params=("query_id", "display_name", "tags", "parameters_json", "sample_parameters_json", "table_name", "overwrite"),
        example_usage='save_validated_query(connection_name="sales", query="EVALUATE ...", description="Monthly sales pattern")',
        category="validated_query_library",
        consumes=("query",),
        produces=("validated_query_entry",),
        rendered=False,
        catalog_visible=False,
        scope="validated_library",
        base_rank=29,
    ),
    FollowupAction(
        name="search_validated_queries",
        label="Search validated queries",
        description="Find known-good DAX examples for this connection before writing a new query.",
        required_params=("connection_name",),
        optional_params=("search_term", "tags", "connections_dir", "max_results", "include_dax"),
        example_usage='search_validated_queries(connection_name="sales", search_term="monthly revenue")',
        category="validated_query_library",
        consumes=(),
        produces=("validated_query_examples",),
        rendered=False,
        catalog_visible=False,
        scope="validated_library",
        base_rank=22,
    ),
    FollowupAction(
        name="validate_query_library",
        label="Validate query library",
        description="Smoke-test saved library examples and persist validation status, columns, and row counts.",
        required_params=("connection_name",),
        optional_params=("connections_dir", "query_id", "max_rows", "continue_on_error"),
        example_usage='validate_query_library(connection_name="sales", max_rows=1)',
        category="validated_query_library",
        consumes=("validated_query_entry",),
        produces=("validation_report",),
        rendered=False,
        catalog_visible=False,
        scope="validated_library",
        base_rank=31,
    ),
)


def catalog_actions() -> list[dict[str, Any]]:
    """Return actions shown in the stable follow-up resource catalog."""
    return [action.to_catalog_item() for action in FOLLOWUP_ACTIONS if action.catalog_visible]


def grouped_catalog_actions() -> dict[str, Any]:
    """Return follow-up actions grouped by whether they act on a query or a pack."""
    group_defs = {
        "this_query": {
            "scope": "this_query",
            "label": "This query",
            "description": "Actions that operate on the current query/result only.",
        },
        "current_pack": {
            "scope": "current_pack",
            "label": "Current pack",
            "description": "Durable multi-query actions for reusable query-pack workflows.",
        },
        "validated_library": {
            "scope": "validated_library",
            "label": "Validated query library",
            "description": "Connection-scoped known-good DAX examples for context and reuse.",
        },
    }
    groups: dict[str, dict[str, Any]] = {
        scope: {**definition, "actions": []}
        for scope, definition in group_defs.items()
    }
    for action in FOLLOWUP_ACTIONS:
        if action.grouped_visible:
            groups.setdefault(
                action.scope,
                {
                    "scope": action.scope,
                    "label": action.scope.replace("_", " ").title(),
                    "description": "",
                    "actions": [],
                },
            )
            groups[action.scope]["actions"].append(action.to_catalog_item())
    return {
        "groups": [group for group in groups.values() if group["actions"]],
        "flat_rendered_menu": rendered_next_steps(),
    }


def rendered_next_steps() -> list[str]:
    """Return the numbered menu labels embedded in query responses."""
    return [action.rendered_label() for action in FOLLOWUP_ACTIONS if action.rendered]


def recommend_actions(context: dict[str, Any] | None, *, limit: int = 5) -> list[dict[str, Any]]:
    """Rank follow-up actions for the latest query context."""
    if context is None:
        return []

    row_count = int(context.get("row_count") or 0)
    columns = list(context.get("columns") or [])
    numeric_columns = list(context.get("numeric_columns") or [])
    workstation_count = int(context.get("workstation_count") or 0)

    ranked: list[tuple[int, FollowupAction, list[str]]] = []
    for action in FOLLOWUP_ACTIONS:
        score = action.base_rank
        reasons: list[str] = []

        if action.name == "save_to_workstation" and workstation_count == 0:
            score -= 15
            reasons.append("preserves the query for multi-step analysis")
        if action.name == "quick_chart" and numeric_columns and len(columns) >= 2:
            score -= 20
            reasons.append("numeric result columns can be visualized")
        if action.name == "aggregate_query" and row_count >= 50:
            score -= 35
            reasons.append("large results may be easier to use after aggregation")
        if action.name == "refine_query" and row_count >= 100:
            score -= 45
            reasons.append("large results may need narrower filters")
        if action.name == "export_to_csv" and row_count > 0:
            score -= 5
            reasons.append("result can be persisted outside the chat")
        if action.name == "copy_to_clipboard" and 0 < row_count <= 1000:
            score -= 5
            reasons.append("result size is suitable for quick paste into Excel")

        ranked.append((score, action, reasons))

    ranked.sort(key=lambda item: (item[0], item[1].base_rank, item[1].name))
    return [
        {
            **action.to_catalog_item(),
            "rank": index + 1,
            "score": score,
            "reasons": reasons,
        }
        for index, (score, action, reasons) in enumerate(ranked[:limit])
    ]
