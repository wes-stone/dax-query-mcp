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
    ),
)


def catalog_actions() -> list[dict[str, Any]]:
    """Return actions shown in the stable follow-up resource catalog."""
    return [action.to_catalog_item() for action in FOLLOWUP_ACTIONS if action.catalog_visible]


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
