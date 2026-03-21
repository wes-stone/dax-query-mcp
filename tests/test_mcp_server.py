import json

import pandas as pd
from mcp.server.fastmcp.exceptions import ToolError

from dax_query_mcp.mcp_server import (
    _SERVER_INSTRUCTIONS,
    copy_to_clipboard,
    export_to_csv,
    get_connection_context,
    get_data_dictionary,
    get_query_builder_schema,
    get_schema,
    inspect_model_metadata,
    list_connections,
    quick_chart,
    run_connection_query,
    run_connection_query_markdown,
    save_query_builder,
    scaffold_power_query,
    scaffold_streamlit_app,
    search_columns,
    summarize_dataframe,
    summarize_rowset,
)


def test_summarize_dataframe_returns_preview_and_columns() -> None:
    dataframe = pd.DataFrame(
        {
            "When": pd.to_datetime(["2026-03-01", "2026-03-02"]),
            "Value": [1, 2],
        }
    )

    summary = summarize_dataframe(dataframe, preview_rows=1)

    assert summary["row_count"] == 2
    assert summary["column_count"] == 2
    assert summary["columns"] == ["When", "Value"]
    assert summary["preview"] == [{"When": "Mar-01-2026", "Value": 1}]
    assert "| When | Value |" in summary["markdown_table"]
    assert "markdown table" in summary["presentation_hint"]


def test_summarize_rowset_prefers_display_columns() -> None:
    dataframe = pd.DataFrame(
        {
            "CUBE_NAME": ["Model"],
            "DIMENSION_NAME": ["Account Information"],
            "DESCRIPTION": ["Account attributes"],
            "IGNORED": ["x"],
        }
    )

    summary = summarize_rowset(
        dataframe,
        preview_rows=5,
        preferred_columns=["CUBE_NAME", "DIMENSION_NAME", "DESCRIPTION"],
    )

    assert summary["row_count"] == 1
    assert summary["columns"] == ["CUBE_NAME", "DIMENSION_NAME", "DESCRIPTION", "IGNORED"]
    assert summary["preview"] == [
        {
            "CUBE_NAME": "Model",
            "DIMENSION_NAME": "Account Information",
            "DESCRIPTION": "Account attributes",
        }
    ]
    assert "| CUBE_NAME | DIMENSION_NAME | DESCRIPTION |" in summary["markdown_table"]
    assert "markdown table" in summary["presentation_hint"]


def test_connection_context_includes_suggested_skill(tmp_path) -> None:
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/SampleWorkspace?readonly;
  Initial Catalog=SampleSemanticModel
description: "Sales model"
suggested_skill: "enrollment-skills"
suggested_skill_reason: "Use this when you want help building KQL from this model's context."
""".strip(),
        encoding="utf-8",
    )
    (connections_dir / "sales.md").write_text("# Sales Model\n\nContext here.\n", encoding="utf-8")

    context_payload = json.loads(get_connection_context("sales", str(connections_dir)))
    listing_payload = json.loads(list_connections(str(connections_dir)))

    assert context_payload["suggested_skill"] == "enrollment-skills"
    assert "KQL" in context_payload["suggested_skill_reason"]
    assert listing_payload["connections"][0]["suggested_skill"] == "enrollment-skills"


def test_inspect_model_metadata_includes_presentation_hint(monkeypatch) -> None:
    monkeypatch.setattr(
        "dax_query_mcp.mcp_server.dax_to_pandas",
        lambda **kwargs: pd.DataFrame({"CUBE_NAME": ["Model"], "DESCRIPTION": ["Sample"]}),
    )

    payload = json.loads(inspect_model_metadata("Provider=MSOLAP.8;Data Source=localhost;Initial Catalog=Model"))

    assert "markdown table" in payload["presentation_hint"]
    assert "markdown table" in payload["cubes"]["presentation_hint"]


def test_run_connection_query_returns_ready_markdown(monkeypatch, tmp_path) -> None:
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/SampleWorkspace?readonly;
  Initial Catalog=SampleSemanticModel
description: "Sales model"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "dax_query_mcp.mcp_server.dax_to_pandas",
        lambda **kwargs: pd.DataFrame({"Month": ["2026-01"], "Revenue": [42]}),
    )

    payload = json.loads(
        run_connection_query(
            connection_name="sales",
            query="EVALUATE ROW(\"Revenue\", 42)",
            connections_dir=str(connections_dir),
            preview_rows=5,
        )
    )
    markdown_only = run_connection_query_markdown(
        connection_name="sales",
        query="EVALUATE ROW(\"Revenue\", 42)",
        connections_dir=str(connections_dir),
        preview_rows=5,
    )

    assert "| Month | Revenue |" in payload["markdown_table"]
    assert "| Month | Revenue |" in payload["response_markdown"]
    assert "next_steps" in payload
    assert len(payload["next_steps"]) == 5
    assert "### Query preview for `sales`" in markdown_only


def test_query_builder_schema_and_error_guidance() -> None:
    schema_payload = json.loads(get_query_builder_schema("example_connection"))

    assert schema_payload["example_payload"]["connection_name"] == "example_connection"
    assert "example_connection" in schema_payload["example_json"]

    try:
        save_query_builder(
            '{"name":"bad","connection_name":"example_connection","columns":[""]}',
            queries_dir="test_queries",
        )
    except (ValueError, ToolError) as exc:
        assert "get_query_builder_schema" in str(exc)
    else:
        raise AssertionError("Expected save_query_builder to raise on invalid payload")


def test_summarize_dataframe_strips_ansi_codes():
    """ANSI escape codes in column names or values must not leak into markdown."""
    df = pd.DataFrame({
        "\x1b[22;219mFiscal_Month\x1b[39m": ["Jan-2026"],
        "\x1b[19mRevenue\x1b[39m": [100.0],
    })
    result = summarize_dataframe(df, preview_rows=10)
    md = result["markdown_table"]
    assert "\x1b" not in md
    assert "Fiscal_Month" in md
    assert "Revenue" in md


def test_copy_to_clipboard_tsv(monkeypatch, tmp_path) -> None:
    """copy_to_clipboard with format='tsv' copies tab-separated data."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        "connection_string: |\n"
        "  Provider=MSOLAP.8;\n"
        "  Data Source=localhost;\n"
        "  Initial Catalog=Model\n"
        'description: "Sales"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "dax_query_mcp.mcp_server.dax_to_pandas",
        lambda **kwargs: pd.DataFrame({"Month": ["Jan", "Feb", "Mar"], "Revenue": [10, 20, 30]}),
    )

    clipboard_content = {}
    monkeypatch.setattr("pyperclip.copy", lambda text: clipboard_content.update(text=text))

    payload = json.loads(
        copy_to_clipboard(
            connection_name="sales",
            query='EVALUATE ROW("Revenue", 42)',
            format="tsv",
            connections_dir=str(connections_dir),
        )
    )

    assert payload["format"] == "tsv"
    assert payload["row_count"] == 3
    assert len(payload["preview"]) == 3
    assert "Copied 3 rows as TSV to clipboard." in payload["message"]
    assert "\t" in clipboard_content["text"]
    assert "Month\tRevenue" in clipboard_content["text"]


def test_copy_to_clipboard_markdown(monkeypatch, tmp_path) -> None:
    """copy_to_clipboard with format='markdown' copies a markdown table."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        "connection_string: |\n"
        "  Provider=MSOLAP.8;\n"
        "  Data Source=localhost;\n"
        "  Initial Catalog=Model\n"
        'description: "Sales"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "dax_query_mcp.mcp_server.dax_to_pandas",
        lambda **kwargs: pd.DataFrame({"Month": ["Jan", "Feb"], "Revenue": [10, 20]}),
    )

    clipboard_content = {}
    monkeypatch.setattr("pyperclip.copy", lambda text: clipboard_content.update(text=text))

    payload = json.loads(
        copy_to_clipboard(
            connection_name="sales",
            query='EVALUATE ROW("Revenue", 42)',
            format="markdown",
            connections_dir=str(connections_dir),
        )
    )

    assert payload["format"] == "markdown"
    assert payload["row_count"] == 2
    assert "| Month | Revenue |" in clipboard_content["text"]


def test_copy_to_clipboard_preview_capped_at_five(monkeypatch, tmp_path) -> None:
    """Preview in the response is capped at 5 rows even for larger results."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "big.yaml").write_text(
        "connection_string: |\n"
        "  Provider=MSOLAP.8;\n"
        "  Data Source=localhost;\n"
        "  Initial Catalog=Model\n"
        'description: "Big model"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "dax_query_mcp.mcp_server.dax_to_pandas",
        lambda **kwargs: pd.DataFrame({"Val": list(range(20))}),
    )

    clipboard_content = {}
    monkeypatch.setattr("pyperclip.copy", lambda text: clipboard_content.update(text=text))

    payload = json.loads(
        copy_to_clipboard(
            connection_name="big",
            query="EVALUATE VALUES('Table'[Val])",
            connections_dir=str(connections_dir),
        )
    )

    assert payload["row_count"] == 20
    assert len(payload["preview"]) == 5
    # Full data in clipboard (all 20 rows + header)
    lines = clipboard_content["text"].strip().split("\n")
    assert len(lines) == 21


def test_export_to_csv_creates_file(tmp_path) -> None:
    """export_to_csv writes a CSV and returns correct metadata."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "contoso.yaml").write_text(
        "connection_string: 'MOCK://contoso'\ndescription: 'Mock cube'\n",
        encoding="utf-8",
    )

    result = json.loads(
        export_to_csv(
            connection_name="contoso",
            query="EVALUATE Products",
            output_dir=str(tmp_path / "out"),
            connections_dir=str(connections_dir),
            filename_prefix="products",
        )
    )

    assert result["row_count"] == 5
    assert result["column_count"] == 4
    assert result["file_path"].endswith(".csv")
    assert "products_" in result["file_path"]

    import csv
    with open(result["file_path"], newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    assert header == ["ProductKey", "ProductName", "Category", "Price"]
    assert len(rows) == 5


def test_export_to_csv_default_prefix(tmp_path) -> None:
    """Default filename_prefix is 'export'."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "contoso.yaml").write_text(
        "connection_string: 'MOCK://contoso'\ndescription: 'Mock cube'\n",
        encoding="utf-8",
    )

    result = json.loads(
        export_to_csv(
            connection_name="contoso",
            query="EVALUATE Products",
            output_dir=str(tmp_path / "out"),
            connections_dir=str(connections_dir),
        )
    )

    assert "export_" in result["file_path"]


def test_export_to_csv_creates_output_dir(tmp_path) -> None:
    """output_dir is created if it doesn't exist."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "contoso.yaml").write_text(
        "connection_string: 'MOCK://contoso'\ndescription: 'Mock cube'\n",
        encoding="utf-8",
    )

    nested_dir = tmp_path / "a" / "b" / "c"
    assert not nested_dir.exists()

    result = json.loads(
        export_to_csv(
            connection_name="contoso",
            query="EVALUATE Products",
            output_dir=str(nested_dir),
            connections_dir=str(connections_dir),
        )
    )

    assert nested_dir.exists()
    assert result["row_count"] == 5


def _make_contoso_connections(tmp_path):
    """Helper: create a Connections dir with a mock contoso connection."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "contoso.yaml").write_text(
        "connection_string: 'MOCK://contoso'\ndescription: 'Mock cube'\n",
        encoding="utf-8",
    )
    return connections_dir


def test_quick_chart_bar(monkeypatch, tmp_path) -> None:
    """quick_chart generates a bar chart and returns correct metadata."""
    connections_dir = _make_contoso_connections(tmp_path)

    saved = {}
    monkeypatch.setattr(
        "matplotlib.pyplot.savefig",
        lambda *a, **kw: saved.update(called=True),
    )

    result = json.loads(
        quick_chart(
            connection_name="contoso",
            query="EVALUATE Products",
            chart_type="bar",
            x_column="ProductName",
            y_column="Price",
            output_path=str(tmp_path / "bar_chart.png"),
            connections_dir=str(connections_dir),
        )
    )

    assert result["chart_type"] == "bar"
    assert result["row_count"] == 5
    assert result["file_path"].endswith("bar_chart.png")


def test_quick_chart_line(monkeypatch, tmp_path) -> None:
    """quick_chart generates a line chart."""
    connections_dir = _make_contoso_connections(tmp_path)

    monkeypatch.setattr(
        "matplotlib.pyplot.savefig",
        lambda *a, **kw: None,
    )

    result = json.loads(
        quick_chart(
            connection_name="contoso",
            query="EVALUATE Products",
            chart_type="line",
            x_column="ProductName",
            y_column="Price",
            output_path=str(tmp_path / "line_chart.png"),
            connections_dir=str(connections_dir),
        )
    )

    assert result["chart_type"] == "line"
    assert result["row_count"] == 5
    assert result["file_path"].endswith("line_chart.png")


def test_quick_chart_pie(monkeypatch, tmp_path) -> None:
    """quick_chart generates a pie chart."""
    connections_dir = _make_contoso_connections(tmp_path)

    monkeypatch.setattr(
        "matplotlib.pyplot.savefig",
        lambda *a, **kw: None,
    )

    result = json.loads(
        quick_chart(
            connection_name="contoso",
            query="EVALUATE Products",
            chart_type="pie",
            x_column="ProductName",
            y_column="Price",
            output_path=str(tmp_path / "pie_chart.png"),
            connections_dir=str(connections_dir),
        )
    )

    assert result["chart_type"] == "pie"
    assert result["row_count"] == 5
    assert result["file_path"].endswith("pie_chart.png")


def test_scaffold_power_query_contains_connection_string(tmp_path) -> None:
    """M code includes the connection string from the named connection."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        "connection_string: |\n"
        "  Provider=MSOLAP.8;\n"
        "  Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;\n"
        "  Initial Catalog=Model\n"
        'description: "Sales"\n',
        encoding="utf-8",
    )

    payload = json.loads(
        scaffold_power_query(
            connection_name="sales",
            query='EVALUATE ROW("Revenue", 42)',
            connections_dir=str(connections_dir),
        )
    )

    assert "powerbi://api.powerbi.com/v1.0/myorg/Workspace" in payload["m_code"]


def test_scaffold_power_query_contains_dax_query(tmp_path) -> None:
    """M code includes the DAX query text."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        "connection_string: 'Provider=MSOLAP.8;Data Source=localhost;Initial Catalog=Model'\n"
        'description: "Sales"\n',
        encoding="utf-8",
    )

    dax = 'EVALUATE SUMMARIZECOLUMNS(Sales[Month], "Total", SUM(Sales[Amount]))'
    payload = json.loads(
        scaffold_power_query(
            connection_name="sales",
            query=dax,
            connections_dir=str(connections_dir),
        )
    )

    assert "SUMMARIZECOLUMNS" in payload["m_code"]
    assert "Sales[Amount]" in payload["m_code"]


def test_scaffold_power_query_custom_table_name(tmp_path) -> None:
    """Custom table_name is reflected in the response."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        "connection_string: 'Provider=MSOLAP.8;Data Source=localhost;Initial Catalog=Model'\n"
        'description: "Sales"\n',
        encoding="utf-8",
    )

    payload = json.loads(
        scaffold_power_query(
            connection_name="sales",
            query='EVALUATE ROW("X", 1)',
            table_name="MonthlySales",
            connections_dir=str(connections_dir),
        )
    )

    assert payload["table_name"] == "MonthlySales"
    assert "MonthlySales" in payload["instructions"]


def test_scaffold_power_query_default_table_name(tmp_path) -> None:
    """Default table_name is 'DAXResults'."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        "connection_string: 'Provider=MSOLAP.8;Data Source=localhost;Initial Catalog=Model'\n"
        'description: "Sales"\n',
        encoding="utf-8",
    )

    payload = json.loads(
        scaffold_power_query(
            connection_name="sales",
            query='EVALUATE ROW("X", 1)',
            connections_dir=str(connections_dir),
        )
    )

    assert payload["table_name"] == "DAXResults"
    assert "DAXResults" in payload["instructions"]


# ── scaffold_streamlit_app tests ─────────────────────────────────────


def test_scaffold_streamlit_app_contains_streamlit_imports() -> None:
    """Generated code imports streamlit and pandas."""
    payload = json.loads(
        scaffold_streamlit_app(
            connection_name="sales",
            query='EVALUATE ROW("Revenue", 42)',
        )
    )
    code = payload["code"]
    assert "import streamlit as st" in code
    assert "import pandas as pd" in code


def test_scaffold_streamlit_app_contains_dax_query() -> None:
    """Generated code embeds the DAX query string."""
    dax = 'EVALUATE SUMMARIZECOLUMNS(Sales[Month], "Total", SUM(Sales[Amount]))'
    payload = json.loads(
        scaffold_streamlit_app(
            connection_name="sales",
            query=dax,
        )
    )
    assert "SUMMARIZECOLUMNS" in payload["code"]
    assert "Sales[Amount]" in payload["code"]


def test_scaffold_streamlit_app_custom_title() -> None:
    """Custom title is reflected in the generated code."""
    payload = json.loads(
        scaffold_streamlit_app(
            connection_name="sales",
            query='EVALUATE ROW("X", 1)',
            title="Monthly Revenue Dashboard",
        )
    )
    assert "Monthly Revenue Dashboard" in payload["code"]


def test_scaffold_streamlit_app_writes_file(tmp_path) -> None:
    """When output_path is provided, the file is written to disk."""
    out_file = tmp_path / "my_app.py"
    payload = json.loads(
        scaffold_streamlit_app(
            connection_name="sales",
            query='EVALUATE ROW("X", 1)',
            output_path=str(out_file),
        )
    )
    assert out_file.exists()
    assert payload["file_path"] == str(out_file)
    contents = out_file.read_text(encoding="utf-8")
    assert "import streamlit as st" in contents
    assert payload["code"] == contents


# ── _SERVER_INSTRUCTIONS content tests ───────────────────────────────


def test_server_instructions_contains_tool_overview() -> None:
    """_SERVER_INSTRUCTIONS must include a tool overview section."""
    assert "## Tool overview" in _SERVER_INSTRUCTIONS
    for tool_name in [
        "list_connections",
        "get_connection_context",
        "run_connection_query",
        "export_to_csv",
        "copy_to_clipboard",
    ]:
        assert tool_name in _SERVER_INSTRUCTIONS


def test_server_instructions_contains_dax_best_practices() -> None:
    """_SERVER_INSTRUCTIONS must include DAX best practices."""
    assert "## DAX best practices" in _SERVER_INSTRUCTIONS
    assert "EVALUATE" in _SERVER_INSTRUCTIONS
    assert "SUMMARIZE" in _SERVER_INSTRUCTIONS


def test_server_instructions_contains_error_codes() -> None:
    """_SERVER_INSTRUCTIONS must document all error codes."""
    assert "## Error codes" in _SERVER_INSTRUCTIONS
    for code in [
        "ADMIN_QUERY_BLOCKED",
        "CONNECTION_NOT_FOUND",
        "QUERY_TIMEOUT",
        "EXECUTION_FAILED",
        "INVALID_PARAMS",
    ]:
        assert code in _SERVER_INSTRUCTIONS


def test_server_instructions_contains_follow_up_options() -> None:
    """_SERVER_INSTRUCTIONS must list follow-up actions."""
    assert "## Follow-up options" in _SERVER_INSTRUCTIONS
    assert "export_to_csv" in _SERVER_INSTRUCTIONS
    assert "copy_to_clipboard" in _SERVER_INSTRUCTIONS
    assert "scaffold" in _SERVER_INSTRUCTIONS.lower()


# ── search_columns tests ────────────────────────────────────────────


def _make_contoso_connections_with_dd(tmp_path):
    """Helper: create Connections dir with mock contoso + data dictionary."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "contoso.yaml").write_text(
        "connection_string: 'MOCK://contoso'\ndescription: 'Mock cube'\n",
        encoding="utf-8",
    )
    import shutil
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "Connections" / "mock_contoso.data_dictionary.yaml"
    shutil.copy(src, connections_dir / "contoso.data_dictionary.yaml")
    return connections_dir


def test_search_columns_exact_name(tmp_path) -> None:
    """Exact column name match appears first in results."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    results = json.loads(
        search_columns(
            connection_name="contoso",
            search_term="Price",
            connections_dir=str(connections_dir),
        )
    )
    assert len(results) > 0
    assert results[0]["column"] == "Price"
    assert results[0]["table"] == "Products"


def test_search_columns_partial_name(tmp_path) -> None:
    """Partial name match finds columns containing the term."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    results = json.loads(
        search_columns(
            connection_name="contoso",
            search_term="Key",
            connections_dir=str(connections_dir),
        )
    )
    assert len(results) >= 3
    column_names = [r["column"] for r in results]
    assert "ProductKey" in column_names
    assert "DateKey" in column_names
    assert "SalesKey" in column_names


def test_search_columns_case_insensitive(tmp_path) -> None:
    """Search is case-insensitive."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    upper = json.loads(
        search_columns(
            connection_name="contoso",
            search_term="PRICE",
            connections_dir=str(connections_dir),
        )
    )
    lower = json.loads(
        search_columns(
            connection_name="contoso",
            search_term="price",
            connections_dir=str(connections_dir),
        )
    )
    assert len(upper) > 0
    assert len(upper) == len(lower)
    assert upper[0]["column"] == lower[0]["column"]


def test_search_columns_includes_descriptions(tmp_path) -> None:
    """Data dictionary descriptions are included and searchable."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    # Search by description text rather than column name
    results = json.loads(
        search_columns(
            connection_name="contoso",
            search_term="foreign key",
            connections_dir=str(connections_dir),
        )
    )
    assert len(results) >= 1
    matched_descs = [r["description"] for r in results]
    assert any("Foreign key" in d for d in matched_descs)


def test_search_columns_max_results(tmp_path) -> None:
    """max_results limits the number of returned matches."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    results = json.loads(
        search_columns(
            connection_name="contoso",
            search_term="e",  # broad term to match many columns
            connections_dir=str(connections_dir),
            max_results=3,
        )
    )
    assert len(results) <= 3


# ── get_data_dictionary tests ────────────────────────────────────────


def test_get_data_dictionary_found(tmp_path) -> None:
    """get_data_dictionary returns the data dictionary when the file exists."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    payload = json.loads(
        get_data_dictionary(
            connection_name="contoso",
            connections_dir=str(connections_dir),
        )
    )
    assert payload["found"] is True
    assert payload["connection_name"] == "contoso"
    dd = payload["data_dictionary"]
    assert dd["version"] == "1.0"
    assert len(dd["tables"]) == 3
    assert len(dd["measures"]) == 5


def test_get_data_dictionary_not_found(tmp_path) -> None:
    """get_data_dictionary returns a helpful message when no file exists."""
    connections_dir = _make_contoso_connections(tmp_path)
    payload = json.loads(
        get_data_dictionary(
            connection_name="contoso",
            connections_dir=str(connections_dir),
        )
    )
    assert payload["found"] is False
    assert "No data dictionary found" in payload["message"]


# ── get_schema tests ─────────────────────────────────────────────────


def test_get_schema_with_data_dictionary(tmp_path) -> None:
    """get_schema includes table/column/measure descriptions from the data dictionary."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    payload = json.loads(
        get_schema(
            connection_name="contoso",
            connections_dir=str(connections_dir),
        )
    )
    assert payload["has_data_dictionary"] is True
    assert payload["connection_name"] == "contoso"

    # Table descriptions
    table_names = [t["name"] for t in payload["tables"]]
    assert "Products" in table_names
    assert "Sales" in table_names

    products = next(t for t in payload["tables"] if t["name"] == "Products")
    assert products["description"] != ""
    assert any(c["description"] != "" for c in products["columns"])

    # Measures
    assert len(payload["measures"]) == 5
    assert any(m["description"] != "" for m in payload["measures"])

    # Filters
    assert len(payload["filters"]) == 3


def test_get_schema_without_data_dictionary(tmp_path) -> None:
    """get_schema returns a fallback message when no data dictionary exists."""
    connections_dir = _make_contoso_connections(tmp_path)
    payload = json.loads(
        get_schema(
            connection_name="contoso",
            connections_dir=str(connections_dir),
        )
    )
    assert payload["has_data_dictionary"] is False
    assert "inspect_connection" in payload["message"]

