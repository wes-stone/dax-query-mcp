import json
from pathlib import Path

import pandas as pd
from fastmcp.exceptions import ToolError

from dax_query_mcp.mcp_server import (
    _FOLLOWUP_MENU,
    _SERVER_INSTRUCTIONS,
    clear_workstation,
    copy_to_clipboard,
    export_to_csv,
    export_workstation,
    followup_menu,
    generate_data_dictionary,
    get_connection_context,
    get_data_dictionary,
    get_query_builder_schema,
    get_schema,
    inspect_model_metadata,
    list_connections,
    list_workstation,
    quick_chart,
    remove_from_workstation,
    run_connection_query,
    save_query_builder,
    save_to_workstation,
    scaffold_power_query,
    scaffold_streamlit_app,
    search_columns,
    search_connection_context,
    search_measures,
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

    result = run_connection_query(
        connection_name="sales",
        query="EVALUATE ROW(\"Revenue\", 42)",
        connections_dir=str(connections_dir),
        preview_rows=5,
    )

    # Result is a plain markdown string, not JSON
    assert isinstance(result, str)
    assert "| Month | Revenue |" in result
    assert "What would you like to do next?" in result
    assert "Copy to clipboard" in result
    assert "### Query preview for `sales`" in result


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
    """_SERVER_INSTRUCTIONS must describe follow-up behaviour."""
    assert "## Follow-up options" in _SERVER_INSTRUCTIONS
    assert "What would you like" in _SERVER_INSTRUCTIONS or "next?" in _SERVER_INSTRUCTIONS.lower()
    assert "verbatim" in _SERVER_INSTRUCTIONS.lower() or "baked" in _SERVER_INSTRUCTIONS.lower()


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


# ── search_measures tests ───────────────────────────────────────────


def test_search_measures_exact_name(tmp_path) -> None:
    """Exact measure name match appears first in results."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    results = json.loads(
        search_measures(
            connection_name="contoso",
            search_term="Total Sales",
            connections_dir=str(connections_dir),
        )
    )
    assert len(results) > 0
    assert results[0]["name"] == "Total Sales"


def test_search_measures_partial_name(tmp_path) -> None:
    """Partial name match finds measures containing the term."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    results = json.loads(
        search_measures(
            connection_name="contoso",
            search_term="Total",
            connections_dir=str(connections_dir),
        )
    )
    assert len(results) >= 2
    names = [r["name"] for r in results]
    assert "Total Sales" in names
    assert "Total Quantity" in names


def test_search_measures_case_insensitive(tmp_path) -> None:
    """Search is case-insensitive."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    upper = json.loads(
        search_measures(
            connection_name="contoso",
            search_term="TOTAL SALES",
            connections_dir=str(connections_dir),
        )
    )
    lower = json.loads(
        search_measures(
            connection_name="contoso",
            search_term="total sales",
            connections_dir=str(connections_dir),
        )
    )
    assert len(upper) > 0
    assert len(upper) == len(lower)
    assert upper[0]["name"] == lower[0]["name"]


def test_search_measures_includes_descriptions(tmp_path) -> None:
    """Data dictionary descriptions are included and searchable."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    results = json.loads(
        search_measures(
            connection_name="contoso",
            search_term="units sold",
            connections_dir=str(connections_dir),
        )
    )
    assert len(results) >= 1
    matched_descs = [r["description"] for r in results]
    assert any("units sold" in d.lower() for d in matched_descs)


def test_search_measures_max_results(tmp_path) -> None:
    """max_results limits the number of returned matches."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    results = json.loads(
        search_measures(
            connection_name="contoso",
            search_term="a",  # broad term to match many measures
            connections_dir=str(connections_dir),
            max_results=2,
        )
    )
    assert len(results) <= 2


def test_search_measures_expression_truncated(tmp_path) -> None:
    """Expressions longer than 100 characters are truncated with ellipsis."""
    connections_dir = _make_contoso_connections_with_dd(tmp_path)
    import yaml
    dd_path = connections_dir / "contoso.data_dictionary.yaml"
    with open(dd_path, encoding="utf-8") as fh:
        dd = yaml.safe_load(fh)
    long_expr = "SUMX(Sales, Sales[Amount] * Sales[Quantity])" * 5
    dd["measures"].append({
        "name": "Long Measure",
        "expression": long_expr,
        "description": "A measure with a very long expression",
    })
    with open(dd_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(dd, fh)

    results = json.loads(
        search_measures(
            connection_name="contoso",
            search_term="Long Measure",
            connections_dir=str(connections_dir),
        )
    )
    assert len(results) == 1
    assert results[0]["expression"].endswith("...")
    assert len(results[0]["expression"]) <= 104  # 100 chars + "..."


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


# ── generate_data_dictionary tests ───────────────────────────────────


def test_generate_data_dictionary_valid_yaml(tmp_path) -> None:
    """generate_data_dictionary returns valid YAML from mock cube."""
    connections_dir = _make_contoso_connections(tmp_path)
    payload = json.loads(
        generate_data_dictionary(
            connection_name="contoso",
            connections_dir=str(connections_dir),
        )
    )
    assert "yaml_content" in payload
    assert payload["table_count"] == 3
    assert payload["measure_count"] == 5
    assert "file_path" not in payload

    import yaml

    parsed = yaml.safe_load(payload["yaml_content"])
    assert parsed["version"] == "1.0"
    table_names = [t["name"] for t in parsed["tables"]]
    assert "Sales" in table_names
    assert "Products" in table_names
    assert "Calendar" in table_names


def test_generate_data_dictionary_roundtrip(tmp_path) -> None:
    """YAML from generate_data_dictionary can be parsed back to DataDictionary."""
    from dax_query_mcp.data_dictionary import DataDictionary

    connections_dir = _make_contoso_connections(tmp_path)
    payload = json.loads(
        generate_data_dictionary(
            connection_name="contoso",
            connections_dir=str(connections_dir),
        )
    )
    import yaml

    raw = yaml.safe_load(payload["yaml_content"])
    dd = DataDictionary.model_validate(raw)
    assert len(dd.tables) == 3
    assert len(dd.measures) == 5
    assert all(t.description == "" for t in dd.tables)
    assert all(m.description == "" for m in dd.measures)


def test_generate_data_dictionary_file_write(tmp_path) -> None:
    """generate_data_dictionary writes YAML to disk when output_path provided."""
    connections_dir = _make_contoso_connections(tmp_path)
    out_file = tmp_path / "output" / "generated.data_dictionary.yaml"
    payload = json.loads(
        generate_data_dictionary(
            connection_name="contoso",
            connections_dir=str(connections_dir),
            output_path=str(out_file),
        )
    )
    assert payload["file_path"] == str(out_file)
    assert out_file.exists()

    from dax_query_mcp.data_dictionary import load_data_dictionary

    dd = load_data_dictionary(out_file)
    assert len(dd.tables) == 3
    assert len(dd.measures) == 5


def test_generate_data_dictionary_correct_counts(tmp_path) -> None:
    """generate_data_dictionary returns correct table and measure counts."""
    connections_dir = _make_contoso_connections(tmp_path)
    payload = json.loads(
        generate_data_dictionary(
            connection_name="contoso",
            connections_dir=str(connections_dir),
        )
    )
    assert payload["table_count"] == 3
    assert payload["measure_count"] == 5
    import yaml

    measure_names = [m["name"] for m in yaml.safe_load(payload["yaml_content"])["measures"]]
    assert "Total Sales" in measure_names
    assert "Total Quantity" in measure_names
    assert "Avg Price" in measure_names
    assert "Product Count" in measure_names
    assert "Day Count" in measure_names


# ── followup_menu resource tests ────────────────────────────────────────

_EXPECTED_ACTIONS = {
    "export_to_csv",
    "copy_to_clipboard",
    "quick_chart",
    "scaffold_power_query",
    "scaffold_streamlit_app",
    "scaffold_python",
    "scaffold_dax_studio",
    "save_to_workstation",
}


def test_followup_menu_returns_valid_json() -> None:
    raw = followup_menu()
    payload = json.loads(raw)
    assert "actions" in payload
    assert isinstance(payload["actions"], list)


def test_followup_menu_contains_all_expected_actions() -> None:
    payload = json.loads(followup_menu())
    action_names = {a["name"] for a in payload["actions"]}
    assert action_names == _EXPECTED_ACTIONS


def test_followup_menu_actions_have_required_fields() -> None:
    payload = json.loads(followup_menu())
    for action in payload["actions"]:
        assert "name" in action, f"Missing 'name' in action: {action}"
        assert "description" in action, f"Missing 'description' in {action['name']}"
        assert "required_params" in action, f"Missing 'required_params' in {action['name']}"
        assert isinstance(action["required_params"], list)
        assert isinstance(action["description"], str)
        assert len(action["description"]) > 0


def test_followup_menu_actions_have_example_usage() -> None:
    payload = json.loads(followup_menu())
    for action in payload["actions"]:
        assert "example_usage" in action, f"Missing 'example_usage' in {action['name']}"
        assert isinstance(action["example_usage"], str)
        assert len(action["example_usage"]) > 0


# ---------------------------------------------------------------------------
# Docstring quality tests
# ---------------------------------------------------------------------------

from dax_query_mcp.mcp_server import mcp as _mcp_server


def _get_mcp_tool_functions():
    """Return (name, func) pairs for every @mcp.tool()-registered function."""
    import asyncio
    lp = getattr(_mcp_server, '_local_provider', None) or getattr(_mcp_server, 'local_provider', None)
    tools = asyncio.run(lp.list_tools())
    return [(t.name, t.fn) for t in tools]


_PLACEHOLDER_PREFIXES = ("todo", "fixme", "hack", "xxx", "placeholder")


def test_all_mcp_tools_have_docstrings() -> None:
    """Every @mcp.tool() function must have a non-empty docstring."""
    tools = _get_mcp_tool_functions()
    assert len(tools) > 0, "No MCP tools found — check discovery logic"
    missing = [name for name, fn in tools if not (fn.__doc__ or "").strip()]
    assert not missing, f"MCP tools missing docstrings: {missing}"


def test_all_mcp_tool_docstrings_are_substantive() -> None:
    """Every @mcp.tool() docstring must have at least 20 characters."""
    tools = _get_mcp_tool_functions()
    short = [
        (name, len((fn.__doc__ or "").strip()))
        for name, fn in tools
        if len((fn.__doc__ or "").strip()) < 20
    ]
    assert not short, f"MCP tools with too-short docstrings (<20 chars): {short}"


def test_no_mcp_tool_docstrings_are_placeholders() -> None:
    """No @mcp.tool() docstring should start with TODO/FIXME/HACK/XXX."""
    tools = _get_mcp_tool_functions()
    placeholders = [
        name
        for name, fn in tools
        if (fn.__doc__ or "").strip().lower().startswith(_PLACEHOLDER_PREFIXES)
    ]
    assert not placeholders, f"MCP tools with placeholder docstrings: {placeholders}"


# ── Workstation tests ────────────────────────────────────────────────


def test_save_to_workstation(tmp_path) -> None:
    """save_to_workstation creates a .workstation.json file."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    payload = json.loads(
        save_to_workstation(
            connection_name="sales",
            query='EVALUATE ROW("Revenue", 42)',
            description="Monthly revenue",
            query_name="monthly_revenue",
            connections_dir=str(connections_dir),
        )
    )

    assert payload["query_name"] == "monthly_revenue"
    ws_file = Path(payload["path"])
    assert ws_file.exists()
    saved = json.loads(ws_file.read_text(encoding="utf-8"))
    assert saved["query_name"] == "monthly_revenue"
    assert saved["connection_name"] == "sales"
    assert saved["query"] == 'EVALUATE ROW("Revenue", 42)'
    assert saved["description"] == "Monthly revenue"
    assert "saved_at" in saved


def test_save_to_workstation_auto_name(tmp_path) -> None:
    """Auto-generates slug from description when query_name is blank."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    payload = json.loads(
        save_to_workstation(
            connection_name="sales",
            query='EVALUATE ROW("X", 1)',
            description="Top 10 Products by Revenue",
            connections_dir=str(connections_dir),
        )
    )

    assert payload["query_name"] == "top_10_products_by_revenue"
    assert Path(payload["path"]).exists()


def test_list_workstation_empty(tmp_path) -> None:
    """Returns empty list message when no queries saved."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    payload = json.loads(list_workstation(connections_dir=str(connections_dir)))

    assert payload["count"] == 0
    assert payload["queries"] == []
    assert "empty" in payload["message"].lower()


def test_list_workstation_with_queries(tmp_path) -> None:
    """Shows saved queries after saving them."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    save_to_workstation(
        connection_name="sales",
        query="EVALUATE Sales",
        description="All sales",
        query_name="all_sales",
        connections_dir=str(connections_dir),
    )
    save_to_workstation(
        connection_name="sales",
        query="EVALUATE Products",
        description="All products",
        query_name="all_products",
        connections_dir=str(connections_dir),
    )

    payload = json.loads(list_workstation(connections_dir=str(connections_dir)))

    assert payload["count"] == 2
    names = [q["query_name"] for q in payload["queries"]]
    assert "all_sales" in names
    assert "all_products" in names


def test_remove_from_workstation(tmp_path) -> None:
    """Removes a query from the workstation."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    save_to_workstation(
        connection_name="sales",
        query="EVALUATE Sales",
        description="All sales",
        query_name="all_sales",
        connections_dir=str(connections_dir),
    )

    payload = json.loads(
        remove_from_workstation(
            query_name="all_sales",
            connections_dir=str(connections_dir),
        )
    )

    assert payload["query_name"] == "all_sales"
    assert "removed" in payload["message"].lower()

    listing = json.loads(list_workstation(connections_dir=str(connections_dir)))
    assert listing["count"] == 0


def test_remove_from_workstation_not_found(tmp_path) -> None:
    """Error when removing a query that doesn't exist."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    try:
        remove_from_workstation(
            query_name="nonexistent",
            connections_dir=str(connections_dir),
        )
    except (ValueError, ToolError) as exc:
        assert "not found" in str(exc).lower()
    else:
        raise AssertionError("Expected remove_from_workstation to raise on missing query")


def test_clear_workstation(tmp_path) -> None:
    """Clears all queries from the workstation."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    save_to_workstation(
        connection_name="s", query="Q1", description="D1",
        query_name="q1", connections_dir=str(connections_dir),
    )
    save_to_workstation(
        connection_name="s", query="Q2", description="D2",
        query_name="q2", connections_dir=str(connections_dir),
    )

    payload = json.loads(clear_workstation(connections_dir=str(connections_dir)))

    assert payload["removed_count"] == 2
    listing = json.loads(list_workstation(connections_dir=str(connections_dir)))
    assert listing["count"] == 0


def test_export_workstation_scaffold(tmp_path) -> None:
    """Exports scaffold workspace with multiple queries."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    save_to_workstation(
        connection_name="sales",
        query='EVALUATE ROW("Revenue", 42)',
        description="Revenue query",
        query_name="revenue",
        connections_dir=str(connections_dir),
    )
    save_to_workstation(
        connection_name="finance",
        query='EVALUATE ROW("Cost", 10)',
        description="Cost query",
        query_name="cost",
        connections_dir=str(connections_dir),
    )

    out_dir = tmp_path / "export_scaffold"
    payload = json.loads(
        export_workstation(
            output_dir=str(out_dir),
            connections_dir=str(connections_dir),
            format="scaffold",
        )
    )

    assert len(payload["files_created"]) >= 4  # 2 .dax + run_queries.py + pyproject + README
    assert (out_dir / "queries" / "revenue.dax").exists()
    assert (out_dir / "queries" / "cost.dax").exists()
    assert (out_dir / "run_queries.py").exists()
    assert (out_dir / "pyproject.toml").exists()
    assert (out_dir / "README.md").exists()

    readme = (out_dir / "README.md").read_text(encoding="utf-8")
    assert "revenue" in readme
    assert "cost" in readme

    run_script = (out_dir / "run_queries.py").read_text(encoding="utf-8")
    assert "revenue" in run_script
    assert "cost" in run_script


def test_export_workstation_dax(tmp_path) -> None:
    """Exports only .dax files in dax format."""
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()

    save_to_workstation(
        connection_name="sales",
        query='EVALUATE ROW("Revenue", 42)',
        description="Revenue query",
        query_name="revenue",
        connections_dir=str(connections_dir),
    )

    out_dir = tmp_path / "export_dax"
    payload = json.loads(
        export_workstation(
            output_dir=str(out_dir),
            connections_dir=str(connections_dir),
            format="dax",
        )
    )

    assert len(payload["files_created"]) == 1
    assert (out_dir / "queries" / "revenue.dax").exists()
    # No scaffold files
    assert not (out_dir / "run_queries.py").exists()
    assert not (out_dir / "pyproject.toml").exists()


# ── Tiered context tests ───────────────────────────────────────────────


def test_get_connection_context_overview_default(tmp_path) -> None:
    """get_connection_context returns overview by default when available."""
    connections_dir = tmp_path / "conns"
    connections_dir.mkdir()
    (connections_dir / "myconn.yaml").write_text(
        'connection_string: "Provider=MSOLAP;Data Source=X"\ndescription: "Test"'
    )
    (connections_dir / "myconn_overview.md").write_text("# Overview\nKey tables: Sales, Products")
    (connections_dir / "myconn.md").write_text("# Full Context\n" + "Lots of detail\n" * 100)

    payload = json.loads(get_connection_context("myconn", str(connections_dir)))

    assert payload["detail_level"] == "overview"
    assert payload["has_overview"] is True
    assert payload["has_full_context"] is True
    assert "# Overview" in payload["context_markdown"]
    assert "Lots of detail" not in payload["context_markdown"]
    assert "NOTE" in payload


def test_get_connection_context_full_detail(tmp_path) -> None:
    """get_connection_context with detail='full' returns full context."""
    connections_dir = tmp_path / "conns"
    connections_dir.mkdir()
    (connections_dir / "myconn.yaml").write_text(
        'connection_string: "Provider=MSOLAP;Data Source=X"\ndescription: "Test"'
    )
    (connections_dir / "myconn_overview.md").write_text("# Overview\nCompact")
    (connections_dir / "myconn.md").write_text("# Full Context\nAll the details here")

    payload = json.loads(get_connection_context("myconn", str(connections_dir), detail="full"))

    assert payload["detail_level"] == "full"
    assert "All the details here" in payload["context_markdown"]
    assert "NOTE" not in payload


def test_get_connection_context_falls_back_to_full(tmp_path) -> None:
    """When no overview exists, get_connection_context falls back to full context."""
    connections_dir = tmp_path / "conns"
    connections_dir.mkdir()
    (connections_dir / "myconn.yaml").write_text(
        'connection_string: "Provider=MSOLAP;Data Source=X"\ndescription: "Test"'
    )
    (connections_dir / "myconn.md").write_text("# Full Context\nOnly this exists")

    payload = json.loads(get_connection_context("myconn", str(connections_dir)))

    assert payload["has_overview"] is False
    assert payload["has_full_context"] is True
    assert "Only this exists" in payload["context_markdown"]


def test_search_connection_context(tmp_path) -> None:
    """search_connection_context finds matching lines."""
    connections_dir = tmp_path / "conns"
    connections_dir.mkdir()
    (connections_dir / "myconn.yaml").write_text(
        'connection_string: "Provider=MSOLAP;Data Source=X"\ndescription: "Test"'
    )
    (connections_dir / "myconn.md").write_text(
        "# Tables\n## Sales\nRevenue column\n## Products\nCategory column\n## Calendar\nFiscal Month"
    )

    payload = json.loads(search_connection_context("myconn", "Revenue", str(connections_dir)))

    assert payload["match_count"] >= 1
    assert any("Revenue" in m["match_line"] for m in payload["matches"])


def test_search_connection_context_no_context(tmp_path) -> None:
    """search_connection_context handles missing context gracefully."""
    connections_dir = tmp_path / "conns"
    connections_dir.mkdir()
    (connections_dir / "myconn.yaml").write_text(
        'connection_string: "Provider=MSOLAP;Data Source=X"\ndescription: "Test"'
    )

    payload = json.loads(search_connection_context("myconn", "anything", str(connections_dir)))

    assert payload["match_count"] == 0
    assert "No context markdown" in payload["message"]


def test_list_connections_shows_overview_status(tmp_path) -> None:
    """list_connections shows has_overview and has_full_context."""
    connections_dir = tmp_path / "conns"
    connections_dir.mkdir()
    (connections_dir / "myconn.yaml").write_text(
        'connection_string: "Provider=MSOLAP;Data Source=X"\ndescription: "Test"'
    )
    (connections_dir / "myconn_overview.md").write_text("# Overview")

    payload = json.loads(list_connections(str(connections_dir)))

    conn = payload["connections"][0]
    assert conn["has_overview"] is True
    assert conn["has_full_context"] is False

