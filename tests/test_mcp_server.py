import json

import pandas as pd

from dax_query_mcp.mcp_server import (
    get_connection_context,
    get_query_builder_schema,
    inspect_model_metadata,
    list_connections,
    run_connection_query,
    run_connection_query_markdown,
    save_query_builder,
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
    except ValueError as exc:
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

