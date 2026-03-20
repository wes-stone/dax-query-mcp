import json

import pandas as pd

from dax_query_mcp.mcp_server import get_connection_context, list_connections, summarize_dataframe, summarize_rowset


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
    assert summary["preview"] == [{"When": "2026-03-01T00:00:00.000", "Value": 1}]
    assert "| When | Value |" in summary["markdown_table"]


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

