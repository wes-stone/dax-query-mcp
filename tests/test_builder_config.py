from __future__ import annotations

from pathlib import Path

from dax_query_mcp.config import load_queries


def test_load_queries_supports_saved_query_builder_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "example_connection.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/SampleWorkspace?readonly;
  Initial Catalog=SampleSemanticModel
description: "Example connection"
command_timeout_seconds: 120
""".strip(),
        encoding="utf-8",
    )

    queries_dir = tmp_path / "queries"
    queries_dir.mkdir()
    (queries_dir / "monthly_revenue.dax").write_text(
        "EVALUATE\nSUMMARIZECOLUMNS('Calendar'[Fiscal Month], \"Revenue\", [Total Revenue])\n",
        encoding="utf-8",
    )
    (queries_dir / "monthly_revenue.dax.queryBuilder").write_text(
        """
{
  "name": "monthly_revenue",
  "connection_name": "example_connection",
  "description": "Monthly revenue",
  "output_filename": "monthly_revenue_export",
  "columns": ["'Calendar'[Fiscal Month]"],
  "measures": [{"caption": "Revenue", "expression": "[Total Revenue]"}]
}
""".strip(),
        encoding="utf-8",
    )

    queries = load_queries(queries_dir)

    assert list(queries.keys()) == ["monthly_revenue"]
    assert queries["monthly_revenue"].description == "Monthly revenue"
    assert queries["monthly_revenue"].output_filename == "monthly_revenue_export"
    assert queries["monthly_revenue"].command_timeout_seconds == 120
