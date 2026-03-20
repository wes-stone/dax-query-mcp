from __future__ import annotations

from pathlib import Path

from dax_query_mcp.query_builder import (
    build_query_builder_dax,
    load_query_builder_artifacts,
    query_builder_from_dict,
    save_query_builder_artifacts,
)


def test_save_and_load_query_builder_artifacts(tmp_path: Path) -> None:
    definition = query_builder_from_dict(
        {
            "name": "monthly_revenue",
            "connection_name": "example_connection",
            "description": "Monthly revenue",
            "columns": ["'Calendar'[Fiscal Month]"],
            "measures": [{"caption": "Revenue", "expression": "[Total Revenue]"}],
            "filters": [
                {
                    "expression": "'Calendar'[Fiscal Year]",
                    "operator": "=",
                    "value": 2026,
                }
            ],
            "order_by": [{"expression": "'Calendar'[Fiscal Month]", "direction": "ASC"}],
        }
    )

    result = save_query_builder_artifacts(definition, tmp_path)
    loaded_definition, dax_query = load_query_builder_artifacts("monthly_revenue", tmp_path)

    assert Path(result["dax_path"]).exists()
    assert Path(result["query_builder_path"]).exists()
    assert result["dax_studio_open_path"].endswith("monthly_revenue.dax")
    assert "DAX Studio" in result["dax_studio_note"]
    assert loaded_definition.connection_name == "example_connection"
    assert loaded_definition.measures[0].caption == "Revenue"
    assert "SUMMARIZECOLUMNS" in dax_query
    assert '"Revenue", [Total Revenue]' in dax_query


def test_build_query_builder_dax_supports_filter_operators() -> None:
    definition = query_builder_from_dict(
        {
            "name": "filtered_query",
            "connection_name": "example_connection",
            "columns": ["'Account Information'[TPID]"],
            "filters": [
                {
                    "expression": "'Account Information'[ATU]",
                    "operator": "in",
                    "values": ["Enterprise", "SMB"],
                }
            ],
        }
    )

    dax_query = build_query_builder_dax(definition)

    assert "KEEPFILTERS" in dax_query
    assert 'IN { "Enterprise", "SMB" }' in dax_query
