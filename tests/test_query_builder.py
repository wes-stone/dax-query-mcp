from __future__ import annotations

import json
from pathlib import Path

from dax_query_mcp.query_builder import (
    build_query_builder_dax,
    load_query_builder_artifacts,
    query_builder_from_dict,
    query_builder_to_dax_studio_payload,
    query_builder_schema_payload,
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


def test_query_builder_schema_payload_contains_copyable_example() -> None:
    schema = query_builder_schema_payload(connection_name="example_connection")

    assert schema["example_payload"]["connection_name"] == "example_connection"
    assert "measure" in schema["notes"][2].lower()
    assert '"connection_name": "example_connection"' in schema["example_json"]


def test_query_builder_exports_dax_studio_compatible_fields() -> None:
    definition = query_builder_from_dict(
        {
            "name": "ahr_style_query",
            "connection_name": "ahr_connection",
            "columns": ["'Calendar'[Fiscal Month]"],
            "measures": [
                {
                    "caption": "Avg Daily Azure Consumed Revenue",
                    "expression": "[Avg Daily Azure Consumed Revenue]",
                }
            ],
            "order_by": [{"expression": "'Calendar'[Fiscal Month]", "direction": "ASC"}],
        }
    )

    payload = query_builder_to_dax_studio_payload(definition)

    assert payload["AutoGenerate"] is False
    assert len(payload["Columns"]) == 2
    assert payload["Columns"][0]["TabularObject"]["TableName"] == "Calendar"
    assert payload["Columns"][0]["SortDirection"] == "ASC"
    assert payload["Columns"][1]["IsOverriden"] is False
    assert payload["Columns"][1]["IsModelItem"] is True
    assert payload["Filters"]["Items"] == []


def test_save_query_builder_artifacts_persists_dax_studio_fields(tmp_path: Path) -> None:
    definition = query_builder_from_dict(
        {
            "name": "ahr_style_query",
            "connection_name": "ahr_connection",
            "columns": ["'Calendar'[Fiscal Month]"],
            "measures": [{"caption": "Revenue", "expression": "[Avg Daily Azure Consumed Revenue]"}],
        }
    )

    result = save_query_builder_artifacts(definition, tmp_path)
    payload = json.loads(Path(result["query_builder_path"]).read_text(encoding="utf-8"))

    assert "Columns" in payload
    assert "Filters" in payload
    assert payload["Filters"]["Items"] == []
