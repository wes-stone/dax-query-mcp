"""Tests for data_dictionary module – models, YAML round-trip, and edge cases."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml

from dax_query_mcp.data_dictionary import (
    ColumnDef,
    DataDictionary,
    FilterDef,
    MeasureDef,
    RelationshipDef,
    TableDef,
    diff_data_dictionaries,
    find_data_dictionary,
    load_data_dictionary,
    merge_data_dictionaries,
    review_data_dictionary_update,
    save_data_dictionary,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def sample_dict() -> DataDictionary:
    """A minimal but complete DataDictionary for testing."""
    return DataDictionary(
        version="1.0",
        tables=[
            TableDef(
                name="Sales",
                description="Fact table",
                columns=[
                    ColumnDef(
                        name="Amount",
                        data_type="decimal",
                        description="Transaction amount in USD",
                        sample_values=["100.00", "250.50"],
                    ),
                    ColumnDef(name="Quantity", data_type="integer"),
                ],
            ),
        ],
        measures=[
            MeasureDef(
                name="Total Sales",
                expression="SUM(Sales[Amount])",
                description="Sum of all sales",
                format_string="$#,##0.00",
            ),
        ],
        filters=[
            FilterDef(
                name="Year Filter",
                column="Calendar[Year]",
                description="Filter by year",
                suggested_values=["2024", "2025"],
            ),
        ],
        relationships=[
            RelationshipDef(
                from_table="Sales",
                from_column="ProductKey",
                to_table="Products",
                to_column="ProductKey",
                description="Sales rows join to product attributes.",
            ),
        ],
    )


# ── Model construction tests ─────────────────────────────────────────────────


class TestModels:
    def test_column_defaults(self):
        col = ColumnDef(name="ID")
        assert col.data_type == "string"
        assert col.description == ""
        assert col.sample_values == []

    def test_table_with_columns(self):
        tbl = TableDef(
            name="T",
            columns=[ColumnDef(name="A"), ColumnDef(name="B", data_type="integer")],
        )
        assert len(tbl.columns) == 2
        assert tbl.columns[1].data_type == "integer"

    def test_measure_required_fields(self):
        m = MeasureDef(name="M", expression="COUNT(T[ID])")
        assert m.format_string == ""

    def test_filter_defaults(self):
        f = FilterDef(name="F", column="T[Col]")
        assert f.suggested_values == []

    def test_relationship_defaults(self):
        r = RelationshipDef(
            from_table="Sales",
            from_column="ProductKey",
            to_table="Products",
            to_column="ProductKey",
        )
        assert r.cardinality == "many-to-one"
        assert r.cross_filter_direction == "single"
        assert r.is_active is True
        assert r.source == "curated"
        assert r.confidence == "high"

    def test_data_dictionary_defaults(self):
        dd = DataDictionary()
        assert dd.version == "1.0"
        assert dd.tables == []
        assert dd.measures == []
        assert dd.filters == []
        assert dd.relationships == []


# ── YAML round-trip tests ────────────────────────────────────────────────────


class TestYAMLRoundTrip:
    def test_save_and_load(self, sample_dict: DataDictionary, tmp_path: Path):
        path = tmp_path / "test.yaml"
        save_data_dictionary(sample_dict, path)
        loaded = load_data_dictionary(path)
        assert loaded == sample_dict

    def test_round_trip_preserves_all_fields(
        self, sample_dict: DataDictionary, tmp_path: Path
    ):
        path = tmp_path / "rt.yaml"
        save_data_dictionary(sample_dict, path)
        loaded = load_data_dictionary(path)

        assert loaded.version == "1.0"
        assert loaded.tables[0].name == "Sales"
        assert loaded.tables[0].columns[0].sample_values == ["100.00", "250.50"]
        assert loaded.measures[0].expression == "SUM(Sales[Amount])"
        assert loaded.filters[0].suggested_values == ["2024", "2025"]
        assert loaded.relationships[0].from_table == "Sales"
        assert loaded.relationships[0].to_table == "Products"

    def test_empty_dictionary_round_trip(self, tmp_path: Path):
        dd = DataDictionary()
        path = tmp_path / "empty.yaml"
        save_data_dictionary(dd, path)
        loaded = load_data_dictionary(path)
        assert loaded == dd

    def test_load_from_raw_yaml(self, tmp_path: Path):
        raw = textwrap.dedent("""\
            version: "1.0"
            tables:
              - name: Products
                description: Product dimension
                columns:
                  - name: Price
                    data_type: decimal
                    description: Unit price
            measures:
              - name: Total Sales
                expression: SUM(Sales[Amount])
                description: Sum of all sales
        """)
        path = tmp_path / "raw.yaml"
        path.write_text(raw, encoding="utf-8")
        dd = load_data_dictionary(path)
        assert dd.tables[0].name == "Products"
        assert dd.measures[0].name == "Total Sales"
        assert dd.filters == []
        assert dd.relationships == []

    def test_saved_yaml_is_valid_yaml(
        self, sample_dict: DataDictionary, tmp_path: Path
    ):
        path = tmp_path / "valid.yaml"
        save_data_dictionary(sample_dict, path)
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(raw, dict)
        assert "tables" in raw


# ── Sample file test ──────────────────────────────────────────────────────────


class TestSampleFile:
    SAMPLE_PATH = (
        Path(__file__).resolve().parent.parent
        / "Connections"
        / "mock_contoso.data_dictionary.yaml"
    )

    def test_sample_file_loads(self):
        dd = load_data_dictionary(self.SAMPLE_PATH)
        assert dd.version == "1.0"
        assert len(dd.tables) == 3
        assert len(dd.measures) == 5
        assert len(dd.filters) == 3
        assert len(dd.relationships) == 2

    def test_sample_file_round_trips(self, tmp_path: Path):
        dd = load_data_dictionary(self.SAMPLE_PATH)
        out = tmp_path / "out.yaml"
        save_data_dictionary(dd, out)
        reloaded = load_data_dictionary(out)
        assert reloaded == dd


# ── find_data_dictionary tests ────────────────────────────────────────────────


class TestFindDataDictionary:
    CONNECTIONS_DIR = Path(__file__).resolve().parent.parent / "Connections"

    def test_finds_existing_file(self):
        dd = find_data_dictionary("mock_contoso", str(self.CONNECTIONS_DIR))
        assert dd is not None
        assert isinstance(dd, DataDictionary)
        assert dd.version == "1.0"
        assert len(dd.tables) == 3
        assert len(dd.relationships) == 2

    def test_returns_none_for_missing_file(self):
        dd = find_data_dictionary("nonexistent_connection", str(self.CONNECTIONS_DIR))
        assert dd is None

    def test_returns_none_for_missing_dir(self, tmp_path: Path):
        dd = find_data_dictionary("anything", str(tmp_path / "does_not_exist"))
        assert dd is None


# ── Lifecycle helper tests ────────────────────────────────────────────────────


class TestLifecycleHelpers:
    def test_diff_reports_entity_changes(self) -> None:
        base = DataDictionary(tables=[TableDef(name="Sales", columns=[ColumnDef(name="Amount")])])
        candidate = DataDictionary(
            tables=[
                TableDef(name="Sales", columns=[ColumnDef(name="Amount"), ColumnDef(name="Date")]),
                TableDef(name="Products"),
            ],
            measures=[MeasureDef(name="Revenue", expression="SUM(Sales[Amount])")],
        )

        diff = diff_data_dictionaries(base, candidate)

        assert diff["tables"]["added"] == ["Products"]
        assert diff["columns"]["Sales"]["added"] == ["Date"]
        assert diff["measures"]["added"] == ["Revenue"]

    def test_merge_preserves_curated_descriptions_and_notes(self) -> None:
        generated = DataDictionary(
            tables=[
                TableDef(
                    name="Sales",
                    description="Generated sales",
                    columns=[ColumnDef(name="Amount", data_type="decimal")],
                )
            ],
            measures=[MeasureDef(name="Revenue", expression="SUM(Sales[Amount])")],
            relationships=[
                RelationshipDef(
                    from_table="Sales",
                    from_column="ProductKey",
                    to_table="Products",
                    to_column="ProductKey",
                    source="mdschema-inferred",
                    confidence="medium",
                )
            ],
        )
        curated = DataDictionary(
            tables=[
                TableDef(
                    name="Sales",
                    description="Curated sales fact",
                    columns=[
                        ColumnDef(
                            name="Amount",
                            data_type="currency",
                            description="Booked amount",
                            sample_values=["100"],
                        )
                    ],
                )
            ],
            measures=[
                MeasureDef(
                    name="Revenue",
                    expression="SUM(Sales[Amount])",
                    description="Curated revenue definition",
                    format_string="$#,##0",
                )
            ],
            relationships=[
                RelationshipDef(
                    from_table="Sales",
                    from_column="ProductKey",
                    to_table="Products",
                    to_column="ProductKey",
                    description="Curated product rollup",
                    source="curated",
                    confidence="high",
                )
            ],
        )

        merged = merge_data_dictionaries(generated, curated)

        assert merged.tables[0].description == "Curated sales fact"
        assert merged.tables[0].columns[0].data_type == "decimal"
        assert merged.tables[0].columns[0].description == "Booked amount"
        assert merged.tables[0].columns[0].sample_values == ["100"]
        assert merged.measures[0].description == "Curated revenue definition"
        assert merged.measures[0].format_string == "$#,##0"
        assert merged.relationships[0].description == "Curated product rollup"
        assert merged.relationships[0].source == "curated"

    def test_review_update_returns_diff_and_merged_payload(self) -> None:
        curated = DataDictionary(tables=[TableDef(name="Sales")])
        generated = DataDictionary(tables=[TableDef(name="Sales"), TableDef(name="Calendar")])

        review = review_data_dictionary_update(curated, generated)

        assert review["diff"]["tables"]["added"] == ["Calendar"]
        assert review["summary"]["tables"] == 2
        assert review["merged"]["tables"][1]["name"] == "Calendar"
