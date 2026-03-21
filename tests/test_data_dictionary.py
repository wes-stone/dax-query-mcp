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
    TableDef,
    load_data_dictionary,
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

    def test_data_dictionary_defaults(self):
        dd = DataDictionary()
        assert dd.version == "1.0"
        assert dd.tables == []
        assert dd.measures == []
        assert dd.filters == []


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

    def test_sample_file_round_trips(self, tmp_path: Path):
        dd = load_data_dictionary(self.SAMPLE_PATH)
        out = tmp_path / "out.yaml"
        save_data_dictionary(dd, out)
        reloaded = load_data_dictionary(out)
        assert reloaded == dd
