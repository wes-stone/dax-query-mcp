from __future__ import annotations

from dax_query_mcp.query_pack import QueryOutputs, QueryParameter
from dax_query_mcp.validated_query_library import (
    ValidatedQueryEntry,
    load_validated_query_library,
    render_validated_query,
    save_validated_query_entry,
    search_validated_query_entries,
    validation_record_from_result,
)


def test_validated_query_library_round_trip_writes_metadata_and_dax(tmp_path) -> None:
    entry = ValidatedQueryEntry(
        id="monthly_revenue",
        display_name="Monthly Revenue",
        connection_name="sales",
        file="monthly_revenue.dax",
        description="Revenue by month",
        tags=["revenue", "monthly"],
        parameters={"fiscal_year": QueryParameter(type="text", required=True)},
        sample_parameters={"fiscal_year": "FY26"},
        outputs=QueryOutputs(table_name="MonthlyRevenue"),
        query_text='EVALUATE ROW("FY", {{fiscal_year}})',
    )

    result = save_validated_query_entry(entry, tmp_path)
    loaded = load_validated_query_library(tmp_path, "sales", include_query_text=True)

    assert result["query_id"] == "monthly_revenue"
    assert (tmp_path / "sales.validated_queries" / "monthly_revenue.yaml").exists()
    assert (tmp_path / "sales.validated_queries" / "monthly_revenue.dax").read_text(encoding="utf-8") == (
        'EVALUATE ROW("FY", {{fiscal_year}})'
    )
    assert loaded[0].display_name == "Monthly Revenue"
    assert loaded[0].parameters["fiscal_year"].required is True
    assert loaded[0].sample_parameters == {"fiscal_year": "FY26"}
    assert loaded[0].outputs.table_name == "MonthlyRevenue"
    assert loaded[0].validation.status == "draft"
    assert render_validated_query(loaded[0]) == 'EVALUATE ROW("FY", "FY26")'


def test_search_validated_queries_matches_tags_text_and_dax(tmp_path) -> None:
    entry = ValidatedQueryEntry(
        id="arr_by_month",
        display_name="ARR by Month",
        connection_name="sales",
        file="arr_by_month.dax",
        description="Recurring revenue pattern",
        tags=["arr", "monthly"],
        query_text='EVALUATE ROW("ARR", 1)',
    )
    save_validated_query_entry(entry, tmp_path)
    entries = load_validated_query_library(tmp_path, "sales", include_query_text=True)

    matches = search_validated_query_entries(entries, "ARR", tags=["monthly"])

    assert len(matches) == 1
    assert matches[0]["id"] == "arr_by_month"
    assert matches[0]["query"] == 'EVALUATE ROW("ARR", 1)'


def test_validated_query_status_becomes_stale_when_dax_changes(tmp_path) -> None:
    query = 'EVALUATE ROW("Ping", 1)'
    validation = validation_record_from_result(
        rendered_query=query,
        row_count=1,
        columns=["Ping"],
        max_rows=1,
        transport="mock",
    )
    entry = ValidatedQueryEntry(
        id="ping",
        connection_name="sales",
        file="ping.dax",
        query_text=query,
        validation=validation,
    )
    save_validated_query_entry(entry, tmp_path)

    (tmp_path / "sales.validated_queries" / "ping.dax").write_text('EVALUATE ROW("Ping", 2)', encoding="utf-8")
    loaded = load_validated_query_library(tmp_path, "sales", include_query_text=True)

    assert loaded[0].validation.status == "stale"
    assert "no longer matches" in loaded[0].validation.error
