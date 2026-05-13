from __future__ import annotations

from datetime import date

import pytest

from dax_query_mcp.query_pack import (
    QueryOutputs,
    QueryPack,
    QueryPackEntry,
    QueryParameter,
    load_query_pack,
    parse_connection_string,
    power_query_m_from_connection,
    read_query_text,
    render_dax_template,
    save_query_pack,
    describe_query_pack_markdown,
    validate_query_pack,
)


def test_query_pack_round_trip_writes_manifest_and_queries(tmp_path) -> None:
    pack = QueryPack(
        name="revenue-pack",
        description="Revenue exploration",
        queries=[
            QueryPackEntry(
                id="monthly_arr",
                display_name="Monthly ARR",
                connection_name="arr_connection",
                file="queries/monthly_arr.dax",
                description="ARR by month",
                tags=["arr", "monthly"],
                parameters={"fiscal_year": QueryParameter(type="text", default="FY26")},
                outputs=QueryOutputs(table_name="MonthlyARR"),
                query_text='EVALUATE ROW("ARR", 1)',
            )
        ],
    )

    result = save_query_pack(pack, tmp_path)
    loaded = load_query_pack(tmp_path)

    assert result["query_count"] == 1
    assert (tmp_path / "pack.yaml").exists()
    assert (tmp_path / "queries" / "monthly_arr.dax").read_text(encoding="utf-8") == 'EVALUATE ROW("ARR", 1)'
    assert loaded.name == "revenue-pack"
    assert loaded.queries[0].parameters["fiscal_year"].default == "FY26"
    assert loaded.queries[0].outputs.table_name == "MonthlyARR"


def test_describe_query_pack_markdown_includes_shareable_summary() -> None:
    pack = QueryPack(
        name="Revenue Pack",
        description="Revenue exploration",
        queries=[
            QueryPackEntry(
                id="monthly_arr",
                display_name="Monthly ARR",
                connection_name="sales",
                file="queries/monthly_arr.dax",
                description="ARR by month",
                tags=["arr", "monthly"],
                parameters={"fiscal_year": QueryParameter(type="text", default="FY26")},
                outputs=QueryOutputs(table_name="MonthlyARR"),
            )
        ],
    )

    markdown = describe_query_pack_markdown(pack, validation={"valid": True})

    assert "## Query Pack: Revenue Pack" in markdown
    assert "Revenue exploration" in markdown
    assert "| monthly_arr | Monthly ARR | sales | arr, monthly | fiscal_year | MonthlyARR |" in markdown
    assert "Validation: passed" in markdown


def test_query_pack_validation_rejects_duplicate_ids_and_admin_queries(tmp_path) -> None:
    pack = QueryPack(
        name="bad-pack",
        queries=[
            QueryPackEntry(id="dup", connection_name="sales", file="queries/a.dax", query_text="EVALUATE ROW(\"A\", 1)"),
            QueryPackEntry(id="dup", connection_name="sales", file="queries/b.dax", query_text="EVALUATE $SYSTEM.DISCOVER_SESSIONS"),
        ],
    )

    payload = validate_query_pack(pack, connection_names={"sales"})

    assert payload["valid"] is False
    assert any("Duplicate query id" in error for error in payload["errors"])
    assert any("blocked admin" in error for error in payload["errors"])


def test_query_pack_validation_checks_files_and_connections(tmp_path) -> None:
    pack = QueryPack(
        name="file-pack",
        queries=[
            QueryPackEntry(id="missing", connection_name="unknown", file="queries/missing.dax"),
        ],
    )

    payload = validate_query_pack(pack, pack_root=tmp_path, connection_names={"sales"})

    assert payload["valid"] is False
    assert any("unknown connection" in error for error in payload["errors"])
    assert any("file not found" in error for error in payload["errors"])


def test_query_pack_validation_checks_output_formats_and_parameter_defaults() -> None:
    pack = QueryPack(
        name="bad-defaults",
        queries=[
            QueryPackEntry(
                id="bad_output",
                connection_name="sales",
                file="queries/bad_output.dax",
                query_text='EVALUATE ROW("A", {{amount}})',
                parameters={"amount": QueryParameter(type="number", default="not-a-number")},
                outputs=QueryOutputs(default_format="parquet"),
            ),
        ],
    )

    payload = validate_query_pack(pack, connection_names={"sales"})

    assert payload["valid"] is False
    assert any("unsupported output format" in error for error in payload["errors"])
    assert any("invalid default" in error for error in payload["errors"])


def test_render_dax_template_escapes_supported_parameter_types() -> None:
    rendered = render_dax_template(
        """
EVALUATE
ROW(
    "Text", {{text_value}},
    "Number", {{number_value}},
    "Date", {{date_value}},
    "Flag", {{flag_value}},
    "List", CONCATENATEX({{list_value}}, [Value], ",")
)
""",
        {
            "text_value": QueryParameter(type="text"),
            "number_value": QueryParameter(type="number"),
            "date_value": QueryParameter(type="date"),
            "flag_value": QueryParameter(type="boolean"),
            "list_value": QueryParameter(type="list[text]"),
        },
        {
            "text_value": 'A "quoted" value',
            "number_value": "42.5",
            "date_value": date(2026, 1, 31),
            "flag_value": "yes",
            "list_value": ["FY25", "FY26"],
        },
    )

    assert '"A ""quoted"" value"' in rendered
    assert "42.5" in rendered
    assert "DATE(2026, 1, 31)" in rendered
    assert "TRUE()" in rendered
    assert '{"FY25", "FY26"}' in rendered


def test_render_dax_template_rejects_undeclared_parameter() -> None:
    with pytest.raises(ValueError, match="Undeclared"):
        render_dax_template("EVALUATE ROW(\"X\", {{missing}})", {})


def test_parse_connection_string_handles_quoted_semicolons() -> None:
    parsed = parse_connection_string(
        'Provider=MSOLAP.8;Data Source="powerbi://workspace;with-semicolon";Initial Catalog="My ""Model"""'
    )

    assert parsed["data source"] == "powerbi://workspace;with-semicolon"
    assert parsed["initial catalog"] == 'My "Model"'


def test_power_query_m_from_connection_uses_server_database_and_query() -> None:
    m_code = power_query_m_from_connection(
        "Provider=MSOLAP.8;Data Source=localhost:1234;Initial Catalog=Model",
        'EVALUATE ROW("Revenue", 42)',
    )

    assert 'AnalysisServices.Database(' in m_code
    assert '"localhost:1234"' in m_code
    assert '"Model"' in m_code
    assert 'EVALUATE ROW(""Revenue"", 42)' in m_code


def test_power_query_m_refuses_secret_connection_properties() -> None:
    with pytest.raises(ValueError, match="secret or impersonation"):
        power_query_m_from_connection(
            "Provider=MSOLAP.8;Data Source=localhost;Initial Catalog=Model;User ID=alice",
            'EVALUATE ROW("Revenue", 42)',
        )


def test_read_query_text_rejects_pack_escape(tmp_path) -> None:
    entry = QueryPackEntry(id="escape", connection_name="sales", file="../outside.dax")

    with pytest.raises(ValueError, match="inside the pack"):
        read_query_text(tmp_path, entry)
