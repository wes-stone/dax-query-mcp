"""Tests for validate_dax_query() guardrails in mcp_server.py.

Ensures admin/DDL queries are rejected while safe analytical queries
and $SYSTEM.MDSCHEMA_* metadata queries are allowed.
"""

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from dax_query_mcp.mcp_server import validate_dax_query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def assert_rejected(query: str) -> None:
    """Assert that validate_dax_query raises ToolError for the query."""
    with pytest.raises(ToolError):
        validate_dax_query(query)


def assert_allowed(query: str) -> None:
    """Assert that validate_dax_query does NOT raise for the query."""
    validate_dax_query(query)  # should not raise


# ---------------------------------------------------------------------------
# REJECTED patterns
# ---------------------------------------------------------------------------

class TestInfoDmvRejected:
    """INFO.*() DMV function variants must be blocked."""

    def test_info_columns(self):
        assert_rejected("SELECT * FROM INFO.COLUMNS()")

    def test_info_tables(self):
        assert_rejected("SELECT * FROM INFO.TABLES()")

    def test_info_measures(self):
        assert_rejected("SELECT * FROM INFO.MEASURES()")

    def test_info_relationships(self):
        assert_rejected("SELECT * FROM INFO.RELATIONSHIPS()")

    def test_info_with_extra_spaces(self):
        assert_rejected("SELECT * FROM INFO  .COLUMNS()")

    def test_info_lowercase(self):
        assert_rejected("select * from info.columns()")

    def test_info_mixed_case(self):
        assert_rejected("Select * From Info.Tables()")


class TestSystemDiscoverRejected:
    """$SYSTEM.DISCOVER_* DMV rowsets must be blocked."""

    def test_discover_sessions(self):
        assert_rejected("SELECT * FROM $SYSTEM.DISCOVER_SESSIONS")

    def test_discover_connections(self):
        assert_rejected("SELECT * FROM $SYSTEM.DISCOVER_CONNECTIONS")

    def test_discover_commands(self):
        assert_rejected("SELECT * FROM $SYSTEM.DISCOVER_COMMANDS")

    def test_discover_object_activity(self):
        assert_rejected("SELECT * FROM $SYSTEM.DISCOVER_OBJECT_ACTIVITY")

    def test_discover_lowercase(self):
        assert_rejected("select * from $system.discover_sessions")

    def test_discover_mixed_case(self):
        assert_rejected("SELECT * FROM $System.Discover_Sessions")


class TestDbccRejected:
    """DBCC commands must be blocked."""

    def test_dbcc_freeproccache(self):
        assert_rejected("DBCC FREEPROCCACHE")

    def test_dbcc_dropcleanbuffers(self):
        assert_rejected("DBCC DROPCLEANBUFFERS")

    def test_dbcc_lowercase(self):
        assert_rejected("dbcc freeproccache")

    def test_dbcc_mixed_case(self):
        assert_rejected("Dbcc FreeProccache")


class TestDdlRejected:
    """ALTER, CREATE, DELETE, DROP statements must be blocked."""

    def test_alter(self):
        assert_rejected("ALTER TABLE MyTable ADD COLUMN x INT")

    def test_create(self):
        assert_rejected("CREATE TABLE MyTable (x INT)")

    def test_delete(self):
        assert_rejected("DELETE FROM MyTable WHERE x = 1")

    def test_drop(self):
        assert_rejected("DROP TABLE MyTable")

    def test_alter_lowercase(self):
        assert_rejected("alter table MyTable add column x int")

    def test_create_lowercase(self):
        assert_rejected("create table MyTable (x INT)")

    def test_delete_lowercase(self):
        assert_rejected("delete from MyTable where x = 1")

    def test_drop_lowercase(self):
        assert_rejected("drop table MyTable")

    def test_alter_mixed_case(self):
        assert_rejected("Alter Table MyTable Add Column x INT")

    def test_create_mixed_case(self):
        assert_rejected("Create Table MyTable (x INT)")


# ---------------------------------------------------------------------------
# ALLOWED patterns
# ---------------------------------------------------------------------------

class TestMdschemaAllowed:
    """$SYSTEM.MDSCHEMA_* queries used by inspect_connection must pass."""

    def test_mdschema_measures(self):
        assert_allowed("SELECT * FROM $SYSTEM.MDSCHEMA_MEASURES")

    def test_mdschema_dimensions(self):
        assert_allowed("SELECT * FROM $SYSTEM.MDSCHEMA_DIMENSIONS")

    def test_mdschema_hierarchies(self):
        assert_allowed("SELECT * FROM $SYSTEM.MDSCHEMA_HIERARCHIES")

    def test_mdschema_lowercase(self):
        assert_allowed("select * from $system.mdschema_measures")

    def test_mdschema_mixed_case(self):
        assert_allowed("SELECT * FROM $System.MDSCHEMA_Measures")


class TestNormalQueriesAllowed:
    """Standard DAX analytical queries must pass."""

    def test_evaluate_simple(self):
        assert_allowed("EVALUATE Sales")

    def test_evaluate_summarize(self):
        assert_allowed(
            "EVALUATE SUMMARIZE(Sales, Sales[Region], \"Total\", SUM(Sales[Amount]))"
        )

    def test_evaluate_filter(self):
        assert_allowed("EVALUATE FILTER(Sales, Sales[Amount] > 100)")

    def test_evaluate_calculatetable(self):
        assert_allowed(
            "EVALUATE CALCULATETABLE(Sales, Sales[Year] = 2024)"
        )

    def test_evaluate_row(self):
        assert_allowed(
            'EVALUATE ROW("Total", SUM(Sales[Amount]))'
        )

    def test_evaluate_addcolumns(self):
        assert_allowed(
            "EVALUATE ADDCOLUMNS(Sales, \"Doubled\", Sales[Amount] * 2)"
        )

    def test_define_measure(self):
        assert_allowed(
            "DEFINE MEASURE Sales[Total] = SUM(Sales[Amount]) "
            "EVALUATE Sales"
        )

    def test_evaluate_topn(self):
        assert_allowed("EVALUATE TOPN(10, Sales, Sales[Amount], DESC)")

    def test_evaluate_values(self):
        assert_allowed("EVALUATE VALUES(Sales[Region])")


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Whitespace, leading/trailing spaces, embedded keywords, etc."""

    def test_leading_whitespace_rejected(self):
        assert_rejected("   INFO.COLUMNS()")

    def test_trailing_whitespace_rejected(self):
        assert_rejected("DBCC FREEPROCCACHE   ")

    def test_leading_newlines_rejected(self):
        assert_rejected("\n\nDROP TABLE MyTable")

    def test_leading_whitespace_allowed(self):
        assert_allowed("   EVALUATE Sales")

    def test_trailing_whitespace_allowed(self):
        assert_allowed("EVALUATE Sales   ")

    def test_multiline_query_with_blocked_keyword(self):
        assert_rejected(
            "EVALUATE Sales\n"
            "-- some comment\n"
            "DELETE FROM Sales WHERE x = 1"
        )

    def test_keyword_inside_table_name_alter(self):
        # "ALTER" as a standalone word in a column/table name context
        # The regex uses \b word boundary, so 'AlteredState' should not match
        assert_allowed("EVALUATE FILTER(AlteredState, AlteredState[Value] > 0)")

    def test_keyword_inside_table_name_create(self):
        assert_allowed("EVALUATE FILTER(CreatedItems, CreatedItems[Qty] > 0)")

    def test_keyword_inside_table_name_drop(self):
        assert_allowed("EVALUATE FILTER(DropShipments, DropShipments[Qty] > 0)")

    def test_keyword_inside_column_name_delete(self):
        assert_allowed(
            "EVALUATE FILTER(Orders, Orders[DeletedFlag] = FALSE)"
        )

    def test_empty_query_allowed(self):
        assert_allowed("")

    def test_whitespace_only_allowed(self):
        assert_allowed("   ")

    def test_comment_only_allowed(self):
        assert_allowed("-- just a comment")

    def test_mdschema_takes_precedence_over_system(self):
        # Even though $SYSTEM is present, MDSCHEMA_ prefix makes it safe
        assert_allowed("SELECT * FROM $SYSTEM.MDSCHEMA_MEASUREGROUPS")

    def test_tabs_in_info_pattern(self):
        assert_rejected("SELECT * FROM INFO\t.COLUMNS()")
