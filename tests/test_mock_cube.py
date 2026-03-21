"""Tests for the mock cube simulation."""

import pandas as pd

from dax_query_mcp.executor import dax_to_pandas
from dax_query_mcp.mock_cube import (
    ContosoCube,
    MockCommand,
    MockConnection,
    MockRecordset,
    create_mock_dispatcher,
    get_mock_cube_name,
    is_mock_connection,
)


class TestMockRecordset:
    def test_getrows_transposes_data(self) -> None:
        rs = MockRecordset(
            fields=["A", "B"],
            rows=[(1, 2), (3, 4), (5, 6)],
        )
        columns = rs.GetRows()
        assert columns == [(1, 3, 5), (2, 4, 6)]

    def test_getrows_with_max_rows(self) -> None:
        rs = MockRecordset(
            fields=["A"],
            rows=[(1,), (2,), (3,)],
        )
        columns = rs.GetRows(max_rows=2)
        assert columns == [(1, 2)]

    def test_close_sets_flag(self) -> None:
        rs = MockRecordset(fields=["A"], rows=[])
        assert rs.closed is False
        rs.Close()
        assert rs.closed is True


class TestMockConnection:
    def test_open_stores_connection_string(self) -> None:
        conn = MockConnection()
        conn.Open("MOCK://contoso")
        assert conn.opened_with == "MOCK://contoso"

    def test_close_sets_flag(self) -> None:
        conn = MockConnection()
        assert conn.closed is False
        conn.Close()
        assert conn.closed is True


class TestContosoCube:
    def test_cube_has_products(self) -> None:
        cube = ContosoCube()
        assert len(cube.products) == 5
        assert cube.products[0]["ProductName"] == "Mountain Bike"

    def test_cube_has_calendar(self) -> None:
        cube = ContosoCube()
        assert len(cube.calendar) == 12 * 28  # 12 months * 28 days

    def test_cube_has_sales(self) -> None:
        cube = ContosoCube()
        assert len(cube.sales) == 100

    def test_mdschema_cubes(self) -> None:
        cube = ContosoCube()
        rs = cube.execute_query("SELECT * FROM $SYSTEM.MDSCHEMA_CUBES")
        assert rs.fields == ["CUBE_NAME", "DESCRIPTION", "CUBE_TYPE"]
        assert len(rs.rows) == 1
        assert rs.rows[0][0] == "Contoso Sales"

    def test_mdschema_dimensions(self) -> None:
        cube = ContosoCube()
        rs = cube.execute_query("SELECT * FROM $SYSTEM.MDSCHEMA_DIMENSIONS")
        assert "DIMENSION_NAME" in rs.fields
        assert len(rs.rows) == 3  # Sales, Products, Calendar

    def test_mdschema_measures(self) -> None:
        cube = ContosoCube()
        rs = cube.execute_query("SELECT * FROM $SYSTEM.MDSCHEMA_MEASURES")
        assert "MEASURE_NAME" in rs.fields
        assert len(rs.rows) == 5

    def test_evaluate_sales(self) -> None:
        cube = ContosoCube()
        rs = cube.execute_query("EVALUATE Sales")
        assert rs.fields == ["SalesKey", "ProductKey", "DateKey", "Quantity", "Amount"]
        assert len(rs.rows) == 100

    def test_evaluate_products(self) -> None:
        cube = ContosoCube()
        rs = cube.execute_query("EVALUATE Products")
        assert rs.fields == ["ProductKey", "ProductName", "Category", "Price"]
        assert len(rs.rows) == 5

    def test_evaluate_calendar(self) -> None:
        cube = ContosoCube()
        rs = cube.execute_query("EVALUATE Calendar")
        assert "Date" in rs.fields
        assert len(rs.rows) == 12 * 28

    def test_summarize_returns_aggregates(self) -> None:
        cube = ContosoCube()
        rs = cube.execute_query("EVALUATE SUMMARIZE(Sales, [Total Sales])")
        assert "Total Sales" in rs.fields
        assert len(rs.rows) == 1


class TestMockDispatcher:
    def test_dispatcher_returns_connection_and_command(self) -> None:
        dispatcher = create_mock_dispatcher()
        conn = dispatcher("ADODB.Connection")
        cmd = dispatcher("ADODB.Command")
        assert isinstance(conn, MockConnection)
        assert isinstance(cmd, MockCommand)

    def test_dispatcher_raises_on_unknown_object(self) -> None:
        dispatcher = create_mock_dispatcher()
        try:
            dispatcher("ADODB.Unknown")
            assert False, "Should raise ValueError"
        except ValueError as exc:
            assert "Unknown ADODB object" in str(exc)


class TestIsMockConnection:
    def test_mock_connection_detected(self) -> None:
        assert is_mock_connection("MOCK://contoso") is True
        assert is_mock_connection("mock://Contoso Sales") is True
        assert is_mock_connection("  MOCK://test  ") is True

    def test_real_connection_not_detected(self) -> None:
        assert is_mock_connection("Provider=MSOLAP.8;Data Source=localhost") is False
        assert is_mock_connection("") is False


class TestGetMockCubeName:
    def test_extracts_cube_name(self) -> None:
        assert get_mock_cube_name("MOCK://contoso") == "contoso"
        assert get_mock_cube_name("MOCK://Contoso Sales") == "Contoso Sales"

    def test_returns_empty_for_real_connection(self) -> None:
        assert get_mock_cube_name("Provider=MSOLAP.8") == ""


class TestDaxToPandasWithMock:
    def test_mock_connection_returns_dataframe(self) -> None:
        df = dax_to_pandas("EVALUATE Products", "MOCK://contoso")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "ProductName" in df.columns

    def test_mock_mdschema_measures(self) -> None:
        df = dax_to_pandas("SELECT * FROM $SYSTEM.MDSCHEMA_MEASURES", "MOCK://contoso")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "MEASURE_NAME" in df.columns

    def test_mock_sales_with_max_rows(self) -> None:
        df = dax_to_pandas("EVALUATE Sales", "MOCK://contoso", max_rows=10)
        assert len(df) == 10
