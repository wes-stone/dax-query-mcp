"""Mock cube for simulation and testing.

Provides a fake ADODB dispatcher that returns canned recordsets for
specific queries. Use `MOCK://contoso` as connection string to route
queries through the mock instead of real COM/ADODB.

Sample cube: "Contoso Sales"
- 3 tables: Sales, Products, Calendar
- 5 measures: Total Sales, Total Quantity, Avg Price, Product Count, Day Count
- ~100 rows of fake transactional data
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

# ---------------------------------------------------------------------------
# Fake ADODB objects (extended from test_executor.py)
# ---------------------------------------------------------------------------


@dataclass
class MockField:
    Name: str


@dataclass
class MockRecordset:
    fields: list[str]
    rows: list[tuple[Any, ...]]
    closed: bool = False

    @property
    def Fields(self) -> list[MockField]:
        return [MockField(name) for name in self.fields]

    def GetRows(self, max_rows: int | None = None) -> list[tuple[Any, ...]]:
        data = self.rows if max_rows is None else self.rows[:max_rows]
        if not data:
            return []
        # Transpose: list of rows -> tuple of columns
        return [tuple(row[i] for row in data) for i in range(len(self.fields))]

    def Close(self) -> None:
        self.closed = True


@dataclass
class MockConnection:
    ConnectionTimeout: int | None = None
    CommandTimeout: int | None = None
    opened_with: str | None = None
    closed: bool = False

    def Open(self, connection_string: str) -> None:
        self.opened_with = connection_string

    def Close(self) -> None:
        self.closed = True


@dataclass
class MockCommand:
    ActiveConnection: MockConnection | None = None
    CommandText: str | None = None
    CommandTimeout: int | None = None
    _cube: "ContosoCube | None" = None

    def Execute(self) -> tuple[MockRecordset]:
        if self._cube is None:
            raise RuntimeError("MockCommand has no cube attached")
        recordset = self._cube.execute_query(self.CommandText or "")
        return (recordset,)


# ---------------------------------------------------------------------------
# Contoso Sales cube definition
# ---------------------------------------------------------------------------


@dataclass
class ContosoCube:
    """A small fake cube with Sales, Products, and Calendar tables."""

    name: str = "Contoso Sales"

    # Prebuilt data
    products: list[dict[str, Any]] = field(default_factory=list)
    calendar: list[dict[str, Any]] = field(default_factory=list)
    sales: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.products:
            self.products = self._build_products()
        if not self.calendar:
            self.calendar = self._build_calendar()
        if not self.sales:
            self.sales = self._build_sales()

    def _build_products(self) -> list[dict[str, Any]]:
        return [
            {"ProductKey": 1, "ProductName": "Mountain Bike", "Category": "Bikes", "Price": 1500.00},
            {"ProductKey": 2, "ProductName": "Road Bike", "Category": "Bikes", "Price": 1200.00},
            {"ProductKey": 3, "ProductName": "Helmet", "Category": "Accessories", "Price": 50.00},
            {"ProductKey": 4, "ProductName": "Gloves", "Category": "Accessories", "Price": 25.00},
            {"ProductKey": 5, "ProductName": "Water Bottle", "Category": "Accessories", "Price": 10.00},
        ]

    def _build_calendar(self) -> list[dict[str, Any]]:
        rows = []
        for month in range(1, 13):
            for day in range(1, 29):  # 28 days per month for simplicity
                d = date(2025, month, day)
                rows.append({
                    "DateKey": int(d.strftime("%Y%m%d")),
                    "Date": datetime(d.year, d.month, d.day),
                    "Month": d.strftime("%B"),
                    "MonthNum": month,
                    "Year": 2025,
                    "Weekday": d.strftime("%A"),
                })
        return rows

    def _build_sales(self) -> list[dict[str, Any]]:
        """Generate ~100 fake sales transactions."""
        import random
        random.seed(42)  # Deterministic for tests
        rows = []
        for i in range(100):
            product = random.choice(self.products)
            cal = random.choice(self.calendar)
            qty = random.randint(1, 5)
            rows.append({
                "SalesKey": i + 1,
                "ProductKey": product["ProductKey"],
                "DateKey": cal["DateKey"],
                "Quantity": qty,
                "Amount": round(product["Price"] * qty, 2),
            })
        return rows

    # -----------------------------------------------------------------------
    # Query execution
    # -----------------------------------------------------------------------

    def execute_query(self, query: str) -> MockRecordset:
        """Route a DAX/SQL query to the appropriate canned response."""
        upper = query.upper().strip()

        # MDSCHEMA rowsets
        if "MDSCHEMA_CUBES" in upper:
            return self._mdschema_cubes()
        if "MDSCHEMA_DIMENSIONS" in upper:
            return self._mdschema_dimensions()
        if "MDSCHEMA_HIERARCHIES" in upper:
            return self._mdschema_hierarchies()
        if "MDSCHEMA_LEVELS" in upper:
            return self._mdschema_levels()
        if "MDSCHEMA_MEASURES" in upper:
            return self._mdschema_measures()

        # Simple EVALUATE queries
        if upper.startswith("EVALUATE"):
            return self._evaluate(query)

        # Fallback: empty result
        return MockRecordset(fields=["Empty"], rows=[])

    # -----------------------------------------------------------------------
    # MDSCHEMA canned responses
    # -----------------------------------------------------------------------

    def _mdschema_cubes(self) -> MockRecordset:
        return MockRecordset(
            fields=["CUBE_NAME", "DESCRIPTION", "CUBE_TYPE"],
            rows=[
                ("Contoso Sales", "Sample cube for testing", "CUBE"),
            ],
        )

    def _mdschema_dimensions(self) -> MockRecordset:
        return MockRecordset(
            fields=["CUBE_NAME", "DIMENSION_NAME", "DIMENSION_UNIQUE_NAME", "DESCRIPTION"],
            rows=[
                ("Contoso Sales", "Sales", "[Sales]", "Fact table with transactions"),
                ("Contoso Sales", "Products", "[Products]", "Product dimension"),
                ("Contoso Sales", "Calendar", "[Calendar]", "Date dimension"),
            ],
        )

    def _mdschema_hierarchies(self) -> MockRecordset:
        return MockRecordset(
            fields=["CUBE_NAME", "DIMENSION_UNIQUE_NAME", "HIERARCHY_NAME", "DESCRIPTION"],
            rows=[
                ("Contoso Sales", "[Products]", "Category", "Product categories"),
                ("Contoso Sales", "[Calendar]", "Month", "Calendar months"),
                ("Contoso Sales", "[Calendar]", "Year", "Calendar years"),
            ],
        )

    def _mdschema_levels(self) -> MockRecordset:
        return MockRecordset(
            fields=["CUBE_NAME", "HIERARCHY_UNIQUE_NAME", "LEVEL_NAME", "DESCRIPTION"],
            rows=[
                ("Contoso Sales", "[Products].[Category]", "Category", "Product category level"),
                ("Contoso Sales", "[Calendar].[Month]", "Month", "Month level"),
                ("Contoso Sales", "[Calendar].[Year]", "Year", "Year level"),
            ],
        )

    def _mdschema_measures(self) -> MockRecordset:
        return MockRecordset(
            fields=["CUBE_NAME", "MEASURE_NAME", "MEASURE_UNIQUE_NAME", "DESCRIPTION"],
            rows=[
                ("Contoso Sales", "Total Sales", "[Measures].[Total Sales]", "SUM of Amount"),
                ("Contoso Sales", "Total Quantity", "[Measures].[Total Quantity]", "SUM of Quantity"),
                ("Contoso Sales", "Avg Price", "[Measures].[Avg Price]", "AVERAGE of Price"),
                ("Contoso Sales", "Product Count", "[Measures].[Product Count]", "COUNT of Products"),
                ("Contoso Sales", "Day Count", "[Measures].[Day Count]", "COUNT of Calendar days"),
            ],
        )

    # -----------------------------------------------------------------------
    # EVALUATE query parsing (simplified)
    # -----------------------------------------------------------------------

    def _evaluate(self, query: str) -> MockRecordset:
        """Parse simple EVALUATE queries and return matching data."""
        upper = query.upper()

        # SUMMARIZE or aggregations — return summary (check first, before table matches)
        if "SUMMARIZE" in upper or "SUMX" in upper or "SUM(" in upper:
            total_sales = sum(row["Amount"] for row in self.sales)
            total_qty = sum(row["Quantity"] for row in self.sales)
            return MockRecordset(
                fields=["Total Sales", "Total Quantity"],
                rows=[(total_sales, total_qty)],
            )

        # EVALUATE Sales (simple table scan)
        if re.search(r"\bSALES\b", upper) and "PRODUCTS" not in upper and "CALENDAR" not in upper:
            return MockRecordset(
                fields=["SalesKey", "ProductKey", "DateKey", "Quantity", "Amount"],
                rows=[tuple(row.values()) for row in self.sales],
            )

        # EVALUATE Products
        if re.search(r"\bPRODUCTS\b", upper) and "SALES" not in upper:
            return MockRecordset(
                fields=["ProductKey", "ProductName", "Category", "Price"],
                rows=[tuple(row.values()) for row in self.products],
            )

        # EVALUATE Calendar
        if re.search(r"\bCALENDAR\b", upper) and "SALES" not in upper:
            return MockRecordset(
                fields=["DateKey", "Date", "Month", "MonthNum", "Year", "Weekday"],
                rows=[tuple(row.values()) for row in self.calendar],
            )

        # ROW() or simple scalar
        if "ROW(" in upper:
            # Extract values from ROW("Label", value, ...)
            return MockRecordset(
                fields=["Value"],
                rows=[(1,)],
            )

        # Default: return Sales as fallback
        return MockRecordset(
            fields=["SalesKey", "ProductKey", "DateKey", "Quantity", "Amount"],
            rows=[tuple(row.values()) for row in self.sales[:10]],
        )


# ---------------------------------------------------------------------------
# Mock dispatcher factory
# ---------------------------------------------------------------------------

_DEFAULT_CUBE = ContosoCube()


def create_mock_dispatcher(cube: ContosoCube | None = None) -> callable:
    """Create a dispatcher function that returns mock ADODB objects.

    Usage:
        dispatcher = create_mock_dispatcher()
        executor = DAXExecutor(dispatcher=dispatcher)
    """
    cube = cube or _DEFAULT_CUBE

    def dispatcher(name: str) -> Any:
        if name == "ADODB.Connection":
            return MockConnection()
        if name == "ADODB.Command":
            return MockCommand(_cube=cube)
        raise ValueError(f"Unknown ADODB object: {name}")

    return dispatcher


def is_mock_connection(connection_string: str) -> bool:
    """Check if a connection string should use the mock dispatcher."""
    return connection_string.strip().upper().startswith("MOCK://")


def get_mock_cube_name(connection_string: str) -> str:
    """Extract the cube name from a mock connection string.

    Examples:
        MOCK://contoso -> contoso
        MOCK://Contoso Sales -> Contoso Sales
    """
    if not is_mock_connection(connection_string):
        return ""
    return connection_string.strip()[7:]  # Strip "MOCK://"
