# Mock Contoso Sales overview

Use this connection for README screenshots, demos, tests, and local development when you want the full DAX MCP workflow without any Power BI credentials.

## Why this is useful

`mock_contoso` behaves like a small semantic model but runs entirely in-process through `MOCK://contoso`. It is safe to commit, safe to screenshot, and deterministic across machines.

## Connection

| Property | Value |
| --- | --- |
| Connection name | `mock_contoso` |
| Connection type | `mock` |
| Connection string | `MOCK://contoso` |
| Authentication | None |
| Live schema inspection | Supported through mock MDSCHEMA rowsets |

## Tables

| Table | Purpose | Useful columns |
| --- | --- | --- |
| `Sales` | Transaction fact table | `SalesKey`, `ProductKey`, `DateKey`, `Quantity`, `Amount` |
| `Products` | Product dimension | `ProductKey`, `ProductName`, `Category`, `Price` |
| `Calendar` | Date dimension for 2025 | `DateKey`, `Date`, `Month`, `MonthNum`, `Year`, `Weekday` |

## Measures

| Measure | Description |
| --- | --- |
| `[Total Sales]` | Sum of `Sales[Amount]` |
| `[Total Quantity]` | Sum of `Sales[Quantity]` |
| `[Avg Price]` | Average product price |
| `[Product Count]` | Count of products |
| `[Day Count]` | Count of calendar days |

## Demo queries

### Product catalog

```DAX
EVALUATE
Products
```

### Sales summary

```DAX
EVALUATE
SUMMARIZE(
    Sales,
    "Total Sales", [Total Sales],
    "Total Quantity", [Total Quantity]
)
```

### Transaction preview

```DAX
EVALUATE
Sales
```

## Screenshot tour prompts

1. "List my DAX connections."
2. "Get the overview for `mock_contoso`."
3. "Search measures for sales in `mock_contoso`."
4. "Run the Contoso sales summary query."
5. "Inspect the `mock_contoso` connection schema."
6. "Export the Contoso sales summary to CSV."
7. "Make a quick chart from the Contoso sales summary."
