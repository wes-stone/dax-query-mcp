# Contoso Sales (Mock Cube)

This is a **mock semantic model** for testing, screenshots, docs, and local development. It simulates a real Power BI / SSAS cube without requiring a live connection, Azure login, Power BI permissions, or MSOLAP server access.

Use `mock_contoso` when you want to demonstrate the connection bundle workflow:

1. `list_connections`
2. `get_connection_context`
3. `search_columns` / `search_measures`
4. `run_connection_query`
5. `inspect_connection`
6. `export_to_csv`, `copy_to_clipboard`, `quick_chart`, or scaffold tools

## Tables

| Table | Description | Rows |
|-------|-------------|------|
| Sales | Fact table with transactions | 100 |
| Products | Product dimension | 5 |
| Calendar | Date dimension | 336 |

## Products

| ProductKey | ProductName | Category | Price |
|------------|-------------|----------|-------|
| 1 | Mountain Bike | Bikes | $1,500 |
| 2 | Road Bike | Bikes | $1,200 |
| 3 | Helmet | Accessories | $50 |
| 4 | Gloves | Accessories | $25 |
| 5 | Water Bottle | Accessories | $10 |

## Measures

| Measure | Description |
|---------|-------------|
| Total Sales | SUM of Amount |
| Total Quantity | SUM of Quantity |
| Avg Price | AVERAGE of Price |
| Product Count | COUNT of Products |
| Day Count | COUNT of Calendar days |

## Example Queries

```dax
-- Get all products
EVALUATE Products

-- Get sales summary
EVALUATE
SUMMARIZE(
    Sales,
    "Total Sales", [Total Sales],
    "Total Quantity", [Total Quantity]
)

-- Get all sales transactions
EVALUATE Sales
```

## Usage

Set your connection string to `MOCK://contoso` to use this mock cube.

## Suggested screenshot flow

### 1. Discovery

Prompt:

```text
List my DAX connections.
```

Look for `mock_contoso` with `connection_type: msolap`.

### 2. Context layer

Prompt:

```text
Get the connection context for mock_contoso.
```

This demonstrates how the overview/context files guide the agent before it writes DAX.

### 3. Search

Prompts:

```text
Search measures for sales in mock_contoso.
Search columns for category in mock_contoso.
```

This demonstrates structured data dictionary lookup.

### 4. Query execution

Prompt:

```text
Run the Contoso sales summary query.
```

Expected query:

```dax
EVALUATE
SUMMARIZE(
    Sales,
    "Total Sales", [Total Sales],
    "Total Quantity", [Total Quantity]
)
```

### 5. Live schema inspection

Prompt:

```text
Inspect the mock_contoso connection.
```

This demonstrates safe mock MDSCHEMA discovery without a real Power BI model.
