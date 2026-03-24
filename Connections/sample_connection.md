# Sample Connection (Mock Contoso Sales)

This sample connection uses the **built-in mock cube** so you can test the full workflow immediately — no Power BI access needed.

## Tables

| Table | Description | Rows |
|-------|-------------|------|
| Sales | Fact table with transactions | 100 |
| Products | Product dimension (5 products) | 5 |
| Calendar | Date dimension (2024–2025) | 336 |

## Measures

| Measure | Description |
|---------|-------------|
| Total Sales | SUM of Amount |
| Total Quantity | SUM of Quantity |
| Avg Price | AVERAGE of Price |
| Product Count | COUNT of Products |

## Quick start queries

```dax
-- List all products
EVALUATE Products

-- Total sales by product
EVALUATE
SUMMARIZECOLUMNS(
    Products[ProductName],
    "Revenue", [Total Sales],
    "Qty", [Total Quantity]
)

-- Monthly sales
EVALUATE
SUMMARIZECOLUMNS(
    Calendar[YearMonth],
    "Revenue", [Total Sales]
)
ORDER BY Calendar[YearMonth]
```

## Switching to a real connection

Replace the `connection_string` in `sample_connection.yaml` with your Power BI connection:

```yaml
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/YourWorkspace?readonly;
  Initial Catalog=YourSemanticModel
```
