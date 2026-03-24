# Contoso Sales (Mock Cube)

This is a **mock semantic model** for testing and development. It simulates a real Power BI / SSAS cube without requiring a live connection.

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
EVALUATE SUMMARIZE(Sales, [Total Sales], [Total Quantity])

-- Get all sales transactions
EVALUATE Sales
```

## Usage

Set your connection string to `MOCK://contoso` to use this mock cube.
