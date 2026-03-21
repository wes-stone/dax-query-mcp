# Azure Health Report Connection

Example DAX query for testing:

```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Calendar'[Fiscal Month],
    "Azure Consumed Revenue", [Azure Consumed Revenue]
)
ORDER BY 
    'Calendar'[Fiscal Month] ASC
```

This query returns ~45 rows in ~5 seconds.
