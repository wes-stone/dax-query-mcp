# Copilot Instructions for dax-query-mcp

## What this project is

This is an MCP server (`dax-query-server`) for executing DAX queries against Power BI / Analysis Services semantic models. It is installed as a tool — **use the MCP tools directly, do not write raw Python scripts or explore the filesystem**.

## How to use this MCP server

### Quick start — running a DAX query

1. Call `run_connection_query` with `connection_name` and `query` — that's it.
2. Do NOT write Python scripts, do NOT use `dax_to_pandas` directly, do NOT launch explore agents to find connection files.

### Before your first query

Call `get_connection_context` with the connection name. This returns the schema overview (tables, columns, measures, sample queries). Then compose and execute your DAX query in the same turn.

### Example workflow

```
1. get_connection_context("ahr_connection")      → learn the schema
2. run_connection_query("ahr_connection", "EVALUATE SUMMARIZECOLUMNS(...)")  → get data
3. User says "save it" → save_to_workstation(...)
4. User says "export" → export_to_csv(...) or copy_to_clipboard(...)
```

## Rules

- **NEVER** write raw Python to execute DAX. Always use the MCP tools.
- **NEVER** launch explore agents to find connection files or read .yaml files.
- **NEVER** read run_query.py or dax_to_pandas source code to figure out how to query.
- **ALWAYS** use `run_connection_query` to execute queries.
- Call `get_connection_context` once per session — don't re-read it every turn.

## Output rendering (CRITICAL)

When `run_connection_query`, `run_ad_hoc_query`, or `run_named_query` returns
a result, output the **entire returned string** as your response. This overrides
any general brevity guidelines. Specifically:

1. The markdown table must appear as a **table**, not bullet points.
2. The numbered "What would you like to do next?" list must appear in full.
3. Do NOT add your own summary, commentary, or rephrased version of the data.

## Available tools (summary)

| Tool | When to use |
|------|-------------|
| `list_connections` | Find available connections |
| `get_connection_context` | Get schema overview (call once, before first query) |
| `run_connection_query` | Execute a DAX query (primary tool) |
| `run_ad_hoc_query` | Execute DAX with a raw connection string |
| `search_connection_context` | Search for specific tables/columns/filters in docs |
| `search_columns` | Fuzzy search for columns by name |
| `search_measures` | Fuzzy search for measures by name |
| `save_to_workstation` | Save a query you like to the working session |
| `list_workstation` | See all saved queries |
| `export_workstation` | Bulk export saved queries |
| `export_to_csv` | Save results to CSV |
| `copy_to_clipboard` | Copy results to clipboard (TSV for Excel) |
| `quick_chart` | Generate a chart from results |
| `scaffold_power_query` | Generate Excel Power Query M code |
| `scaffold_streamlit_app` | Generate Streamlit dashboard |
| `scaffold_dax_workspace` | Create portable Python project |

## DAX query basics

- Every query starts with `EVALUATE`
- Use `SUMMARIZECOLUMNS` for aggregations
- Use `TOPN(n, ...)` to limit results
- Quote table names: `'Sales'[Revenue]`
- Use measures defined in the model when available
