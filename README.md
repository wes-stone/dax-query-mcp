# dax-query-mcp

[![PyPI](https://img.shields.io/pypi/v/dax-query-mcp)](https://pypi.org/project/dax-query-mcp/)

MCP server for running DAX queries against Power BI semantic models.

Check out [making use of skills and extensions](docs/dax-mcp-skill-and-extension-guide.md) for getting better results since the MCP can't do it all

## Features

- **Connection-centric MCP server** — discover models, query with DAX, inspect schemas
- **Relationship-aware context** — teach the LLM your model with markdown, structured dictionaries, relationships, and progressive context bundles
- **Fuzzy search** — search columns and measures across tables by name or description
- **Server-authored follow-ups** — every query response includes a durable next-step workflow for exports, charts, scaffolds, and workstation saves
- **Export anywhere** — CSV, clipboard, Power Query M code, Streamlit apps, standalone Python projects
- **Query builder** — save `.dax` + `.dax.queryBuilder` artifacts, open directly in DAX Studio
- **Workstation session** — save, list, and batch-export queries during an exploration session

## Prerequisites

1. **uv** — Python package manager. Install with:
   ```powershell
   winget install astral-sh.uv
   ```
2. **For MSOLAP connections: Windows + MSOLAP provider** — the default transport uses COM/ADODB.
   Download from [Microsoft](https://learn.microsoft.com/en-us/analysis-services/client-libraries?view=asallproducts-allversions) — grab the **"AMO + ADOMD.NET"** or **"MSOLAP (OLE DB)"** installer. If you already have Power BI Desktop, Excel with Power Pivot, or SSMS installed, you likely have it.
   
   To check: open PowerShell and run:
   ```powershell
   (New-Object System.Data.OleDb.OleDbEnumerator).GetElements() | Where-Object { $_.SOURCES_NAME -like "*MSOLAP*" }
    ```
    If that returns a row, you're good.
3. **For Power BI REST connections: Azure CLI or access token** — REST uses the Power BI `executeQueries` API and does not require Windows or MSOLAP.
    ```powershell
    az login --allow-no-subscriptions
    ```
   On Windows, the standard Azure CLI install path is auto-detected if `az` is
   not on `PATH`. For custom installs, set `AZURE_CLI_PATH` to the full
   `az.cmd` path.

## Quick start

### 1. Install

**From PyPI** (recommended):

```bash
uvx --from dax-query-mcp dax-query-server
```

**From source** (for development or latest changes):

```bash
git clone https://github.com/wes-stone/dax-query-mcp.git
cd dax-query-mcp
uv sync
```

### 2. Add a connection bundle

Each model is represented by a small **connection bundle** in `Connections/`.
The YAML file provides access to the model; the companion context files are the
layer that makes Copilot useful because they explain the model in business terms.

Create `Connections/my_model.yaml` for the connection string and runtime
settings. When `transport` is omitted, it defaults to `msolap`, so existing
connection files keep working unchanged:

```yaml
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/MyWorkspace?readonly;
  Initial Catalog=MySemanticModel

description: "My semantic model"
command_timeout_seconds: 1800
```

For a REST-backed connection, use `transport: powerbi_rest` and the Power BI
dataset ID instead of an MSOLAP connection string:

```yaml
transport: powerbi_rest
dataset_id: "00000000-0000-0000-0000-000000000000"
description: "My semantic model via Power BI REST"
auth_mode: azure_cli
command_timeout_seconds: 1800
max_rows: 50000
```

REST execution always uses the dataset-only `executeQueries` endpoint:
`https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries`.
Do not put `/groups/{workspace_id}` in `api_base_url`; workspace-scoped REST
paths can fail for Build-only access even when the dataset-only endpoint works.

Then add context files with the same connection name:

| File | Purpose |
| ---- | ------- |
| `Connections/my_model.yaml` | Required runtime config: MSOLAP connection string or REST dataset ID, plus description, timeouts, and row limits. |
| `Connections/my_model_overview.md` | Optional compact overview used first by `get_connection_context`; include the most important tables, measures, filters, and example queries. |
| `Connections/my_model.md` | Optional full model context; include detailed table notes, business definitions, caveats, relationships, and query patterns. |
| `Connections/my_model.data_dictionary.yaml` | Optional structured dictionary used by `get_data_dictionary`, `get_schema`, `search_columns`, `search_measures`, and context detail tools; include tables, columns, measures, filters, relationships, descriptions, and sample values. |

Recommended setup flow:

1. Create the `.yaml` connection file.
2. Add a short `_overview.md` so Copilot can quickly understand the model before writing DAX.
3. Add the fuller `.md` context for detailed business logic, naming conventions, and examples.
4. Add or generate the `.data_dictionary.yaml` so tools can search fields and measures structurally.

You can create the dictionary manually or, for MSOLAP connections, scaffold one
from the live model with the `generate_data_dictionary` MCP tool. The generator
uses safe `MDSCHEMA` rowsets for tables, columns, and measures, and includes
high-confidence relationships when optional `TMSCHEMA` rowsets are available.
Then fill in business-specific definitions and sample values.

### 3. Wire up MCP

Add to `.copilot/mcp.json` (or your MCP client config):

**Using PyPI (via `uvx`):**

```json
{
  "mcpServers": {
    "dax-query-server": {
      "command": "uvx",
      "args": ["--from", "dax-query-mcp", "dax-query-server"],
      "env": {
        "DAX_QUERY_MCP_CONNECTIONS_DIR": "C:\\absolute\\path\\to\\Connections"
      }
    }
  }
}
```

**Using a local clone:**

```json
{
  "mcpServers": {
    "dax-query-server": {
      "command": "uv",
      "args": ["run", "--directory", "C:\\path\\to\\dax-query-mcp", "dax-query-server"],
      "env": {
        "DAX_QUERY_MCP_CONNECTIONS_DIR": "C:\\absolute\\path\\to\\Connections"
      }
    }
  }
}
```

> **Tip:** `DAX_QUERY_MCP_CONNECTIONS_DIR` lets you share one `Connections/` folder across workspaces.

### 4. Run your first query

Ask Copilot (or any MCP client):

> "List connections, then run a DAX query against my model."

The server returns plain markdown — results render as tables directly in chat.

## Demo without Power BI: Mock Contoso

The repo includes a safe, deterministic `mock_contoso` connection for README
screenshots, demos, tests, and local development. It uses `MOCK://contoso`, so
it does not require Azure login, Power BI permissions, MSOLAP server access, or
private dataset IDs.

Use it to show the full MCP workflow:

| Step | Prompt to capture | Feature shown |
| --- | --- | --- |
| 1 | `List my DAX connections.` | Connection discovery with `connection_type` |
| 2 | `Get the connection context for mock_contoso.` | Overview/context layer |
| 3 | `Search measures for sales in mock_contoso.` | Structured data dictionary search |
| 4 | `Search columns for category in mock_contoso.` | Column search across model metadata |
| 5 | `Run the Contoso sales summary query.` | DAX execution and markdown result table |
| 6 | `Inspect the mock_contoso connection.` | Live schema inspection via safe MDSCHEMA rowsets |
| 7 | `Export the Contoso sales summary to CSV.` | Export workflow |

### Mock Contoso context layer

The demo connection is a complete connection bundle. This is the feature worth
showing in screenshots: the model is not just a connection string, it includes
LLM-readable context and structured metadata.

```text
Connections/
  mock_contoso.yaml
  mock_contoso_overview.md
  mock_contoso.md
  mock_contoso.data_dictionary.yaml
```

| File | Used by | What it teaches the agent |
| --- | --- | --- |
| `mock_contoso.yaml` | `list_connections`, query execution | Connection name, connection type, runtime settings |
| `mock_contoso_overview.md` | `get_connection_context(..., detail="overview")` | Fast summary: tables, measures, demo queries, screenshot prompts |
| `mock_contoso.md` | `get_connection_context(..., detail="full")` | Deeper guidance and the end-to-end demo walkthrough |
| `mock_contoso.data_dictionary.yaml` | `get_data_dictionary`, `get_schema`, `search_columns`, `search_measures`, context bundle/detail tools | Searchable tables, columns, measures, filters, relationships, descriptions, and sample values |

Connection YAML:

```yaml
connection_string: "MOCK://contoso"
description: "Mock Contoso Sales cube for testing and development"
connection_timeout_seconds: 30
command_timeout_seconds: 300
```

Overview excerpt:

```markdown
## Tables

| Table | Purpose | Useful columns |
| --- | --- | --- |
| `Sales` | Transaction fact table | `SalesKey`, `ProductKey`, `DateKey`, `Quantity`, `Amount` |
| `Products` | Product dimension | `ProductKey`, `ProductName`, `Category`, `Price` |
| `Calendar` | Date dimension for 2025 | `DateKey`, `Date`, `Month`, `MonthNum`, `Year`, `Weekday` |
```

Data dictionary excerpt:

```yaml
measures:
  - name: Total Sales
    expression: SUM(Sales[Amount])
    description: Sum of all sales amounts
    format_string: "$#,##0.00"
filters:
  - name: Category Filter
    column: Products[Category]
    description: Filter by product category
    suggested_values: ["Bikes", "Accessories"]
relationships:
  - from_table: Sales
    from_column: ProductKey
    to_table: Products
    to_column: ProductKey
    cardinality: many-to-one
    cross_filter_direction: single
    is_active: true
    description: Sales transactions roll up to product attributes through ProductKey
    source: curated
    confidence: high
```

Good screenshot query:

```dax
EVALUATE
SUMMARIZE(
    Sales,
    "Total Sales", [Total Sales],
    "Total Quantity", [Total Quantity]
)
```

Expected result shape:

| Total_Sales | Total_Quantity |
| --- | --- |
| 178390.0 | 290 |

## Connection YAML

```yaml
transport: "msolap" # optional — "msolap" (default) or "powerbi_rest"
connection_string: "..." # required for MSOLAP connections
dataset_id: "..." # required for powerbi_rest connections
description: "..." # human-readable label
auth_mode: "azure_cli" # REST auth: "azure_cli" (default) or "env"
access_token_env: "POWERBI_ACCESS_TOKEN" # env var used when auth_mode="env"
api_base_url: "https://api.powerbi.com/v1.0/myorg" # optional REST override
impersonated_user_name: "..." # optional REST impersonation UPN
command_timeout_seconds: 1800 # DAX query timeout
connection_timeout_seconds: 300 # connection open timeout
max_rows: null # row cap (null = unlimited)
suggested_skill: "..." # optional — hint an MCP client toward a specific skill
suggested_skill_reason: "..." # optional — why that skill is relevant
```

## Connection context layer

The context layer is what turns a raw semantic model connection into an
LLM-friendly workspace. Keep the filenames aligned to the connection name:

```text
Connections/
  my_model.yaml
  my_model_overview.md
  my_model.md
  my_model.data_dictionary.yaml
```

`get_connection_context` reads the overview first when it exists, falling back to
the full markdown context. Use the overview for the shortest useful description:
key tables, trusted measures, common filters, grain, date handling, and a few
known-good DAX examples.

Use the full `.md` file for deeper guidance: business definitions, metric
caveats, relationship notes, security/filtering assumptions, common query
patterns, and examples of what not to do.

Use the data dictionary YAML when you want structured metadata that tools can
search and return precisely:

```yaml
version: "1.0"
tables:
  - name: Sales
    description: Fact table with booked transactions
    columns:
      - name: Amount
        data_type: decimal
        description: Transaction amount in USD
        sample_values: ["100.00", "250.50"]
measures:
  - name: Total Sales
    expression: SUM(Sales[Amount])
    description: Sum of booked sales amount
    format_string: "$#,##0.00"
filters:
  - name: Fiscal Year
    column: Calendar[FiscalYear]
    description: Filter by fiscal year
    suggested_values: ["FY25", "FY26"]
relationships:
  - from_table: Sales
    from_column: DateKey
    to_table: Calendar
    to_column: DateKey
    cardinality: many-to-one
    cross_filter_direction: single
    is_active: true
    description: Sales rows filter through Calendar by date key
    source: curated
    confidence: high
```

For agent workflows, start with `get_connection_context(detail="overview")`,
then use `get_context_bundle(detail="overview")` for structured counts and
relationship hints. Fetch scoped context only when needed with
`get_table_detail`, `get_measure_detail`, `get_relationships`, and
`get_filter_suggestions`. `check_context_staleness` compares the dictionary to
live metadata when the transport supports it; `check_ai_readiness` highlights
missing descriptions, ambiguous columns, undocumented relationships, and other
context gaps.

For `powerbi_rest` connections, the context layer is especially important:
Power BI `executeQueries` supports DAX query execution, but not DMV/MDSCHEMA
metadata queries. `inspect_connection` and live dictionary generation are
available for MSOLAP connections; REST connections should rely on the overview,
full markdown context, and `.data_dictionary.yaml`. If a model only works
through REST with Build access, document trusted tables/measures and known-good
queries in the context layer so agents do not try workspace-scoped metadata
paths.

## MCP tools

| Tool                        | Purpose                                              |
| --------------------------- | ---------------------------------------------------- |
| **Discovery**               |                                                      |
| `list_connections`          | Discover available connections as a markdown table (`output_format="json"` for machine-readable output) |
| `get_connection_context`    | Curated markdown context (tables, columns, measures) |
| `search_connection_context` | Search context docs for specific terms               |
| `inspect_connection`        | Live schema via safe `MDSCHEMA` rowsets              |
| `get_context_bundle`        | Progressive structured context with counts, tables, measures, filters, and relationships |
| `get_table_detail`          | Scoped table context plus related relationships      |
| `get_measure_detail`        | Scoped measure context                               |
| `get_relationships`         | Relationship topology, optionally filtered by table  |
| `get_filter_suggestions`    | Suggested filters and allowed values from the dictionary |
| `check_context_staleness`   | Compare dictionary names/counts against live metadata |
| `check_ai_readiness`        | Flag missing/ambiguous context before query writing  |
| `probe_tmschema_capabilities` | Check optional high-fidelity TMSCHEMA relationship access |
| **Querying**                |                                                      |
| `run_connection_query`      | Run DAX against a named connection                   |
| `run_ad_hoc_query`          | Run DAX against a raw connection string              |
| **Search**                  |                                                      |
| `search_columns`            | Fuzzy-search columns across tables                   |
| `search_measures`           | Fuzzy-search measures by name or expression          |
| **Export**                  |                                                      |
| `export_to_csv`             | Export results to a timestamped CSV                  |
| `copy_to_clipboard`         | Copy results to clipboard (TSV or markdown)          |
| `scaffold_power_query`      | Generate Power Query M code for Excel                |
| `scaffold_streamlit_app`    | Generate a Streamlit visualization app               |
| `scaffold_dax_workspace`    | Scaffold a standalone Python project                 |
| `quick_chart`               | Render a bar/line/pie chart as PNG                   |
| **Query builder**           |                                                      |
| `save_query_builder`        | Save `.dax` + `.dax.queryBuilder` artifacts          |
| `get_query_builder`         | Load a saved query builder definition                |
| `get_query_builder_schema`  | Get the expected JSON payload shape                  |
| **Workstation**             |                                                      |
| `save_to_workstation`       | Save a query to the session workstation              |
| `list_workstation`          | List saved workstation queries                       |
| `export_workstation`        | Batch-export workstation as scaffold or `.dax` files |

> **Admin queries are blocked.** `INFO.*()` and `$SYSTEM.DISCOVER_*` require server admin rights.
> General query execution only allows safe `MDSCHEMA` rowsets. Optional
> `TMSCHEMA` access is isolated behind dedicated metadata tools and may be
> unavailable depending on XMLA permissions.

### Python scaffolds

`scaffold_dax_workspace` exports one query as a standalone Python project.
`export_workstation(format="scaffold")` exports every query saved in the
session workstation as one multi-query project.

Generated projects include transport-aware connection config:

| Connection type | Generated behavior |
| --- | --- |
| `msolap` | Uses the embedded Power BI / SSAS connection string through ADODB |
| `powerbi_rest` | Uses Power BI REST `executeQueries` with Azure CLI or env-token auth |
| `mock` (`MOCK://contoso`) | Runs the deterministic demo data path without Power BI or ADODB |

The generated scripts use a `CONNECTION` or `CONNECTIONS` dict instead of a
connection-string-only placeholder, so follow-through exports keep dataset ID,
auth mode, timeout, and mock/REST metadata aligned with the named connection.
Do not commit generated workspaces that contain private dataset IDs, workspace
names, connection strings, or tokens.

## CLI usage

```bash
# List configured queries
dax-query --list --config-dir queries

# Run a query
dax-query --query my_query --preview --config-dir queries

# Inspect a connection schema
dax-query --inspect-connection my_model --connections-dir Connections

# Save a query builder artifact
dax-query-builder --save-query-builder-from builder.json --config-dir queries
```

Saved `.dax` files open directly in **DAX Studio**. See `docs/` for detailed CLI documentation.

## Copilot guard hook

A `pre-commit` hook reviews staged changes for private content (real workspace URIs, local paths, non-sample connection files).

Connection files are ignored by default, and the guard blocks likely real Power
BI workspace URIs or dataset IDs. Keep real REST dataset IDs in local,
untracked connection files; use all-zero sample IDs in docs and tests.

```powershell
# Install
powershell -ExecutionPolicy Bypass -File .\scripts\install-git-hooks.ps1

# Runs automatically on commit:
dax-query-guard --mode staged
```

Add repo-specific patterns via `.copilot-guard.local.json`:

```json
{
  "blocked_content_patterns": [
    {
      "pattern": "PrivateWorkspace|InternalDataset",
      "reason": "Internal identifiers"
    }
  ]
}
```

Fails closed by default. Set `COPILOT_GUARD_FAIL_OPEN=1` to allow commits when Copilot CLI is unavailable.

## Requirements

- **Windows + MSOLAP** for the default `msolap` transport (COM/ADODB)
- **Azure CLI or access token** for the optional `powerbi_rest` transport
- **Python 3.12+** (handled automatically by `uvx`)
- **uv** (`winget install astral-sh.uv`)
