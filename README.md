# dax-query-mcp

[![PyPI](https://img.shields.io/pypi/v/dax-query-mcp)](https://pypi.org/project/dax-query-mcp/)

MCP server for running DAX queries against Power BI semantic models.

Check out [making use of skills and extensions](docs/dax-mcp-skill-and-extension-guide.md) for getting better results since the MCP can't do it all

## Features

- **Connection-centric MCP server** — discover models, query with DAX, inspect schemas
- **Curated context** — teach the LLM your model via markdown docs (no admin privileges needed)
- **Fuzzy search** — search columns and measures across tables by name or description
- **Export anywhere** — CSV, clipboard, Power Query M code, Streamlit apps, standalone Python projects
- **Query builder** — save `.dax` + `.dax.queryBuilder` artifacts, open directly in DAX Studio
- **Workstation session** — save, list, and batch-export queries during an exploration session

## Prerequisites

1. **Windows** — the server uses COM/ADODB under the hood, so it's Windows-only.
2. **MSOLAP provider** — the OLE DB driver that talks to Power BI semantic models.
   Download from [Microsoft](https://learn.microsoft.com/en-us/analysis-services/client-libraries?view=asallproducts-allversions) — grab the **"AMO + ADOMD.NET"** or **"MSOLAP (OLE DB)"** installer. If you already have Power BI Desktop, Excel with Power Pivot, or SSMS installed, you likely have it.
   
   To check: open PowerShell and run:
   ```powershell
   (New-Object System.Data.OleDb.OleDbEnumerator).GetElements() | Where-Object { $_.SOURCES_NAME -like "*MSOLAP*" }
   ```
   If that returns a row, you're good.

3. **uv** — Python package manager. Install with:
   ```powershell
   winget install astral-sh.uv
   ```

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
settings:

```yaml
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/MyWorkspace?readonly;
  Initial Catalog=MySemanticModel

description: "My semantic model"
command_timeout_seconds: 1800
```

Then add context files with the same connection name:

| File | Purpose |
| ---- | ------- |
| `Connections/my_model.yaml` | Required connection string, description, timeouts, and row limits. |
| `Connections/my_model_overview.md` | Optional compact overview used first by `get_connection_context`; include the most important tables, measures, filters, and example queries. |
| `Connections/my_model.md` | Optional full model context; include detailed table notes, business definitions, caveats, relationships, and query patterns. |
| `Connections/my_model.data_dictionary.yaml` | Optional structured dictionary used by `get_data_dictionary`, `get_schema`, `search_columns`, and `search_measures`; include tables, columns, measures, filters, descriptions, and sample values. |

Recommended setup flow:

1. Create the `.yaml` connection file.
2. Add a short `_overview.md` so Copilot can quickly understand the model before writing DAX.
3. Add the fuller `.md` context for detailed business logic, naming conventions, and examples.
4. Add or generate the `.data_dictionary.yaml` so tools can search fields and measures structurally.

You can create the dictionary manually or scaffold one from the live model with
the `generate_data_dictionary` MCP tool, then fill in business descriptions and
sample values.

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

## Connection YAML

```yaml
connection_string: "..." # required — MSOLAP connection string
description: "..." # human-readable label
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
```

## MCP tools

| Tool                        | Purpose                                              |
| --------------------------- | ---------------------------------------------------- |
| **Discovery**               |                                                      |
| `list_connections`          | Discover available connections                       |
| `get_connection_context`    | Curated markdown context (tables, columns, measures) |
| `search_connection_context` | Search context docs for specific terms               |
| `inspect_connection`        | Live schema via safe `MDSCHEMA` rowsets              |
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
> Use `get_connection_context` or `inspect_connection` for metadata.

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

- **Windows** (COM/ADODB used for DAX execution)
- **MSOLAP** OLE DB provider (see [Prerequisites](#prerequisites))
- **Python 3.12+** (handled automatically by `uvx`)
- **uv** (`winget install astral-sh.uv`)
