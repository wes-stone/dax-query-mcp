# dax-query-mcp

MCP server for running DAX queries against Power BI semantic models.

Check out [making use of skills and extensions](docs/dax-mcp-skill-and-extension-guide.md) for getting better results since the MCP can't do it all

## Features

- **Connection-centric MCP server** — discover models, query with DAX, inspect schemas
- **Curated context** — teach the LLM your model via markdown docs (no admin privileges needed)
- **Fuzzy search** — search columns and measures across tables by name or description
- **Export anywhere** — CSV, clipboard, Power Query M code, Streamlit apps, standalone Python projects
- **Query builder** — save `.dax` + `.dax.queryBuilder` artifacts, open directly in DAX Studio
- **Workstation session** — save, list, and batch-export queries during an exploration session

## Quick start

### 1. Install

**From PyPI** (recommended):

```bash
uv pip install dax-query-mcp
```

**From source** (for development or latest changes):

```bash
git clone https://github.com/wes-stone/dax-query-mcp.git
cd dax-query-mcp
uv sync
```

### 2. Add a connection

Create `Connections/my_model.yaml`:

```yaml
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/MyWorkspace?readonly;
  Initial Catalog=MySemanticModel

description: "My semantic model"
command_timeout_seconds: 1800
```

Optionally add `Connections/my_model.md` alongside it to document tables, measures, and common filters for the LLM.

### 3. Wire up MCP

Add to `.copilot/mcp.json` (or your MCP client config):

**Using PyPI (via `uvx`):**

```json
{
  "mcpServers": {
    "dax-query-server": {
      "command": "uvx",
      "args": ["dax-query-mcp", "dax-query-server"],
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
- **MSOLAP** / Analysis Services client libraries
- **Python 3.12+**
- **uv**
