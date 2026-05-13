# Getting the Most Out of dax-query-mcp — Skill & Extension Guide

> **TL;DR:** The MCP server gives you the tools. A **Copilot Skill** teaches the
> LLM *how* to use them effectively. A **Copilot Extension** enforces good
> behavior *at runtime*. Together, they turn a decent MCP experience into a
> flawless one.

---

## Why Bother?

Out of the box, an LLM connected to dax-query-mcp will:

1. ❌ **Forget which tools to call** — it has 100+ MCP tools loaded across servers
   and won't always reach for `run_connection_query` when you say "show me ARR."
2. ❌ **Summarize query results** — system prompts tell LLMs "be concise" so they
   convert your beautiful table into bullet points and drop the follow-up menu.
3. ❌ **Skip the workflow** — it'll try to write raw Python instead of using the
   MCP tools, or jump straight to querying without reading the schema first.

A **Skill** fixes #1 and #3 by giving the LLM a playbook to follow.  
An **Extension** fixes #2 by injecting instructions at exactly the right moment.

---

## Part 1: Create a Copilot Skill

A skill is a markdown file that gets loaded into the LLM's context when invoked.
It teaches the LLM what tools exist, what order to call them, and how to handle
the output.

### Step 1: Create the skill directory

```bash
mkdir -p ~/.copilot/skills/dax-query
```

### Step 2: Create `SKILL.md`

Create `~/.copilot/skills/dax-query/SKILL.md` with the following content.
Customize the sections marked with `<!-- CUSTOMIZE -->` for your environment.

````markdown
---
name: dax-query
description: >
  Skill for querying Power BI / Analysis Services data using the
  dax-query-server MCP. Covers connection discovery, progressive context,
  relationship-aware schema exploration,
  DAX execution, workstation management, query packs, validated query libraries, exports, scaffolding, and
  follow-up workflows. Triggers on DAX query, Power BI data, run query,
  connection, semantic model, workstation, query pack, validated query, export CSV, scaffold, chart.
---

# DAX Query Skill — Using the dax-query-server MCP

> Operational guide for querying Power BI / Analysis Services semantic models
> via the **dax-query-server** MCP tools. This skill teaches workflows, not
> DAX syntax.

---

## 1. Golden Rules

1. **Use MCP tools only.** Never write raw Python, never call `dax_to_pandas`
   directly, never launch explore agents to find connection files.
2. **Execute queries — don't just show DAX text.** Build the DAX AND call
   `run_connection_query` in the same turn.
3. **Output query results verbatim.** The tool returns a complete markdown
   string (table + follow-up menu). Output the ENTIRE string as your response.
   Do NOT summarize, convert the table to bullet points, or omit the
   "What would you like to do next?" menu.
4. **Call `get_connection_context` once per session** before writing your first
   query. Don't re-read it every turn.
5. **Use progressive context when needed.** Start compact, then drill into
   `get_context_bundle`, `get_table_detail`, `get_measure_detail`,
   `get_relationships`, and `get_filter_suggestions` instead of loading
   everything.
6. **Preserve discovered context.** Exploration can discover
   tables/measures/relationships, but durable knowledge belongs in overview/full
   markdown, `.data_dictionary.yaml`, and validated query libraries.
7. **Reuse known-good patterns.** Search `search_validated_queries` before
   inventing DAX for a recurring metric/grain/filter shape. Save repeated,
   successful patterns with `save_validated_query`, then smoke-test them with
   `validate_query_library`.
8. **Never run admin queries.** No `INFO.*()`, `$SYSTEM.DISCOVER_*`, `DBCC`,
   `ALTER`, `CREATE`, `DELETE`, `DROP`. Use context tools, `inspect_connection`,
   or `generate_data_dictionary` for metadata.

---

## 2. First-Time Workflow

```
Step 1 → list_connections()                             # discover what's available
Step 2 → get_connection_context("my_connection")        # compact model overview
Step 2b → get_context_bundle("my_connection")           # optional structured counts/relationships
Step 2c → get_table_detail / get_measure_detail / get_relationships only if needed
Step 2d → search_validated_queries("my_connection", "metric/grain") if reusable examples may exist
Step 3 → run_connection_query("my_connection", "EVALUATE SUMMARIZECOLUMNS(...)")
         ↳ Output the ENTIRE returned string verbatim (table + menu)
Step 4 → User picks from the follow-up menu (e.g., "3" = save to workstation)
```

### Quick Start Example

```
User: "Show me revenue by month"

You should:
1. Call get_connection_context("my_connection") if not already cached
2. Use search_validated_queries, search_measures/search_columns, and scoped context tools to resolve exact names and reusable patterns
3. Build DAX: EVALUATE SUMMARIZECOLUMNS('Calendar'[Month], "Revenue", [Revenue])
4. Call run_connection_query("my_connection", <DAX>)
5. Output the full result verbatim — table AND numbered menu
```

---

## 3. Tool Reference

### Query Execution

| Tool | When to Use |
|------|-------------|
| `list_connections` | First thing — discover available connections |
| `get_connection_context` | Before first query — compact model overview, tables, measures |
| `run_connection_query` | **Primary tool** — execute DAX against a named connection |
| `run_ad_hoc_query` | Execute DAX with a raw connection string |
| `run_named_query` | Execute a pre-configured named query |

### Schema Discovery

| Tool | When to Use |
|------|-------------|
| `search_connection_context` | Search context docs for terms |
| `search_columns` | Fuzzy-search columns by name |
| `search_measures` | Fuzzy-search measures by name or expression |
| `get_data_dictionary` | Structured JSON data dictionary |
| `get_context_bundle` | Progressive structured context: overview counts, schema, relationships, next levels |
| `get_table_detail` | Drill into one table, including columns and related relationships |
| `get_measure_detail` | Drill into one measure, expression, description, and format string |
| `get_relationships` | Inspect relationship paths globally or for one table |
| `get_filter_suggestions` | Get curated filter columns and suggested values |
| `inspect_connection` | Live schema via MDSCHEMA rowsets |
| `generate_data_dictionary` | Scaffold `.data_dictionary.yaml` from live MDSCHEMA metadata, with TMSCHEMA relationships when available |
| `check_ai_readiness` | Find missing descriptions, ambiguous columns, missing filters, or undocumented relationships |
| `check_context_staleness` | Compare live model metadata with the saved data dictionary |
| `probe_tmschema_capabilities` | Check whether richer relationship metadata is available for an MSOLAP connection |

### Progressive Context Pattern

Use this pattern when the compact overview is not enough:

```
get_connection_context(connection, detail="overview")
→ search_measures/search_columns for likely names
→ search_validated_queries(connection, search_term) when reusable examples may exist
→ get_context_bundle(connection, detail="overview") for counts and relationship summary
→ get_table_detail/get_measure_detail/get_relationships only for relevant objects
→ run_connection_query
```

Do **not** force full context every turn. The improved context layer keeps agents
focused: start small, drill in, then preserve useful discoveries in markdown or
the dictionary so the next session starts smarter.

### Export & Output

| Tool | What It Produces |
|------|-----------------|
| `export_to_csv` | Timestamped CSV file |
| `copy_to_clipboard` | TSV or markdown on clipboard |
| `quick_chart` | Bar/line/pie chart as PNG |

### Scaffolding

| Tool | What It Produces |
|------|-----------------|
| `scaffold_power_query` | Excel Power Query M code |
| `scaffold_streamlit_app` | Live single-query Streamlit explorer |
| `scaffold_dax_workspace` | Full Python project |
| `save_query_builder` | `.dax` + `.dax.queryBuilder` for DAX Studio |

### Workstation (Session-Scoped)

| Tool | When to Use |
|------|-------------|
| `save_to_workstation` | Save a query to the in-memory session |
| `list_workstation` | List accumulated queries |
| `export_workstation` | Batch-export as scaffold project or `.dax` files |
| `remove_from_workstation` | Remove one by name |
| `clear_workstation` | Wipe all saved queries |

> The workstation is **ephemeral** — resets each session.
> Use `export_workstation` to make queries permanent.

### Query Packs (Durable)

| Tool | When to Use |
|------|-------------|
| `create_query_pack` | Start a reusable DAX pack with `pack.yaml` |
| `save_query_to_pack` | Save a curated query to `pack.yaml` + `queries/*.dax` |
| `list_query_pack` | Inspect saved query IDs, tags, parameters, and outputs |
| `validate_query_pack` | Check safe DAX, declared parameters, files, and connections; optionally dry-run live queries with low row caps |
| `describe_query_pack` | Generate markdown pack summaries for review and sharing |
| `export_query_pack` | Generate `run_queries.py`, `streamlit_app.py`, `power_query/*.pq`, and docs |

Use query packs when an exploration becomes reusable or shareable. Prefer the
workstation for scratch queries, then `export_workstation(format="scaffold")`
to make the whole session durable as a query pack.

If the `dax_pack_*` Copilot CLI extension tools are available, use them for
repo-local pack creation/export orchestration and command generation. They are
thin wrappers over the same query-pack library, not a replacement for MCP query
execution.

Generated workspaces include `pyproject.toml` and are uv-first. Use `uv run`
for one-off execution, or `uv sync` followed by `uv run --no-sync ...` when you
want to materialize the environment before repeated runs.

### Validated Query Libraries (Connection-Scoped Context)

| Tool | When to Use |
|------|-------------|
| `save_validated_query` | Save a successful query as a reusable connection-scoped DAX example |
| `list_validated_queries` | List saved examples, tags, parameters, and validation status |
| `search_validated_queries` | Find known-good DAX templates before writing a new query |
| `validate_query_library` | Smoke-test saved examples and persist validation status, columns, and row counts |

Use validated query libraries when a query pattern should improve future
context, but does not need a full runnable project. They live next to the
connection as `<connection_name>.validated_queries/<query_id>.yaml` plus
`<query_id>.dax`.

`search_validated_queries` returns DAX by default because it is an explicit
retrieval tool. General context surfaces such as `get_context_bundle` include
metadata-only summaries. Validation uses live execution; `max_rows` caps returned
rows but may not make the server-side DAX plan cheap, so keep validation examples
small and well-filtered. Required parameters need defaults or
`sample_parameters_json` before an entry can pass validation.

---

## 4. Follow-Up Menu Mapping

After every query, the tool returns a server-authored numbered menu. Output it
exactly as returned, do not generate your own version, then map user selections:

| User Says | Tool to Call |
|-----------|-------------|
| "1" or "filter" | Modify DAX + `run_connection_query` again |
| "2" or "aggregate" | Modify DAX grouping + `run_connection_query` |
| "3" or "save" | `save_to_workstation` |
| "4" or "copy" | `copy_to_clipboard` |
| "5" or "csv" | `export_to_csv` |
| "6" or "chart" | `quick_chart` |
| "7" or "power query" | `scaffold_power_query` |
| "8" or "streamlit" | `scaffold_streamlit_app` |
| "9" or "dax studio" | `save_query_builder` |
| "10" or "scaffold" | `scaffold_dax_workspace` |
| "11" or "re-run" | `run_connection_query` with same params |

The flat 11-item menu is the compatibility contract for the current query.
For durable multi-query work, use query packs. Agents can inspect
`followup://menu/grouped` to distinguish **This query** actions from
**Current pack** actions such as validate, describe, and export query pack, and
**Validated query library** actions such as save/search/validate reusable DAX
examples.

---

## 5. Output Rendering (CRITICAL)

When any query tool returns a result, output the **entire returned string**.

### ✅ Correct

```
### Query preview for `my_connection`

| Month | Revenue |
|-------|---------|
| Jan   | 28M     |

---

What would you like to do next?

 1. Filter / refine — ...
...
11. Re-run last query — ...
```

### ❌ Wrong

- Summarizing: "The data shows 12 months of revenue..."
- Bullet points instead of table
- Omitting the "What would you like to do next?" menu

---

## 6. Error Recovery

| Error | What to Do |
|-------|------------|
| `ADMIN_QUERY_BLOCKED` | Rewrite with EVALUATE or use context tools |
| `CONNECTION_NOT_FOUND` | Call `list_connections`, retry with correct name |
| `QUERY_TIMEOUT` | Add filters (TREATAS, TOPN), simplify query |
| `EXECUTION_FAILED` | Check names via `get_connection_context`, fix syntax |

---

## 7. DAX Quick Reference

```dax
-- Every query starts with EVALUATE
EVALUATE <table_expression>

-- Grouped aggregation (most common)
EVALUATE SUMMARIZECOLUMNS(
    'Table'[GroupColumn],
    "Metric Name", [MeasureName]
)

-- Cross-table filtering
EVALUATE SUMMARIZECOLUMNS(
    'Calendar'[Month],
    TREATAS({"Value"}, 'Dimension'[Column]),
    "Total", [SomeMeasure]
)

-- Limit results
EVALUATE TOPN(100, 'LargeTable')

-- Order results
EVALUATE SUMMARIZECOLUMNS(...) ORDER BY 'Calendar'[MonthId]
```

**Naming:** `'Table'[Column]` — single quotes for tables, brackets for columns.

---

## 8. Connection Architecture

Connections are YAML files in a configurable directory:

```yaml
# ~/.copilot/dax-query-mcp/Connections/my_connection.yaml
connection_string: "Provider=MSOLAP;Data Source=localhost:PORT;..."
description: "My Power BI dataset"
connection_timeout_seconds: 300
command_timeout_seconds: 1800
max_rows: 50000
```

Each connection can have companion files:
- `my_connection_overview.md` — compact overview used first by `get_connection_context(detail="overview")`
- `my_connection.md` — full detailed context with business rules, caveats, relationships, and examples
- `my_connection.data_dictionary.yaml` — structured tables, columns, measures, filters, relationships, descriptions, and sample values
- `my_connection.validated_queries/` — metadata and `.dax` files for reusable known-good query patterns

The default connections directory is set by `DAX_QUERY_MCP_CONNECTIONS_DIR`, or
falls back to `~/.copilot/dax-query-mcp/Connections/`.

### Data Dictionary Shape

Existing dictionaries still work. Add relationships and richer descriptions over
time; relationships are optional but make DAX generation much more reliable.

```yaml
relationships:
  - from_table: Sales
    from_column: ProductKey
    to_table: Products
    to_column: ProductKey
    cardinality: many-to-one
    cross_filter_direction: single
    is_active: true
    description: Sales rows roll up to product attributes through ProductKey
    source: curated
    confidence: high
```

Best mental model: exploration generates context; the overview, full markdown,
data dictionary, and validated query library preserve it.

---

## 9. Workstation Workflow

The workstation is an **in-memory scratchpad** for iterative query exploration:

```
1. Run queries → refine → run again
2. When you like a query: save_to_workstation(connection, query, "description")
3. Accumulate several queries over the session
4. When done: export_workstation(output_dir, format="scaffold")
   → Creates portable Python project with all your queries
```

The workstation resets when the server restarts. This is intentional. Use
`export_workstation` to make queries permanent.

---

## 10. Common Workflows

### "Show me data about X"
```
get_connection_context → search/context drill-in if needed → build DAX → run_connection_query → output verbatim
```

### "Save this and export later"
```
save_to_workstation → more queries → save_to_workstation → export_workstation
```

### "Make this reusable/shareable"
```
create_query_pack → save_query_to_pack → validate_query_pack → export_query_pack
→ run generated run_queries.py or open generated streamlit_app.py
```

The generated query-pack `streamlit_app.py` is the richest exploration surface:
catalog search, tag/connection filters, typed parameters, editable rendered DAX,
cached execution, an Explore tab that keeps run/results/charts/pivots together,
run history, result filters, drag-and-drop CSV/JSON upload exploration,
profiling, and CSV/JSON/schema/DAX downloads.

### "Reuse a known-good query pattern"
```
search_validated_queries("my_connection", "monthly revenue") → adapt DAX/template params → run_connection_query
```

### "Save a repeated pattern into context"
```
run_connection_query → save_validated_query → validate_query_library(max_rows=1)
```

### "I want to use this in Excel"
```
Option A: copy_to_clipboard(format="tsv") → paste into Excel
Option B: scaffold_power_query → paste M code into Power Query Editor
Option C: export_to_csv → open CSV in Excel
```

### "Build me a dashboard"
```
For a quick one-off live explorer: scaffold_streamlit_app → uv run streamlit run app.py
For a multi-query library: save queries to a query pack → export_query_pack → uv run streamlit run streamlit_app.py
```

### "Make this pack shareable"
```
validate_query_pack(dry_run=True, max_rows=1) → describe_query_pack → export_query_pack
```

### "What columns have revenue?"
```
search_columns("my_connection", "revenue") → get fuzzy matches → use in DAX
```

### "Which measure calculates ARR?"
```
search_measures("my_connection", "ARR") → get name + expression → use in DAX
```

### "How does this model join?"
```
get_context_bundle("my_connection") → get_relationships("my_connection", "Sales") → use filter paths correctly
```

### "Is my context good enough?"
```
check_ai_readiness("my_connection") → fill missing descriptions/relationships/filters → check_context_staleness("my_connection")
```
````

### Step 3: Verify the skill loads

Open a new Copilot CLI session and check the skill appears:

```
❯ /skills
```

You should see `dax-query` listed. Invoke it by saying something that matches
the trigger words (e.g., "run a DAX query" or "show me Power BI data").

### Customization Tips

- **Add your own connection names** in the Quick Start Example section so the
  LLM knows what to call `get_connection_context` with.
- **Add domain-specific examples** — if your model has a `'Calendar'[Fiscal
  Month]` column, show that in the DAX Quick Reference.
- **Add common queries** — if you always start with the same SUMMARIZECOLUMNS
  pattern, include it as a template.

---

## Part 2: Create a Copilot Extension

Extensions are JavaScript hooks that fire at specific points in the
conversation. They're more powerful than skills because they inject instructions
**at runtime** — not just at the start.

This repo also ships a project-scoped query-pack extension at
`.github/extensions/dax-query-pack/extension.mjs`. It adds thin `dax_pack_*`
tools for creating, adding, validating, exporting, and returning run/Streamlit
commands for query packs. Use it as an additive workflow layer: it calls the
same Python query-pack library as the MCP tools and does not replace the MCP
server.

### Why an Extension?

The #1 problem with MCP tools is that the LLM **summarizes query results
instead of showing the full table and menu**. This happens because:

1. System prompts tell LLMs to "be concise" (~100 words)
2. The LLM sees your 50-row table and thinks "I should summarize this"
3. The "What would you like to do next?" menu gets dropped

An extension fixes this with two hooks:

- **`onUserPromptSubmitted`** — When the user says something DAX-related, inject
  "use the dax-query-server tools" before the LLM starts thinking
- **`onPostToolUse`** — After a DAX query tool returns results, inject "render
  this verbatim" right before the LLM generates its response

### Step 1: Create the extension directory

```bash
mkdir -p ~/.copilot/extensions/dax-output-enforcer
```

### Step 2: Create `extension.mjs`

Create `~/.copilot/extensions/dax-output-enforcer/extension.mjs`:

```javascript
import { joinSession } from "@github/copilot-sdk/extension";

// ── Keywords that indicate a DAX query request ──────────────────────────
// Customize this regex to match your domain vocabulary.
const DAX_KEYWORDS =
    /\b(dax|power\s*bi|summarizecolumns|evaluate\b|semantic\s*model|measure|run.*query|fiscal|revenue|seats?\s*(by|per))/i;

// ── Tool names that return query results ────────────────────────────────
const DAX_QUERY_TOOLS = new Set([
    "dax-query-server-run_connection_query",
    "dax-query-server-run_ad_hoc_query",
    "dax-query-server-run_named_query",
]);

const session = await joinSession({
    hooks: {
        onSessionStart: async () => {
            await session.log("📌 DAX output enforcer extension loaded");
        },

        // ── Before the LLM starts thinking ──────────────────────────────
        // Detect DAX-related prompts and inject tool routing instructions.
        onUserPromptSubmitted: async (input) => {
            const prompt = input.prompt || "";

            if (!DAX_KEYWORDS.test(prompt)) return;

            return {
                additionalContext: `## DAX Query Instructions
Use the dax-query-server MCP tools — do NOT write Python scripts.

**Workflow:**
1. list_connections → discover connections
2. get_connection_context(connection_name) → compact overview (once per session)
3. Search validated query examples when a recurring metric/grain/filter pattern may exist
4. Use search/context tools only when needed:
   get_context_bundle, get_table_detail, get_measure_detail, get_relationships
5. run_connection_query(connection_name, query) → execute DAX

**CRITICAL OUTPUT RULE:** When run_connection_query returns results,
output the ENTIRE returned string verbatim — the data table AND the
numbered "What would you like to do next?" menu. Do NOT summarize,
truncate, convert to bullet points, or omit the menu.`,
            };
        },

        // ── After a DAX query tool returns results ──────────────────────
        // Remind the LLM to render verbatim, right before it generates.
        onPostToolUse: async (input) => {
            if (DAX_QUERY_TOOLS.has(input.toolName)) {
                return {
                    additionalContext:
                        "IMPORTANT: You just received DAX query results. " +
                        "Output the ENTIRE returned string as your response — " +
                        "the full data table AND the numbered " +
                        "'What would you like to do next?' menu. " +
                        "Do NOT summarize, truncate, or omit the menu. " +
                        "The tool formatted this output specifically for the user.",
                };
            }

            // When a query fails, hint at recovery
            if (
                input.toolName === "dax-query-server-run_connection_query" &&
                input.toolResult?.resultType === "failure"
            ) {
                return {
                    additionalContext:
                        "The DAX query failed. Check column/table names via " +
                        "get_connection_context, search_columns, search_measures, " +
                        "or scoped context tools. Common " +
                        "issues: wrong column name, missing single quotes " +
                        "around table names, or referencing a measure that " +
                        "doesn't exist in this model.",
                };
            }
        },
    },
    tools: [],
});
```

### Step 3: Reload extensions

In your Copilot CLI session:

```
/extensions reload
```

You should see `📌 DAX output enforcer extension loaded` in your session.

### Step 4: Verify it works

1. Say: "Show me data from my Power BI model"
2. The extension injects DAX instructions before the LLM starts
3. The LLM calls `list_connections` → `get_connection_context` → `run_connection_query`
4. After the query tool returns, the extension injects the verbatim rendering rule
5. The LLM outputs the full table + follow-up menu

---

## Part 3: Multi-MCP Routing Extension (Advanced)

If you use multiple MCP servers (not just dax-query-mcp), you can build a
single extension that routes to the right tools based on keywords.

This is useful when the LLM has many tools loaded and doesn't know which server
to reach for.

```javascript
import { joinSession } from "@github/copilot-sdk/extension";

// ── Keyword → MCP routing map ───────────────────────────────────────────
// Add entries for each MCP server you use.

const TOOL_ROUTES = [
    {
        name: "DAX / Power BI queries",
        keywords: /\b(dax|power\s*bi|summarizecolumns|evaluate\b|semantic\s*model|run.*query)/i,
        context: `## DAX Query Instructions
Use dax-query-server MCP tools. Workflow: list_connections → get_connection_context → search validated queries or context drill-in if needed → run_connection_query.
Output the ENTIRE result verbatim including the "What would you like to do next?" menu.`,
    },
    // <!-- CUSTOMIZE: Add routes for your other MCP servers -->
    // {
    //     name: "My Other MCP",
    //     keywords: /\b(keyword1|keyword2)/i,
    //     context: `Use my-other-mcp tools. Start with list_items(), then...`,
    // },
];

const DAX_QUERY_TOOLS = new Set([
    "dax-query-server-run_connection_query",
    "dax-query-server-run_ad_hoc_query",
    "dax-query-server-run_named_query",
]);

const session = await joinSession({
    hooks: {
        onSessionStart: async () => {
            await session.log("📌 MCP Tool Router extension loaded");
        },

        onUserPromptSubmitted: async (input) => {
            const prompt = input.prompt || "";
            const matched = [];

            for (const route of TOOL_ROUTES) {
                if (route.keywords.test(prompt)) {
                    matched.push(route.context);
                }
            }

            if (matched.length === 0) return;

            return {
                additionalContext: matched.join("\n\n---\n\n"),
            };
        },

        onPostToolUse: async (input) => {
            if (DAX_QUERY_TOOLS.has(input.toolName)) {
                return {
                    additionalContext:
                        "IMPORTANT: Output the ENTIRE returned string " +
                        "verbatim — table AND numbered menu. Do NOT summarize.",
                };
            }
        },
    },
    tools: [],
});
```

---

## How It All Fits Together

```
┌─────────────────────────────────────────────────────────────┐
│                     Your MCP Ecosystem                      │
│                                                             │
│  ┌─────────────────┐   ┌─────────────┐   ┌──────────────┐  │
│  │  dax-query-mcp  │   │   Skill     │   │  Extension   │  │
│  │  (MCP Server)   │   │  (SKILL.md) │   │  (.mjs hook) │  │
│  │                 │   │             │   │              │  │
│  │  30+ tools      │   │  Teaches    │   │  Enforces    │  │
│  │  Connections    │   │  workflows  │   │  behavior    │  │
│  │  Context layer  │   │  Tool refs  │   │  at runtime  │  │
│  └────────┬────────┘   └──────┬──────┘   └──────┬───────┘  │
│           │                   │                  │          │
│           └───────────────────┼──────────────────┘          │
│                               │                             │
│                    ┌──────────▼──────────┐                  │
│                    │    LLM / Copilot    │                  │
│                    │                     │                  │
│                    │  1. Extension fires  │                  │
│                    │     (inject context) │                  │
│                    │  2. Skill loaded     │                  │
│                    │     (if invoked)     │                  │
│                    │  3. MCP tools called │                  │
│                    │  4. Extension fires  │                  │
│                    │     (enforce output) │                  │
│                    └─────────────────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

**Layer 1 — MCP Server** (`dax-query-mcp`): The tools themselves. This is what
gets installed via `uvx dax-query-mcp`.

**Layer 2 — Skill** (`SKILL.md`): A markdown playbook that gets loaded into the
LLM's context. Teaches the full workflow, tool reference, error recovery, and
output rendering rules. Loaded when the user invokes the skill or when trigger
keywords match.

**Layer 3 — Extension** (`extension.mjs`): JavaScript hooks that fire at runtime.
`onUserPromptSubmitted` catches keyword-matching prompts and injects tool routing.
`onPostToolUse` catches query results and enforces verbatim rendering. This is the
strongest signal because it fires *right before* the LLM generates its response.

### Why all three layers?

Each layer has a different strength:

| Layer | When it fires | Strength | Weakness |
|-------|--------------|----------|----------|
| MCP tool docstrings | Tool discovery phase | Always present | Far from generation point |
| Skill (SKILL.md) | Session start / invocation | Comprehensive context | Can be buried by other context |
| Extension hook | Per-prompt and per-tool-use | Fires at exactly the right moment | Limited to short injections |

The extension is your **last line of defense** — even if the LLM ignores the
skill's rendering rules, the `onPostToolUse` hook fires right before generation
and says "output this verbatim." That's the signal closest to where the LLM is
actually writing its response.

---

## Troubleshooting

### LLM still summarizes query results

1. Check that the extension is loaded: `/extensions` should show your extension
2. Look for `📌 DAX output enforcer extension loaded` at session start
3. Try reloading: `/extensions reload`
4. If using the multi-MCP router, check that `DAX_QUERY_TOOLS` contains the
   correct tool names (they include the server prefix, e.g.,
   `dax-query-server-run_connection_query`)

### LLM doesn't call the right tools

1. Check that your keyword regex matches what you're typing
2. Add more trigger words to the `keywords` regex
3. Check that the skill is loaded: `/skills` should show `dax-query`

### Skill doesn't appear

1. Verify the file is at `~/.copilot/skills/dax-query/SKILL.md`
2. Check the YAML frontmatter has `name:` and `description:`
3. The `description:` field must contain trigger words — the LLM matches on this

### Extension doesn't fire

1. Verify the file is at `~/.copilot/extensions/dax-output-enforcer/extension.mjs`
2. Run `/extensions reload`
3. Check for JavaScript errors in the extension output

---

## Customization Checklist

Before using, customize these sections:

- [ ] **Skill SKILL.md** — Update the Quick Start Example with your real
  connection names
- [ ] **Skill SKILL.md** — Add domain-specific DAX patterns your team uses
- [ ] **Extension keywords regex** — Add terms specific to your data domain
  (product names, metric names, team-specific jargon)
- [ ] **Extension TOOL_ROUTES** — If you use other MCP servers, add routing
  entries for them
- [ ] **Connection context docs** — Create `_overview.md`, full `.md`, and
  `.data_dictionary.yaml` companions alongside connection YAML files to document
  tables, measures, relationships, filters, and common query patterns

---

## Further Reading

- [dax-query-mcp README](../README.md) — Full MCP server documentation
- [Copilot CLI Skills](https://docs.github.com/en/copilot/customizing-copilot/adding-repository-custom-instructions) — How skills work
- [Copilot CLI Extensions](https://docs.github.com/en/copilot/customizing-copilot) — How extensions work
- [MCP Protocol](https://modelcontextprotocol.io) — Model Context Protocol specification
