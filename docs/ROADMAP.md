# DAX MCP Improvement Plan

## Vision

Make `dax-query-mcp` the best way for AI agents (and humans) to explore and query Power BI / Analysis Services semantic models — fast, context-aware, and with rich follow-up options.

---

## Priority Areas

### 1. Performance — Speed Speed Speed

**Current state**: Single-threaded ADODB execution, full `GetRows()` into memory, no caching, no connection pooling.

**Remote tasks** (no live connection needed):
- [ ] **Streaming row fetch** — Replace eager `GetRows()` with incremental fetch using `Recordset.MoveNext()` and yield pattern. Benchmark with mock recordset.
- [ ] **Connection pooling design** — Spec out a pooled executor that reuses open connections across queries. Can prototype with fake dispatcher.
- [ ] **Async executor wrapper** — Wrap blocking COM calls in `asyncio.to_thread()` so MCP server can handle concurrent tool calls.
- [ ] **Result caching layer** — LRU cache keyed by (connection_string_hash, query_hash, max_rows). Useful for repeated metadata queries.
- [ ] **Profiling harness** — Add timing instrumentation (`time.perf_counter()`) around connection open, execute, fetch, normalize. Log to stderr so MCP clients can see.

**Local tasks** (require real connection):
- [ ] **Benchmark baseline** — Run 5 representative queries and record times for connect / execute / fetch / normalize.
- [ ] **Test streaming fetch on large result** — Compare memory + wall time for 100k+ row queries.
- [ ] **Test connection reuse** — Measure overhead of opening a new connection vs. reusing an existing one.

---

### 2. Instruction Following & Context Understanding

**Current state**: `_SERVER_INSTRUCTIONS` in mcp_server.py, `validate_dax_query()` guard, but LLMs still hallucinate admin queries or forget to show tables.

**Remote tasks**:
- [ ] **Expand server instructions** — Add explicit examples of good/bad behavior, chain-of-thought hints.
- [ ] **Tool docstring audit** — Rewrite every tool docstring with numbered rules the LLM must follow.
- [ ] **Guardrail test suite** — Unit tests that assert `validate_dax_query()` rejects all admin patterns and passes all safe ones.
- [ ] **Add `last_query` context resource** — MCP resource that returns the last executed query + result shape so follow-ups can reference it.
- [ ] **Structured error responses** — Return JSON errors with `error_code`, `message`, `suggestion` so LLM can self-correct.

**Local tasks**:
- [ ] **Live LLM eval** — Run 10 prompts through Copilot and score instruction adherence.
- [ ] **Iterate on instructions** — Based on eval, tweak `_SERVER_INSTRUCTIONS` and re-test.

---

### 3. Schema Exploration & Data Dictionaries

**Current state**: `inspect_connection()` runs MDSCHEMA rowsets, `get_connection_context()` reads sibling `.md` file.

**Remote tasks**:
- [ ] **Data dictionary YAML schema** — Define a structured `data_dictionary.yaml` format per connection: tables → columns → descriptions, measures, filters.
- [ ] **Data dictionary loader** — Parse `data_dictionary.yaml` alongside `.md` context and expose via `get_connection_context`.
- [ ] **Data dictionary generator tool** — MCP tool that runs MDSCHEMA rowsets and writes a starter `data_dictionary.yaml`.
- [ ] **Column search tool** — `find_column(pattern)` searches all tables/columns in the data dictionary for a fuzzy match.
- [ ] **Measure search tool** — `find_measure(pattern)` searches measures by name or description.

**Local tasks**:
- [ ] **Generate real data dictionary** — Run the generator against a live cube and validate output.
- [ ] **Test column search accuracy** — Ensure fuzzy matching returns sensible results.

---

### 4. Follow-Ups & Export Options

**Current state**: `scaffold_dax_workspace` (Python script + notebook), `save_query_builder` (DAX Studio `.dax` + `.queryBuilder`).

**Remote tasks**:
- [ ] **Excel Power Query scaffold** — Generate an `.xlsx` with a Power Query `M` snippet that calls the DAX query. Users open it, refresh, done.
- [ ] **Streamlit app scaffold** — Generate a `streamlit_app.py` that runs the query and displays an interactive table + basic chart.
- [ ] **Quick chart tool** — MCP tool that takes last query result and renders a simple Matplotlib/Plotly chart, returns base64 PNG or HTML.
- [ ] **CSV export tool** — MCP tool that writes query result to a timestamped CSV and returns the path.
- [ ] **Clipboard copy tool** — Copy result to clipboard as tab-separated (paste into Excel) or markdown.
- [ ] **Follow-up menu resource** — MCP resource listing available follow-up actions so LLM can offer them dynamically.

**Local tasks**:
- [ ] **Validate Excel scaffold** — Open generated `.xlsx` in Excel, refresh Power Query, confirm data loads.
- [ ] **Validate Streamlit scaffold** — Run `streamlit run streamlit_app.py` and confirm it works.

---

### 5. Simulation Cube for Remote Development

**Goal**: Enable full integration testing and remote AI development without VPN / live data.

**Remote tasks**:
- [ ] **Mock ADODB dispatcher** — Extend fake dispatcher to return canned recordsets for specific queries.
- [ ] **Sample cube definition** — Define a small "Contoso Sales" cube: 3 tables, 5 measures, 100 rows of fake data.
- [ ] **Query router** — If connection string contains `MOCK://`, route to the mock dispatcher instead of COM.
- [ ] **Canned query responses** — Predefine responses for common queries (MDSCHEMA_*, simple EVALUATE).
- [ ] **CI integration** — GitHub Actions workflow that runs full MCP tool tests using mock cube.

**Local tasks**:
- [ ] **Validate mock parity** — Compare mock output to real output for the same queries.

---

## Task Summary

| Category | Remote | Local |
|----------|--------|-------|
| Performance | 5 | 3 |
| Instruction following | 5 | 2 |
| Schema exploration | 5 | 2 |
| Follow-ups | 6 | 2 |
| Simulation cube | 5 | 1 |
| **Total** | **26** | **10** |

---

## Suggested Execution Order

1. **Simulation cube** — unlocks remote dev for everything else.
2. **Performance profiling harness** — understand where time goes before optimizing.
3. **Instruction following guardrails + tests** — improve reliability.
4. **Data dictionary schema + loader** — better context for queries.
5. **Follow-up scaffolds** — Excel, Streamlit, CSV export.
6. **Performance optimizations** — streaming, pooling, async.

---

## Notes

- All **remote tasks** can be done via GitHub Codespaces or AI agents without VPN.
- **Local tasks** require Windows + MSOLAP + network access to Power BI / SSAS.
- The mock cube should be realistic enough to exercise all MCP tools.
