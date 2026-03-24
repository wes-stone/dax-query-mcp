"""AI-in-the-loop integration tests for dax-query-server.

These tests use the GitHub Copilot SDK to run real conversations against the
dax-query-server MCP. The LLM connects to the MCP tools, receives a user
prompt, and must follow the server's instructions (auto-execute queries,
render tables, show next-steps).

The MCP server runs as a real subprocess (stdio transport) — no mocking.

Requirements:
    - Copilot CLI running (github-copilot-sdk connects to it)
    - github-copilot-sdk package installed
    - Power BI Desktop running with the semantic model

Run:
    pytest tests/test_ai_integration.py -v -s
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

import pytest

copilot_mod = pytest.importorskip("copilot", reason="github-copilot-sdk required for AI integration tests")

from copilot import CopilotClient, MCPLocalServerConfig, PermissionRequestResult  # noqa: E402

# Permission result that auto-approves all MCP tool calls
_APPROVE = PermissionRequestResult(kind="approved")

# ---------------------------------------------------------------------------
# MCP server configuration (must match working mcp-config.json pattern)
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_DAX_SERVER_EXE = os.path.join(_PROJECT_ROOT, ".venv", "Scripts", "dax-query-server.exe")

if not os.path.exists(_DAX_SERVER_EXE):
    _DAX_SERVER_EXE = sys.executable
    _DAX_SERVER_ARGS = ["-m", "dax_query_mcp.mcp_server"]
else:
    _DAX_SERVER_ARGS: list[str] = []

# Connections directory (same as production config)
_CONNECTIONS_DIR = os.path.join(
    os.path.expanduser("~"), ".copilot", "dax-query-mcp", "Connections"
)

MCP_STARTUP_WAIT = 15   # seconds to wait for MCP server to connect
SEND_TIMEOUT = 300.0    # seconds to wait for LLM response (DAX queries can be slow)
METADATA_TIMEOUT = 600.0  # metadata discovery can trigger many MDSCHEMA calls

# Directory for conversation transcripts
_CONVO_OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "llm_test_convos")

pytestmark = [
    pytest.mark.ai_integration,
]

# Tool names as they appear in event.data.tool_name (prefixed by server name)
_QUERY_TOOLS = {
    "dax-query-server-run_connection_query",
    "dax-query-server-run_ad_hoc_query",
}
_CONTEXT_TOOL = "dax-query-server-get_connection_context"
_LIST_TOOL = "dax-query-server-list_connections"


# ---------------------------------------------------------------------------
# Conversation runner using GitHub Copilot SDK
# ---------------------------------------------------------------------------
async def run_ai_conversation(
    user_prompt: str,
    timeout: float = SEND_TIMEOUT,
    convo_label: str = "conversation",
) -> dict[str, Any]:
    """Run a conversation via Copilot SDK with the dax-query-server MCP.

    Returns:
        tool_calls_made: list of tool names invoked (prefixed, e.g.
            "dax-query-server-list_connections")
        final_response: the LLM's final text to the user
        query_args: list of (tool_name, arguments_dict) for query tools
        tool_results: list of (tool_name, result_content) for completed tools

    Side-effect: writes a conversation transcript to llm_test_convos/.
    """
    client = CopilotClient()
    await client.start()

    # Key: tools=["*"] exposes all MCP tools (empty list = no tools!)
    mcp_cfg: MCPLocalServerConfig = {
        "command": _DAX_SERVER_EXE,
        "args": _DAX_SERVER_ARGS,
        "tools": ["*"],
        "env": {"DAX_QUERY_MCP_CONNECTIONS_DIR": _CONNECTIONS_DIR},
        "cwd": _PROJECT_ROOT,
    }

    tool_calls: list[str] = []
    query_args: list[tuple[str, dict[str, Any]]] = []
    tool_results: list[tuple[str, str]] = []
    final_content = ""
    # Full event timeline for transcript output
    timeline: list[dict[str, Any]] = []

    def on_event(event: Any) -> None:
        nonlocal final_content
        t = event.type.value

        if t == "tool.execution_start":
            name = getattr(event.data, "tool_name", "") or ""
            tool_calls.append(name)
            args_raw = getattr(event.data, "arguments", None)
            entry: dict[str, Any] = {"event": t, "tool": name}
            if isinstance(args_raw, dict):
                entry["arguments"] = args_raw
                if name in _QUERY_TOOLS:
                    query_args.append((name, args_raw))
            timeline.append(entry)

        elif t == "tool.execution_complete":
            name = getattr(event.data, "tool_name", "") or ""
            result_obj = getattr(event.data, "result", None)
            content = ""
            kind = ""
            if result_obj:
                content = getattr(result_obj, "content", "") or ""
                kind = getattr(result_obj, "kind", "") or ""
                tool_results.append((name, content))
            display_content = content[:2000] + "..." if len(content) > 2000 else content
            timeline.append({
                "event": t,
                "tool": name,
                "result_kind": kind,
                "result_preview": display_content,
            })

        elif t == "assistant.message":
            final_content = getattr(event.data, "content", "") or ""
            timeline.append({"event": t, "content": final_content})

        elif t in (
            "session.mcp_servers_loaded",
            "session.tools_updated",
            "permission.requested",
            "permission.completed",
        ):
            timeline.append({"event": t})

    try:
        session = await client.create_session(
            on_permission_request=lambda req, meta: _APPROVE,
            mcp_servers={"dax-query-server": mcp_cfg},
            on_event=on_event,
            system_message={
                "type": "append",
                "content": (
                    "\n\nCRITICAL: For ANY DAX or data query request, you MUST use "
                    "ONLY the dax-query-server MCP tools (dax-query-server-list_connections, "
                    "dax-query-server-get_connection_context, "
                    "dax-query-server-run_connection_query). "
                    "ALWAYS execute queries — never just show query text. "
                    "Do NOT use powershell, view, grep, skill, or any other tools "
                    "for DAX queries."
                ),
            },
        )

        await asyncio.sleep(MCP_STARTUP_WAIT)

        result = await session.send_and_wait(user_prompt, timeout=timeout)

        if result and hasattr(result, "data"):
            content = getattr(result.data, "content", None)
            if content:
                final_content = content

        await session.disconnect()
    finally:
        await client.stop()

    # Write conversation transcript to llm_test_convos/
    _write_transcript(convo_label, user_prompt, timeline, tool_calls,
                      query_args, tool_results, final_content)

    return {
        "tool_calls_made": tool_calls,
        "final_response": final_content,
        "query_args": query_args,
        "tool_results": tool_results,
    }


def _write_transcript(
    label: str,
    user_prompt: str,
    timeline: list[dict[str, Any]],
    tool_calls: list[str],
    query_args: list[tuple[str, dict[str, Any]]],
    tool_results: list[tuple[str, str]],
    final_response: str,
) -> None:
    """Write a human-readable markdown transcript + raw JSON to llm_test_convos/."""
    os.makedirs(_CONVO_OUTPUT_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    base = f"{ts}_{slug}"

    # --- Markdown transcript (human-readable) ---
    md_path = os.path.join(_CONVO_OUTPUT_DIR, f"{base}.md")
    lines = [
        f"# LLM Test Conversation: {label}",
        f"**Timestamp:** {datetime.now(timezone.utc).isoformat()}",
        f"**User Prompt:** {user_prompt}",
        "",
        "---",
        "",
        "## Event Timeline",
        "",
    ]
    for i, evt in enumerate(timeline, 1):
        etype = evt.get("event", "unknown")
        if etype == "tool.execution_start":
            tool = evt.get("tool", "?")
            args = evt.get("arguments", {})
            lines.append(f"### {i}. 🔧 Tool Call: `{tool}`")
            if args:
                if "query" in args:
                    lines.append(f"\n**DAX Query:**\n```dax\n{args['query']}\n```")
                    other_args = {k: v for k, v in args.items() if k != "query"}
                    if other_args:
                        lines.append(f"\n**Other Args:** `{json.dumps(other_args)}`")
                else:
                    lines.append(f"\n**Arguments:** `{json.dumps(args)}`")
            lines.append("")
        elif etype == "tool.execution_complete":
            tool = evt.get("tool", "?")
            kind = evt.get("result_kind", "")
            preview = evt.get("result_preview", "")
            lines.append(f"### {i}. ✅ Tool Result: `{tool}` ({kind})")
            if preview:
                lines.append(f"\n```\n{preview}\n```")
            lines.append("")
        elif etype == "assistant.message":
            content = evt.get("content", "")
            lines.append(f"### {i}. 💬 Assistant Response")
            lines.append(f"\n{content}")
            lines.append("")
        else:
            lines.append(f"### {i}. ℹ️ {etype}")
            lines.append("")

    lines.extend([
        "---",
        "",
        "## Summary",
        "",
        f"**Tools Called ({len(tool_calls)}):** {', '.join(f'`{t}`' for t in tool_calls) or 'none'}",
        "",
        f"**Query Tools Used:** {len(query_args)}",
        "",
        f"**Final Response Length:** {len(final_response)} chars",
        "",
    ])

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # --- Raw JSON (machine-readable) ---
    json_path = os.path.join(_CONVO_OUTPUT_DIR, f"{base}.json")
    raw = {
        "label": label,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_prompt": user_prompt,
        "timeline": timeline,
        "tool_calls_made": tool_calls,
        "query_args": [(n, a) for n, a in query_args],
        "tool_results": [(n, r[:2000]) for n, r in tool_results],
        "final_response": final_response,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Helper to run async tests
# ---------------------------------------------------------------------------
def _run(coro: Any) -> Any:
    """Run an async coroutine in a new event loop."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Shared fixture — run the query conversation ONCE, assert many things
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def query_result() -> dict[str, Any]:
    """Run a single GitHub Copilot DAX query conversation.

    Shared across all tests in the module to avoid repeated 2-5 min
    LLM round-trips. Each test asserts a different behavioral property
    on the same conversation result.
    """
    return _run(run_ai_conversation(
        "Give me a DAX query example of GitHub Copilot and execute it",
        convo_label="query_execution",
    ))


@pytest.fixture(scope="module")
def metadata_result() -> dict[str, Any]:
    """Run a metadata discovery conversation (triggers admin query guard).

    Uses a simpler prompt and longer timeout because metadata discovery
    can trigger multiple MDSCHEMA rowset calls via inspect_connection.
    """
    return _run(run_ai_conversation(
        "List the tables in the data model using get_connection_context",
        timeout=METADATA_TIMEOUT,
        convo_label="metadata_discovery",
    ))


# ---------------------------------------------------------------------------
# AI integration tests
# ---------------------------------------------------------------------------
class TestQueryExecution:
    """Tests that the LLM executes queries (not just shows text)."""

    def test_query_tool_was_called(self, query_result: dict[str, Any]) -> None:
        """The LLM should call a query execution tool, not just show DAX text."""
        executed = _QUERY_TOOLS & set(query_result["tool_calls_made"])
        assert executed, (
            f"LLM did NOT execute the query. Tools called: {query_result['tool_calls_made']}. "
            f"Expected at least one of {_QUERY_TOOLS}. "
            f"Final response snippet: {query_result['final_response'][:300]}"
        )

    def test_response_contains_table(self, query_result: dict[str, Any]) -> None:
        """The LLM's final response must contain an actual markdown table."""
        final = query_result["final_response"]
        # Match markdown table: | header | ... |\n| --- | ... |
        has_table = bool(re.search(r"\|.+\|.*\n\|[-\s|]+\|", final))
        assert has_table, (
            f"LLM response does not contain a markdown table. "
            f"Response snippet: {final[:500]}"
        )

    def test_response_contains_next_steps(self, query_result: dict[str, Any]) -> None:
        """The LLM's final response must include the numbered follow-up menu."""
        final = query_result["final_response"]
        final_lower = final.lower()

        # Must contain the heading
        has_heading = "what would you like to do next?" in final_lower

        # Must contain at least 5 of the 11 numbered items
        numbered_items_found = sum(
            1 for i in range(1, 12) if f"{i}." in final
        )

        # Must contain specific action keywords from _NEXT_STEPS
        has_filter = "filter" in final_lower or "refine" in final_lower
        has_export = "csv" in final_lower or "export" in final_lower
        has_chart = "chart" in final_lower

        assert has_heading, (
            f"Missing 'What would you like to do next?' heading. "
            f"Response snippet: {final[:500]}"
        )
        assert numbered_items_found >= 5, (
            f"Expected at least 5 numbered items (1. through 11.), "
            f"found {numbered_items_found}. Response snippet: {final[:500]}"
        )
        assert has_filter and has_export and has_chart, (
            f"Missing key action keywords (filter/export/chart). "
            f"Response snippet: {final[:500]}"
        )

    def test_get_connection_context_called_before_query(
        self, query_result: dict[str, Any]
    ) -> None:
        """The LLM should call get_connection_context before running a query."""
        tools = query_result["tool_calls_made"]
        context_idx = None
        query_idx = None
        for i, name in enumerate(tools):
            if name == _CONTEXT_TOOL and context_idx is None:
                context_idx = i
            if name in _QUERY_TOOLS and query_idx is None:
                query_idx = i

        if query_idx is not None:
            assert context_idx is not None and context_idx < query_idx, (
                f"get_connection_context should be called before query execution. "
                f"Tool order: {tools}"
            )


class TestAdminQueryGuard:
    """Tests that admin/DDL queries are blocked by the server."""

    def test_admin_queries_rejected_or_avoided(
        self, metadata_result: dict[str, Any]
    ) -> None:
        """If the LLM attempts admin queries, the server must reject them.
        The LLM should use safe tools like get_connection_context or
        inspect_connection for metadata discovery."""
        # Check if any admin queries were attempted
        admin_attempted = False
        for _, args in metadata_result["query_args"]:
            query = args.get("query", "")
            if "INFO." in query.upper() or "$SYSTEM.DISCOVER_" in query.upper():
                admin_attempted = True

        if admin_attempted:
            # Server should have rejected — check ALL tool results for
            # rejection language (tool_name on execution_complete may
            # differ from execution_start, so don't filter by name)
            all_results = [r for _, r in metadata_result["tool_results"]]
            rejected = any(
                "admin" in r.lower()
                or "prohibited" in r.lower()
                or "not allowed" in r.lower()
                or "blocked" in r.lower()
                for r in all_results
            )
            assert rejected, (
                f"Admin query was attempted but not rejected by server. "
                f"Tool results (first 3): {all_results[:3]}"
            )

    def test_safe_metadata_tools_used(
        self, metadata_result: dict[str, Any]
    ) -> None:
        """The LLM should use get_connection_context or inspect_connection
        for metadata discovery, not admin queries."""
        safe_tools = {_CONTEXT_TOOL, "dax-query-server-inspect_connection"}
        used_safe = safe_tools & set(metadata_result["tool_calls_made"])
        assert used_safe, (
            f"LLM did not use safe metadata tools. "
            f"Tools called: {metadata_result['tool_calls_made']}"
        )
