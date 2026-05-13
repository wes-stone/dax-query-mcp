from __future__ import annotations

import json
from pathlib import Path

import subprocess

from dax_query_mcp.copilot_guard import (
    deterministic_scan,
    iter_added_lines,
    load_guard_config,
    run_copilot_review,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_tracked_guard_config() -> dict:
    return json.loads((REPO_ROOT / ".github" / "copilot-guard.json").read_text(encoding="utf-8"))


def test_iter_added_lines_tracks_file_context() -> None:
    diff = """diff --git a/README.md b/README.md
+++ b/README.md
@@ -1,0 +1,1 @@
+hello
diff --git a/Connections/private.yaml b/Connections/private.yaml
+++ b/Connections/private.yaml
@@ -0,0 +1,1 @@
+secret
"""

    lines = iter_added_lines(diff)

    assert lines == [("README.md", "hello"), ("Connections/private.yaml", "secret")]


def test_deterministic_scan_blocks_private_connection_files_and_patterns() -> None:
    config = {
        "allowed_file_globs": ["Connections/sample_connection.yaml"],
        "blocked_file_globs": ["Connections/*.yaml"],
        "blocked_content_patterns": [
            {
                "pattern": "Data Source=powerbi://api\\.powerbi\\.com/v1\\.0/myorg/(?!SampleWorkspace\\b)",
                "reason": "Potential real Power BI workspace URI detected",
            }
        ],
    }

    findings = deterministic_scan(
        ["Connections/private.yaml", "README.md"],
        """diff --git a/README.md b/README.md
+++ b/README.md
@@ -1,0 +1,1 @@
+Data Source=powerbi://api.powerbi.com/v1.0/myorg/RealWorkspace
""",
        config,
    )

    assert len(findings) == 2
    assert findings[0].file_path == "Connections/private.yaml"
    assert findings[1].file_path == "README.md"


def test_tracked_guard_blocks_real_powerbi_rest_identifiers() -> None:
    config = _load_tracked_guard_config()

    findings = deterministic_scan(
        ["README.md"],
        """diff --git a/README.md b/README.md
+++ b/README.md
@@ -1,0 +1,2 @@
+dataset_id: "12345678-1234-1234-1234-123456789abc"
+POST https://api.powerbi.com/v1.0/myorg/datasets/12345678-1234-1234-1234-123456789abc/executeQueries
""",
        config,
    )

    assert [finding.message for finding in findings] == [
        "Potential real Power BI dataset ID detected",
        "Potential real Power BI executeQueries dataset endpoint detected",
    ]


def test_tracked_guard_allows_powerbi_sample_identifiers() -> None:
    config = _load_tracked_guard_config()

    findings = deterministic_scan(
        ["README.md"],
        """diff --git a/README.md b/README.md
+++ b/README.md
@@ -1,0 +1,4 @@
+Data Source=powerbi://api.powerbi.com/v1.0/myorg/SampleWorkspace
+Data Source=powerbi://api.powerbi.com/v1.0/myorg/YourWorkspace
+dataset_id: "00000000-0000-0000-0000-000000000000"
+POST https://api.powerbi.com/v1.0/myorg/datasets/00000000-0000-0000-0000-000000000000/executeQueries
""",
        config,
    )

    assert findings == []


def test_tracked_guard_allows_mock_contoso_overview_file() -> None:
    config = _load_tracked_guard_config()

    findings = deterministic_scan(
        ["Connections/mock_contoso_overview.md"],
        "",
        config,
    )

    assert findings == []


def test_load_guard_config_merges_tracked_and_local_files(tmp_path, monkeypatch) -> None:
    repo_root = tmp_path
    (repo_root / ".github").mkdir()
    (repo_root / ".github" / "copilot-guard.json").write_text(
        '{"blocked_file_globs":["Connections/*.yaml"]}',
        encoding="utf-8",
    )
    (repo_root / ".copilot-guard.local.json").write_text(
        '{"blocked_content_patterns":[{"pattern":"PrivateWorkspace","reason":"internal"}]}',
        encoding="utf-8",
    )

    config = load_guard_config(repo_root)

    assert config["blocked_file_globs"] == ["Connections/*.yaml"]
    assert config["blocked_content_patterns"][0]["pattern"] == "PrivateWorkspace"


def test_run_copilot_review_handles_timeout_with_fail_open(tmp_path, monkeypatch) -> None:
    def timeout_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="copilot", timeout=120)

    monkeypatch.setattr(subprocess, "run", timeout_run)
    monkeypatch.setenv("COPILOT_GUARD_FAIL_OPEN", "1")

    result = run_copilot_review(tmp_path, ["README.md"], "diff")

    assert result == {
        "allow": True,
        "summary": "Copilot CLI review timed out after 120 seconds",
        "findings": [],
    }
