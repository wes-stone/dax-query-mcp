from __future__ import annotations

from dax_query_mcp.copilot_guard import deterministic_scan, iter_added_lines, load_guard_config


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
