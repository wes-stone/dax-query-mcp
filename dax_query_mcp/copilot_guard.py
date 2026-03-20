from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path(".github") / "copilot-guard.json"
DEFAULT_LOCAL_CONFIG_PATH = Path(".copilot-guard.local.json")


@dataclass(slots=True, frozen=True)
class GuardFinding:
    source: str
    message: str
    file_path: str | None = None
    evidence: str | None = None


def main() -> int:
    parser = argparse.ArgumentParser(description="Git hook guard powered by deterministic rules and Copilot review")
    parser.add_argument("--mode", choices=["staged"], default="staged", help="What git diff scope to scan")
    args = parser.parse_args()

    repo_root = Path.cwd()
    config = load_guard_config(repo_root)
    changed_files = get_staged_files(repo_root)
    if not changed_files:
        print("Copilot guard: no staged changes to scan.")
        return 0

    diff_text = get_staged_diff(repo_root)
    findings = deterministic_scan(changed_files, diff_text, config)

    if findings:
        print_findings(findings)
        return 1

    copilot_result = run_copilot_review(repo_root, changed_files, diff_text)
    if not copilot_result["allow"]:
        findings = [
            GuardFinding(
                source="copilot",
                message=item.get("message", "Copilot flagged a potential leak"),
                file_path=item.get("file"),
                evidence=item.get("evidence"),
            )
            for item in copilot_result.get("findings", [])
        ] or [
            GuardFinding(
                source="copilot",
                message=copilot_result.get("summary", "Copilot blocked the commit"),
            )
        ]
        print_findings(findings)
        return 1

    print("Copilot guard: passed.")
    return 0


def load_guard_config(repo_root: Path) -> dict[str, Any]:
    config: dict[str, Any] = {
        "allowed_file_globs": [],
        "blocked_file_globs": [],
        "blocked_content_patterns": [],
    }

    for path in (repo_root / DEFAULT_CONFIG_PATH, repo_root / DEFAULT_LOCAL_CONFIG_PATH):
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            for key, value in payload.items():
                if isinstance(value, list):
                    config.setdefault(key, []).extend(value)

    return config


def get_staged_files(repo_root: Path) -> list[str]:
    output = run_git(repo_root, ["diff", "--cached", "--name-only", "--diff-filter=ACMR"])
    return [line.strip() for line in output.splitlines() if line.strip()]


def get_staged_diff(repo_root: Path) -> str:
    return run_git(repo_root, ["diff", "--cached", "--no-color", "--unified=0", "--diff-filter=ACMR"])


def deterministic_scan(changed_files: list[str], diff_text: str, config: dict[str, Any]) -> list[GuardFinding]:
    findings: list[GuardFinding] = []
    allowed_globs = config.get("allowed_file_globs", [])
    blocked_globs = config.get("blocked_file_globs", [])

    for file_path in changed_files:
        if _matches_any(file_path, blocked_globs) and not _matches_any(file_path, allowed_globs):
            findings.append(
                GuardFinding(
                    source="rules",
                    file_path=file_path,
                    message="Blocked file path matched a protected pattern",
                    evidence=file_path,
                )
            )

    for file_path, line in iter_added_lines(diff_text):
        for pattern in config.get("blocked_content_patterns", []):
            regex = pattern["pattern"]
            if re.search(regex, line):
                findings.append(
                    GuardFinding(
                        source="rules",
                        file_path=file_path,
                        message=pattern.get("reason", "Blocked content pattern matched"),
                        evidence=line,
                    )
                )

    return findings


def iter_added_lines(diff_text: str) -> list[tuple[str | None, str]]:
    current_file: str | None = None
    results: list[tuple[str | None, str]] = []

    for line in diff_text.splitlines():
        if line.startswith("+++ b/"):
            current_file = line[6:]
            continue
        if line.startswith("+") and not line.startswith("+++"):
            results.append((current_file, line[1:]))

    return results


def run_copilot_review(repo_root: Path, changed_files: list[str], diff_text: str) -> dict[str, Any]:
    prompt = build_copilot_prompt(changed_files, diff_text)
    command = [
        "copilot",
        "-p",
        prompt,
        "-s",
        "--allow-all-tools",
        "--no-custom-instructions",
        "--disable-builtin-mcps",
    ]
    completed = subprocess.run(
        command,
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    stdout = completed.stdout.strip()

    if completed.returncode != 0:
        return _handle_copilot_failure(
            f"Copilot CLI exited with code {completed.returncode}: {completed.stderr.strip() or stdout}"
        )

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return _handle_copilot_failure(f"Copilot CLI returned non-JSON output: {stdout}")


def build_copilot_prompt(changed_files: list[str], diff_text: str) -> str:
    trimmed_diff = diff_text[:18000]
    return (
        "You are a release guard for a public Git repository. Review the staged diff and decide if it is safe to commit. "
        "Look for private connection strings, internal workspace or semantic model names, concrete local filesystem paths, "
        "private hostnames, accidentally committed local config files, or examples that look too real instead of generic samples. "
        "Respond with JSON only using this schema: "
        '{"allow": true|false, "summary": "short text", "findings": [{"file": "path", "message": "why risky", "evidence": "quoted snippet"}]}. '
        "Be conservative. If unsure, set allow=false.\n\n"
        f"Changed files:\n{json.dumps(changed_files, indent=2)}\n\n"
        f"Staged diff:\n{trimmed_diff}"
    )


def print_findings(findings: list[GuardFinding]) -> None:
    print("Copilot guard blocked this commit.")
    for finding in findings:
        location = f" [{finding.file_path}]" if finding.file_path else ""
        print(f"- {finding.source}{location}: {finding.message}")
        if finding.evidence:
            print(f"  {finding.evidence}")
    print("Use 'git commit --no-verify' only if you intentionally accept the risk.")


def run_git(repo_root: Path, args: list[str]) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return completed.stdout


def _handle_copilot_failure(message: str) -> dict[str, Any]:
    if os.getenv("COPILOT_GUARD_FAIL_OPEN") == "1":
        return {"allow": True, "summary": message, "findings": []}
    return {
        "allow": False,
        "summary": message,
        "findings": [{"message": message}],
    }


def _matches_any(value: str, patterns: list[str]) -> bool:
    return any(fnmatch(value, pattern) for pattern in patterns)


if __name__ == "__main__":
    raise SystemExit(main())
