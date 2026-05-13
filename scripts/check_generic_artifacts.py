from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


DEFAULT_TARGET_GLOBS = (
    "README.md",
    "docs/**/*.md",
    ".github/extensions/**/*.mjs",
    "dax_query_mcp/scaffold.py",
    "dax_query_mcp/query_pack_export.py",
)

BLOCKED_PATTERNS = (
    (re.compile(r"\bMercury\b", re.IGNORECASE), "internal model name"),
    (re.compile(r"\bFinHub\b", re.IGNORECASE), "internal model name"),
    (re.compile(r"\bBilled\s+AGR\b", re.IGNORECASE), "internal model name"),
    (re.compile(r"\bAHR\b"), "internal model acronym"),
    (re.compile(r"C:\\+Users\\+[^\\\s\"]+", re.IGNORECASE), "user-specific local path"),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check shareable docs/templates for non-generic examples.")
    parser.add_argument("--root", default=".", help="Repository root to scan.")
    parser.add_argument("--glob", action="append", default=[], help="Additional glob to scan.")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    findings = scan_generic_artifacts(root, tuple(args.glob) or DEFAULT_TARGET_GLOBS)
    if findings:
        for path, line_number, reason, evidence in findings:
            print(f"{path}:{line_number}: {reason}: {evidence}")
        return 1
    print("Generic artifact guard: passed.")
    return 0


def scan_generic_artifacts(
    root: Path,
    target_globs: tuple[str, ...] = DEFAULT_TARGET_GLOBS,
) -> list[tuple[str, int, str, str]]:
    findings: list[tuple[str, int, str, str]] = []
    for path in _target_files(root, target_globs):
        relative = path.relative_to(root).as_posix()
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for pattern, reason in BLOCKED_PATTERNS:
                if pattern.search(line):
                    findings.append((relative, line_number, reason, line.strip()))
    return findings


def _target_files(root: Path, target_globs: tuple[str, ...]) -> list[Path]:
    files: set[Path] = set()
    for target_glob in target_globs:
        files.update(path for path in root.glob(target_glob) if path.is_file())
    return sorted(files)


if __name__ == "__main__":
    sys.exit(main())
