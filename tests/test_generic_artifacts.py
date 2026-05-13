from pathlib import Path

from scripts.check_generic_artifacts import scan_generic_artifacts


def test_shareable_artifacts_do_not_contain_internal_examples() -> None:
    repo_root = Path(__file__).resolve().parent.parent

    assert scan_generic_artifacts(repo_root) == []


def test_shareable_artifact_scan_blocks_user_specific_paths(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text(
        "Run this from C:\\Users\\alice\\dax-query-mcp or C:\\\\Users\\\\alice\\\\dax-query-mcp.",
        encoding="utf-8",
    )

    findings = scan_generic_artifacts(tmp_path, ("README.md",))

    assert len(findings) == 1
    assert findings[0][2] == "user-specific local path"
