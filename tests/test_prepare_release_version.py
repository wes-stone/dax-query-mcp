from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "prepare_release_version.py"
SPEC = importlib.util.spec_from_file_location("prepare_release_version", SCRIPT_PATH)
assert SPEC is not None
prepare_release_version = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = prepare_release_version
SPEC.loader.exec_module(prepare_release_version)


def test_choose_release_version_uses_local_when_package_is_new() -> None:
    assert prepare_release_version.choose_release_version("0.2.1", None) == "0.2.1"


def test_choose_release_version_uses_local_when_local_is_newer() -> None:
    assert prepare_release_version.choose_release_version("0.3.0", "0.2.9") == "0.3.0"


def test_choose_release_version_bumps_patch_when_versions_match() -> None:
    assert prepare_release_version.choose_release_version("0.2.1", "0.2.1") == "0.2.2"


def test_choose_release_version_bumps_from_pypi_when_pypi_is_newer() -> None:
    assert prepare_release_version.choose_release_version("0.2.1", "0.2.4") == "0.2.5"


def test_choose_release_version_rejects_non_simple_versions() -> None:
    with pytest.raises(ValueError, match="major.minor.patch"):
        prepare_release_version.choose_release_version("0.2.1.dev1", "0.2.1")


def test_replace_project_version_updates_only_project_table() -> None:
    original = """[build-system]
requires = ["setuptools>=69"]
version = "99.99.99"

[project]
name = "demo"
version = "0.2.1"

[tool.example]
version = "1.0.0"
"""

    updated = prepare_release_version.replace_project_version(original, "0.2.2")

    assert 'version = "99.99.99"' in updated
    assert 'version = "0.2.2"' in updated
    assert 'version = "1.0.0"' in updated
    assert 'version = "0.2.1"' not in updated


def test_read_project_metadata(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """[project]
name = "demo"
version = "0.1.0"
""",
        encoding="utf-8",
    )

    metadata = prepare_release_version.read_project_metadata(pyproject)

    assert metadata.name == "demo"
    assert metadata.version == "0.1.0"
