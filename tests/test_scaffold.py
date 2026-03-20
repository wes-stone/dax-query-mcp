"""Tests for the scaffold module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dax_query_mcp.scaffold import scaffold_workspace


def test_scaffold_creates_all_files(tmp_path: Path) -> None:
    output = tmp_path / "my-project"
    result = scaffold_workspace(
        output,
        query_text="EVALUATE ROW('x', 1)",
        query_name="test-query",
    )
    assert output.exists()
    assert (output / "run_query.py").exists()
    assert (output / "pyproject.toml").exists()
    assert (output / "README.md").exists()
    assert (output / "queries" / "test-query.dax").exists()
    assert result["project_name"] == "my-project"
    assert result["query_filename"] == "test-query.dax"
    assert len(result["files_created"]) == 4


def test_scaffold_from_file(tmp_path: Path) -> None:
    # Create a source .dax file
    src = tmp_path / "source.dax"
    src.write_text("EVALUATE ROW('hello', 42)", encoding="utf-8")

    output = tmp_path / "exported"
    result = scaffold_workspace(output, query_file=str(src))

    query_path = output / "queries" / "source.dax"
    assert query_path.exists()
    assert query_path.read_text(encoding="utf-8") == "EVALUATE ROW('hello', 42)"
    assert result["query_filename"] == "source.dax"


def test_scaffold_copies_sidecar(tmp_path: Path) -> None:
    src = tmp_path / "my.dax"
    src.write_text("EVALUATE ROW('x', 1)", encoding="utf-8")
    sidecar = tmp_path / "my.dax.queryBuilder"
    sidecar.write_text('{"test": true}', encoding="utf-8")

    output = tmp_path / "with-sidecar"
    scaffold_workspace(output, query_file=str(src))

    assert (output / "queries" / "my.dax.queryBuilder").exists()


def test_scaffold_run_query_has_placeholder(tmp_path: Path) -> None:
    output = tmp_path / "proj"
    scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)")

    script = (output / "run_query.py").read_text(encoding="utf-8")
    assert "YOUR_CONNECTION_STRING_HERE" in script
    assert "dax_to_pandas" in script
    assert "_clean_column_name" in script


def test_scaffold_embeds_connection_string(tmp_path: Path) -> None:
    output = tmp_path / "proj"
    scaffold_workspace(
        output,
        query_text="EVALUATE ROW('x', 1)",
        connection_string="Provider=MSOLAP;Data Source=localhost",
    )
    script = (output / "run_query.py").read_text(encoding="utf-8")
    assert "Provider=MSOLAP;Data Source=localhost" in script


def test_scaffold_pyproject_has_project_name(tmp_path: Path) -> None:
    output = tmp_path / "Cool Project"
    scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)")

    toml = (output / "pyproject.toml").read_text(encoding="utf-8")
    assert 'name = "cool-project"' in toml


def test_scaffold_rejects_existing_dir_without_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "exists"
    output.mkdir()
    with pytest.raises(FileExistsError):
        scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)")


def test_scaffold_overwrites_with_flag(tmp_path: Path) -> None:
    output = tmp_path / "exists"
    output.mkdir()
    result = scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)", overwrite=True)
    assert (output / "run_query.py").exists()


def test_scaffold_raises_without_query(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="query_file or query_text"):
        scaffold_workspace(tmp_path / "empty")


def test_scaffold_appends_dax_extension(tmp_path: Path) -> None:
    output = tmp_path / "proj"
    result = scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)", query_name="my-query")
    assert result["query_filename"] == "my-query.dax"


def test_scaffold_multiline_connection_string_produces_valid_python(tmp_path: Path) -> None:
    """Connection strings with newlines must not break the generated script."""
    output = tmp_path / "multiline"
    scaffold_workspace(
        output,
        query_text="EVALUATE ROW('x', 1)",
        connection_string=(
            "Provider=MSOLAP.8;\n"
            "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;\n"
            'Initial Catalog="My Model"'
        ),
    )
    script = (output / "run_query.py").read_text(encoding="utf-8")
    # Must compile without SyntaxError
    compile(script, "run_query.py", "exec")
    # Connection string should be on one line
    assert "\\n" not in script.split("CONNECTION_STRING")[1].split("\n")[0]


def test_scaffold_connection_string_with_quotes(tmp_path: Path) -> None:
    """Double quotes in connection strings must be escaped."""
    output = tmp_path / "quotes"
    scaffold_workspace(
        output,
        query_text="EVALUATE ROW('x', 1)",
        connection_string='Provider=MSOLAP;Initial Catalog="My Model"',
    )
    script = (output / "run_query.py").read_text(encoding="utf-8")
    compile(script, "run_query.py", "exec")
