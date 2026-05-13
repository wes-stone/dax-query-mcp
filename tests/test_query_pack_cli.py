from __future__ import annotations

import json
from pathlib import Path

from dax_query_mcp import query_pack_cli


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    exit_code = query_pack_cli.main(args)
    payload = json.loads(capsys.readouterr().out)
    return exit_code, payload


def _write_sales_connection(connections_dir: Path) -> None:
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        "connection_string: |\n"
        "  Provider=MSOLAP.8;\n"
        "  Data Source=localhost:1234;\n"
        "  Initial Catalog=SalesModel\n"
        'description: "Sales model"\n',
        encoding="utf-8",
    )


def test_query_pack_cli_create_add_validate_and_export(capsys, tmp_path: Path) -> None:
    pack_dir = tmp_path / "pack"
    connections_dir = tmp_path / "Connections"
    export_dir = tmp_path / "workspace"
    _write_sales_connection(connections_dir)

    exit_code, payload = _run_cli(
        ["create", "--output-dir", str(pack_dir), "--name", "Revenue Exploration"],
        capsys,
    )
    assert exit_code == 0
    assert payload["ok"] is True
    assert Path(payload["data"]["manifest_path"]).exists()

    exit_code, payload = _run_cli(
        [
            "add-query",
            "--pack-path",
            str(pack_dir),
            "--connection-name",
            "sales",
            "--query",
            'EVALUATE ROW("FY", {{fiscal_year}})',
            "--description",
            "Monthly ARR",
            "--query-id",
            "monthly_arr",
            "--tags",
            "arr,monthly",
            "--parameters-json",
            '{"fiscal_year": {"type": "text", "default": "FY26"}}',
        ],
        capsys,
    )
    assert exit_code == 0
    assert payload["data"]["query_id"] == "monthly_arr"
    assert (pack_dir / "queries" / "monthly_arr.dax").exists()

    exit_code, payload = _run_cli(["list", "--pack-path", str(pack_dir)], capsys)
    assert exit_code == 0
    assert payload["data"]["queries"][0]["id"] == "monthly_arr"

    exit_code, payload = _run_cli(
        [
            "validate",
            "--pack-path",
            str(pack_dir),
            "--connections-dir",
            str(connections_dir),
        ],
        capsys,
    )
    assert exit_code == 0
    assert payload["data"]["valid"] is True

    exit_code, payload = _run_cli(
        [
            "describe",
            "--pack-path",
            str(pack_dir),
            "--connections-dir",
            str(connections_dir),
        ],
        capsys,
    )
    assert exit_code == 0
    assert "## Query Pack: Revenue Exploration" in payload["data"]["markdown"]
    assert "monthly_arr" in payload["data"]["markdown"]

    exit_code, payload = _run_cli(
        [
            "export",
            "--pack-path",
            str(pack_dir),
            "--output-dir",
            str(export_dir),
            "--connections-dir",
            str(connections_dir),
        ],
        capsys,
    )
    assert exit_code == 0
    assert payload["data"]["query_count"] == 1
    assert (export_dir / "pack.yaml").exists()
    assert (export_dir / "run_queries.py").exists()
    assert (export_dir / "streamlit_app.py").exists()
    assert (export_dir / "power_query" / "monthly_arr.pq").exists()


def test_query_pack_cli_duplicate_add_returns_json_error(capsys, tmp_path: Path) -> None:
    pack_dir = tmp_path / "pack"
    assert query_pack_cli.main(["create", "--output-dir", str(pack_dir)]) == 0
    capsys.readouterr()

    args = [
        "add-query",
        "--pack-path",
        str(pack_dir),
        "--connection-name",
        "sales",
        "--query",
        'EVALUATE ROW("A", 1)',
        "--description",
        "duplicate query",
        "--query-id",
        "dup",
    ]
    assert query_pack_cli.main(args) == 0
    capsys.readouterr()

    exit_code, payload = _run_cli(args, capsys)

    assert exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ValueError"
    assert "already exists" in payload["error"]["message"]


def test_query_pack_cli_command_helpers_return_shell_commands(capsys, tmp_path: Path) -> None:
    workspace_dir = tmp_path / "workspace"

    exit_code, payload = _run_cli(
        [
            "run-command",
            "--workspace-dir",
            str(workspace_dir),
            "--only",
            "monthly_arr",
            "--tag",
            "arr",
            "--param",
            "fiscal_year=FY26",
            "--output",
            "results",
            "--format",
            "json",
            "--max-rows",
            "25",
        ],
        capsys,
    )
    assert exit_code == 0
    assert payload["data"]["command"] == [
        "uv",
        "run",
        "run_queries.py",
        "--only",
        "monthly_arr",
        "--tag",
        "arr",
        "--param",
        "fiscal_year=FY26",
        "--output",
        "results",
        "--format",
        "json",
        "--max-rows",
        "25",
    ]
    assert "uv run run_queries.py" in payload["data"]["shell_command"]

    exit_code, payload = _run_cli(
        ["streamlit-command", "--workspace-dir", str(workspace_dir)],
        capsys,
    )
    assert exit_code == 0
    assert payload["data"]["command"] == ["uv", "run", "streamlit", "run", "streamlit_app.py"]
