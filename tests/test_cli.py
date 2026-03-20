from __future__ import annotations

import json

from dax_query_mcp import cli


def test_cli_inspect_connection_prints_metadata(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "inspect_connection_metadata",
        lambda connection_name, **kwargs: {
            "connection_name": connection_name,
            "cubes": {"row_count": 1},
            "kwargs": kwargs,
        },
    )
    monkeypatch.setattr(
        cli.sys,
        "argv",
        [
            "dax-query",
            "--inspect-connection",
            "example_connection",
            "--connections-dir",
            "Connections",
            "--preview-rows",
            "5",
        ],
    )

    exit_code = cli.main()

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["connection_name"] == "example_connection"
    assert payload["cubes"]["row_count"] == 1
    assert payload["kwargs"]["connections_dir"] == "Connections"
    assert payload["kwargs"]["preview_rows"] == 5
