from __future__ import annotations

import json
from pathlib import Path

from dax_query_mcp import cli


def test_cli_saves_query_builder_artifacts(monkeypatch, capsys, tmp_path: Path) -> None:
    builder_path = tmp_path / "builder.json"
    builder_path.write_text(
        """
{
  "name": "monthly_revenue",
  "connection_name": "example_connection",
  "columns": ["'Calendar'[Fiscal Month]"],
  "measures": [{"caption": "Revenue", "expression": "[Total Revenue]"}]
}
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        cli.sys,
        "argv",
        [
            "dax-query",
            "--save-query-builder-from",
            str(builder_path),
            "--config-dir",
            str(tmp_path / "queries"),
        ],
    )

    exit_code = cli.main()

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert Path(payload["dax_path"]).exists()
    assert Path(payload["query_builder_path"]).exists()
    assert payload["dax_studio_open_path"].endswith("monthly_revenue.dax")
