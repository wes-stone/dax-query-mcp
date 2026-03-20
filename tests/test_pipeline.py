from pathlib import Path

import pandas as pd

from dax_query_mcp.models import DAXQueryConfig
from dax_query_mcp.pipeline import DAXPipeline


class StubExecutor:
    def __init__(self, responses):
        self.responses = responses
        self.executed = []

    def execute(self, query: DAXQueryConfig) -> pd.DataFrame:
        self.executed.append(query.name)
        return self.responses[query.name]


def test_pipeline_runs_specific_query_and_exports(tmp_path: Path) -> None:
    config_dir = tmp_path / "queries"
    config_dir.mkdir()
    (config_dir / "sales.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=my-source;
  Initial Catalog=model
dax_query: |
  EVALUATE ROW("Value", 1)
output_filename: "sales_export"
""".strip(),
        encoding="utf-8",
    )
    executor = StubExecutor({"sales": pd.DataFrame({"Value": [1, 2]})})
    pipeline = DAXPipeline(str(config_dir), str(tmp_path / "custom"), executor=executor)

    result = pipeline.run_query("sales", export=True)

    assert result is not None
    assert executor.executed == ["sales"]
    assert next((tmp_path / "custom").glob("export_*\\sales_export.csv")).exists()
    assert next((Path("export")).glob("export_*\\sales_export.csv")).exists()


def test_pipeline_preview_renders_markdown_table(tmp_path: Path, capsys) -> None:
    config_dir = tmp_path / "queries"
    config_dir.mkdir()
    (config_dir / "sales.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=my-source;
  Initial Catalog=model
dax_query: |
  EVALUATE ROW("Value", 1)
""".strip(),
        encoding="utf-8",
    )
    executor = StubExecutor({"sales": pd.DataFrame({"Value": [1, 2], "Label": ["One", "Two"]})})
    pipeline = DAXPipeline(str(config_dir), executor=executor)

    pipeline.run_query("sales", preview=True)

    output = capsys.readouterr().out
    assert "Preview" in output
    assert "Value" in output
    assert "Label" in output

