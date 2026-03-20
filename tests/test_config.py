from pathlib import Path

from dax_query_mcp.config import create_sample_config, load_queries


def test_load_queries_supports_single_and_multi_query_files(tmp_path: Path) -> None:
    (tmp_path / "single.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=my-source;
  Initial Catalog=my-model
dax_query: |
  EVALUATE ROW("Value", 1)
description: "single query"
command_timeout_seconds: 90
""".strip(),
        encoding="utf-8",
    )
    (tmp_path / "multi.yaml").write_text(
        """
queries:
  alpha:
    connection_string: |
      Provider=MSOLAP.8;
      Data Source=alpha;
      Initial Catalog=model
    dax_query: |
      EVALUATE ROW("Value", 1)
    max_rows: 10
  beta:
    connection_string: |
      Provider=MSOLAP.8;
      Data Source=beta;
      Initial Catalog=model
    dax_query: |
      EVALUATE ROW("Value", 2)
    output_filename: "beta_export"
""".strip(),
        encoding="utf-8",
    )

    queries = load_queries(tmp_path)

    assert set(queries) == {"single", "alpha", "beta"}
    assert queries["single"].description == "single query"
    assert queries["single"].command_timeout_seconds == 90
    assert queries["alpha"].max_rows == 10
    assert queries["beta"].export_name == "beta_export"


def test_create_sample_config_is_written_once(tmp_path: Path) -> None:
    sample_path = create_sample_config(tmp_path)

    assert sample_path.exists()
    first_contents = sample_path.read_text(encoding="utf-8")

    second_path = create_sample_config(tmp_path)

    assert second_path == sample_path
    assert second_path.read_text(encoding="utf-8") == first_contents

