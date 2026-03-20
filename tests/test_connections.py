from pathlib import Path

from dax_query_mcp.connections import load_connections, resolve_connections_dir


def test_load_connections_reads_yaml_and_markdown_context(tmp_path: Path) -> None:
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sample_model.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/SampleWorkspace?readonly;
  Initial Catalog=SampleSemanticModel
description: "Sample semantic model"
suggested_skill: "enrollment-skills"
suggested_skill_reason: "Use this when you want help drafting KQL for this model."
command_timeout_seconds: 120
""".strip(),
        encoding="utf-8",
    )
    (connections_dir / "sample_model.md").write_text(
        "# Sample Semantic Model\n\nImportant dimensions and measures live here.\n",
        encoding="utf-8",
    )

    connections = load_connections(connections_dir)
    connection = connections["sample_model"]

    assert connection.description == "Sample semantic model"
    assert connection.suggested_skill == "enrollment-skills"
    assert connection.suggested_skill_reason == "Use this when you want help drafting KQL for this model."
    assert connection.command_timeout_seconds == 120
    assert connection.context_markdown.startswith("# Sample Semantic Model")
    assert connection.context_path is not None


def test_resolve_connections_dir_prefers_connections(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "Connections").mkdir()
    (tmp_path / "queries").mkdir()

    resolved = resolve_connections_dir()

    assert resolved.name == "Connections"


def test_load_connections_hides_bundled_sample_by_default(tmp_path: Path) -> None:
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sample_connection.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/SampleWorkspace?readonly;
  Initial Catalog=SampleSemanticModel
description: "Bundled sample connection"
""".strip(),
        encoding="utf-8",
    )
    (connections_dir / "example_connection.yaml").write_text(
        """
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/SampleWorkspace?readonly;
  Initial Catalog=SampleSemanticModel
description: "Example connection"
""".strip(),
        encoding="utf-8",
    )

    connections = load_connections(connections_dir)

    assert list(connections.keys()) == ["example_connection"]

