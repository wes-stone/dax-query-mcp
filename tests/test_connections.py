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
    assert connection.transport == "msolap"
    assert connection.dataset_id is None
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


def test_load_connections_supports_powerbi_rest_without_connection_string(tmp_path: Path) -> None:
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "revenue.yaml").write_text(
        """
transport: powerbi_rest
dataset_id: "00000000-0000-0000-0000-000000000000"
description: "REST-backed semantic model"
auth_mode: env
access_token_env: "TEST_POWERBI_TOKEN"
command_timeout_seconds: 90
""".strip(),
        encoding="utf-8",
    )

    connections = load_connections(connections_dir)
    connection = connections["revenue"]

    assert connection.transport == "powerbi_rest"
    assert connection.connection_string == ""
    assert connection.dataset_id == "00000000-0000-0000-0000-000000000000"
    assert connection.auth_mode == "env"
    assert connection.access_token_env == "TEST_POWERBI_TOKEN"
    assert connection.command_timeout_seconds == 90


def test_load_connections_ignores_data_dictionary_yaml(tmp_path: Path) -> None:
    connections_dir = tmp_path / "Connections"
    connections_dir.mkdir()
    (connections_dir / "sales.yaml").write_text(
        "connection_string: 'MOCK://contoso'\ndescription: 'Sales'\n",
        encoding="utf-8",
    )
    (connections_dir / "sales.data_dictionary.yaml").write_text(
        "version: '1.0'\ntables: []\nmeasures: []\nfilters: []\n",
        encoding="utf-8",
    )

    connections = load_connections(connections_dir)

    assert list(connections.keys()) == ["sales"]
