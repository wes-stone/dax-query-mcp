from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any

from .connections import load_connections
from .models import TRANSPORT_POWERBI_REST
from .query_pack import (
    PACK_MANIFEST,
    QueryPack,
    QueryPackEntry,
    power_query_m_from_connection,
    read_query_text,
    render_dax_template,
    load_query_pack,
    resolve_pack_manifest,
    save_query_pack,
)
from .scaffold import (
    build_scaffold_connection_config,
    render_run_queries_script,
    render_streamlit_query_pack_app,
)


MULTI_PYPROJECT_TEMPLATE = textwrap.dedent("""\
    [project]
    name = "{project_name}"
    version = "0.1.0"
    description = "DAX query pack workspace"
    requires-python = ">=3.12"
    dependencies = [
        "ipykernel>=6.29.0",
        "pandas>=2.3.0",
        "pywin32>=310; sys_platform == 'win32'",
        "rich>=13.0.0",
        "streamlit>=1.37.0",
    ]
""")


def build_query_pack_connection_configs(pack: QueryPack, connections_dir: str) -> dict[str, dict[str, Any]]:
    connection_names = sorted({entry.connection_name for entry in pack.queries})
    available_connections = load_connections(connections_dir)
    connections_config: dict[str, dict[str, Any]] = {}
    for connection_name in connection_names:
        connection = available_connections.get(connection_name)
        if connection is None:
            connections_config[connection_name] = build_scaffold_connection_config()
            continue

        connections_config[connection_name] = build_scaffold_connection_config(
            connection_string=connection.connection_string,
            transport=connection.transport,
            dataset_id=connection.dataset_id,
            auth_mode=connection.auth_mode,
            access_token_env=connection.access_token_env,
            api_base_url=connection.api_base_url,
            impersonated_user_name=connection.impersonated_user_name,
            connection_timeout_seconds=connection.connection_timeout_seconds,
            command_timeout_seconds=connection.command_timeout_seconds,
            max_rows=connection.max_rows,
        )
    return connections_config


def query_pack_run_payload(pack: QueryPack) -> list[dict[str, Any]]:
    return [
        {
            "id": entry.id,
            "name": entry.id,
            "display_name": entry.display_name or entry.id,
            "file": entry.file,
            "connection_name": entry.connection_name,
            "connection": entry.connection_name,
            "description": entry.description,
            "tags": list(entry.tags),
            "parameters": {
                name: parameter.to_dict()
                for name, parameter in sorted(entry.parameters.items())
            },
            "outputs": entry.outputs.to_dict(),
        }
        for entry in pack.queries
    ]


def query_text_for_static_artifact(pack_root: str | Path, entry: QueryPackEntry) -> str:
    query_text = read_query_text(pack_root, entry)
    if not entry.parameters:
        return query_text
    return render_dax_template(query_text, entry.parameters, {})


def write_power_query_pack(
    pack: QueryPack,
    pack_root: str | Path,
    output_dir: str | Path,
    connections_dir: str,
) -> list[str]:
    root = Path(pack_root)
    output = Path(output_dir)
    available_connections = load_connections(connections_dir)
    power_query_dir = output / "power_query"
    power_query_dir.mkdir(exist_ok=True)
    created: list[str] = []
    for entry in pack.queries:
        connection = available_connections.get(entry.connection_name)
        target = power_query_dir / f"{entry.id}.pq"
        try:
            query_text = query_text_for_static_artifact(root, entry)
        except ValueError as exc:
            m_code = (
                f"// Power Query M was not generated for '{entry.id}'.\n"
                f"// Reason: {exc}\n"
                "// Add parameter defaults to pack.yaml or run this query through run_queries.py/Streamlit with explicit parameters.\n"
            )
        else:
            if connection is None:
                m_code = (
                    f"// Query pack entry '{entry.id}' references missing connection "
                    f"'{entry.connection_name}'.\n"
                    "// Add the connection and regenerate this Power Query file.\n"
                )
            elif connection.transport == TRANSPORT_POWERBI_REST:
                m_code = (
                    f"// Query pack entry '{entry.id}' uses Power BI REST transport.\n"
                    "// Excel Power Query refresh needs a reliable token/auth story for REST.\n"
                    "// Use run_queries.py, Streamlit, export_to_csv, or copy_to_clipboard for now.\n"
                )
            else:
                try:
                    m_code = power_query_m_from_connection(connection.connection_string, query_text)
                except ValueError as exc:
                    m_code = (
                        f"// Power Query M was not generated for '{entry.id}'.\n"
                        f"// Reason: {exc}\n"
                    )
        target.write_text(m_code, encoding="utf-8")
        created.append(str(target))
    return created


def write_query_pack_artifacts(
    pack: QueryPack,
    pack_root: str | Path,
    output_dir: str | Path,
    connections_dir: str,
    *,
    include_power_query: bool = True,
    include_streamlit: bool = True,
) -> list[str]:
    output = Path(output_dir)
    created: list[str] = []
    connections_config = build_query_pack_connection_configs(pack, connections_dir)
    queries_payload = query_pack_run_payload(pack)

    connections_path = output / "connections.json"
    connections_path.write_text(json.dumps(connections_config, indent=2, default=str), encoding="utf-8")
    created.append(str(connections_path))

    run_script = output / "run_queries.py"
    run_script.write_text(
        render_run_queries_script(
            connections_config=connections_config,
            queries=queries_payload,
        ),
        encoding="utf-8",
    )
    created.append(str(run_script))

    if include_streamlit:
        streamlit_app = output / "streamlit_app.py"
        streamlit_app.write_text(
            render_streamlit_query_pack_app(
                connections_config=connections_config,
                queries=queries_payload,
            ),
            encoding="utf-8",
        )
        created.append(str(streamlit_app))

    if include_power_query:
        created.extend(write_power_query_pack(pack, pack_root, output, connections_dir))

    return created


def write_query_pack_workspace(
    pack: QueryPack,
    pack_root: str | Path,
    output_dir: str | Path,
    connections_dir: str,
    *,
    include_power_query: bool = True,
    include_streamlit: bool = True,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    created = write_query_pack_artifacts(
        pack,
        pack_root,
        output,
        connections_dir,
        include_power_query=include_power_query,
        include_streamlit=include_streamlit,
    )

    safe_project = output.name.replace(" ", "-").lower()
    pyproject = output / "pyproject.toml"
    pyproject.write_text(
        MULTI_PYPROJECT_TEMPLATE.format(project_name=safe_project),
        encoding="utf-8",
    )
    created.append(str(pyproject))

    readme = output / "README.md"
    readme.write_text(query_pack_readme(pack, output), encoding="utf-8")
    created.append(str(readme))

    return {
        "files_created": created,
        "output_dir": str(output),
        "manifest_path": str(output / PACK_MANIFEST),
        "project_name": safe_project,
        "next_steps": f"cd {output} && uv run run_queries.py --list",
    }


def export_query_pack_workspace(
    pack_path: str | Path,
    output_dir: str | Path | None,
    connections_dir: str,
    *,
    include_power_query: bool = True,
    include_streamlit: bool = True,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Copy a pack when needed, then write runnable workspace artifacts."""
    manifest_path = resolve_pack_manifest(pack_path)
    source_root = manifest_path.parent
    pack = load_query_pack(manifest_path)
    destination = Path(output_dir) if output_dir else source_root
    created: list[str] = []

    if output_dir:
        destination.mkdir(parents=True, exist_ok=True)
        copied_pack = QueryPack(
            name=pack.name,
            description=pack.description,
            pack_version=pack.pack_version,
            queries=[
                QueryPackEntry(
                    id=entry.id,
                    display_name=entry.display_name,
                    connection_name=entry.connection_name,
                    file=entry.file,
                    description=entry.description,
                    tags=list(entry.tags),
                    grain=entry.grain,
                    parameters=dict(entry.parameters),
                    outputs=entry.outputs,
                    source=dict(entry.source),
                    query_text=read_query_text(source_root, entry),
                )
                for entry in pack.queries
            ],
        )
        result = save_query_pack(copied_pack, destination, overwrite=overwrite)
        pack = copied_pack
        pack_root = destination
        created.extend(result["files_created"])
    else:
        pack_root = source_root

    workspace_result = write_query_pack_workspace(
        pack,
        pack_root,
        destination,
        connections_dir,
        include_power_query=include_power_query,
        include_streamlit=include_streamlit,
    )
    created.extend(workspace_result["files_created"])
    return {
        **workspace_result,
        "files_created": created,
        "pack_name": pack.name,
        "query_count": len(pack.queries),
    }


def query_pack_readme(pack: QueryPack, output_dir: Path) -> str:
    lines = [
        f"# {pack.name}\n",
        "\nDAX query pack exported by **dax-query-mcp**.\n",
        "\n## Quick start\n",
        "\n```bash\n",
        f"cd {output_dir}\n",
        "uv run run_queries.py --list\n",
        "uv run run_queries.py --output results\n",
        "uv run streamlit run streamlit_app.py\n",
        "\n# Optional: materialize the uv-managed environment before repeated runs\n",
        "uv sync\n",
        "uv run --no-sync run_queries.py --list\n",
        "```\n",
        "\n## Files\n",
        "\n| File | Purpose |",
        "\n|------|---------|",
        "\n| `pack.yaml` | Versioned query-pack manifest |",
        "\n| `connections.json` | Embedded generated connection config for Python runners |",
        "\n| `queries/*.dax` | Saved DAX queries |",
        "\n| `run_queries.py` | Batch runner with `--list`, `--only`, `--tag`, `--param`, `--output`, and result logs |",
        "\n| `pyproject.toml` | uv dependency manifest for the generated Python tools |",
        "\n| `results/` | Default output folder for CSV/JSON results, schema files, and `run_log.json` |",
        "\n| `power_query/*.pq` | Excel Power Query M files or honest auth/connection stubs |",
        "\n| `streamlit_app.py` | Interactive query-pack explorer |",
        "\n\n## Queries\n",
        "\n| ID | Connection | Tags | Description |",
        "\n|----|------------|------|-------------|",
    ]
    for entry in pack.queries:
        lines.append(
            f"\n| {entry.id} | {entry.connection_name} | {', '.join(entry.tags)} | {entry.description} |"
        )
    lines.append(
        "\n\nDo not commit generated workspaces that contain private dataset IDs, "
        "workspace names, connection strings, or tokens.\n"
    )
    return "".join(lines)
