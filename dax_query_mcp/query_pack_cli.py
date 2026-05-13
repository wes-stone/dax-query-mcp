from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Sequence

from .connections import load_connections, resolve_connections_dir
from .query_pack import (
    QueryOutputs,
    QueryPack,
    QueryPackEntry,
    QueryParameter,
    describe_query_pack_markdown,
    load_query_pack,
    query_pack_summary,
    resolve_pack_manifest,
    save_query_pack,
    slugify_query_id,
    validate_query_pack,
)
from .query_pack_export import export_query_pack_workspace


DEFAULT_CONNECTIONS_DIR = str(resolve_connections_dir(os.getenv("DAX_QUERY_MCP_CONNECTIONS_DIR")))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        payload = args.handler(args)
    except Exception as exc:  # noqa: BLE001 - CLI boundary returns stable JSON errors.
        _print_json({
            "ok": False,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        })
        return 1

    _print_json({"ok": True, "data": payload})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create, validate, and export DAX query packs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create", help="Create an empty query pack.")
    create.add_argument("--output-dir", required=True)
    create.add_argument("--name", default="query-pack")
    create.add_argument("--description", default="")
    create.add_argument("--overwrite", action="store_true")
    create.set_defaults(handler=_create)

    add_query = subparsers.add_parser("add-query", help="Add or update one DAX query in a pack.")
    add_query.add_argument("--pack-path", required=True)
    add_query.add_argument("--connection-name", required=True)
    query_source = add_query.add_mutually_exclusive_group(required=True)
    query_source.add_argument("--query")
    query_source.add_argument("--query-file")
    add_query.add_argument("--description", required=True)
    add_query.add_argument("--query-id", default="")
    add_query.add_argument("--display-name", default="")
    add_query.add_argument("--tags", default="")
    add_query.add_argument("--parameters-json", default="")
    add_query.add_argument("--table-name", default="")
    add_query.add_argument("--overwrite", action="store_true")
    add_query.set_defaults(handler=_add_query)

    list_pack = subparsers.add_parser("list", help="List pack metadata and queries.")
    list_pack.add_argument("--pack-path", required=True)
    list_pack.set_defaults(handler=_list)

    validate = subparsers.add_parser("validate", help="Validate a query pack.")
    validate.add_argument("--pack-path", required=True)
    validate.add_argument("--connections-dir", default=DEFAULT_CONNECTIONS_DIR)
    validate.set_defaults(handler=_validate)

    describe = subparsers.add_parser("describe", help="Generate a markdown pack summary.")
    describe.add_argument("--pack-path", required=True)
    describe.add_argument("--connections-dir", default=DEFAULT_CONNECTIONS_DIR)
    describe.add_argument("--no-validation", action="store_true")
    describe.set_defaults(handler=_describe)

    export = subparsers.add_parser("export", help="Export a pack as a runnable workspace.")
    export.add_argument("--pack-path", required=True)
    export.add_argument("--output-dir", default="")
    export.add_argument("--connections-dir", default=DEFAULT_CONNECTIONS_DIR)
    export.add_argument("--no-power-query", action="store_true")
    export.add_argument("--no-streamlit", action="store_true")
    export.add_argument("--overwrite", action="store_true")
    export.set_defaults(handler=_export)

    run_command = subparsers.add_parser("run-command", help="Return the command to run exported pack queries.")
    run_command.add_argument("--workspace-dir", required=True)
    run_command.add_argument("--only", action="append", default=[])
    run_command.add_argument("--tag", action="append", default=[])
    run_command.add_argument("--param", action="append", default=[])
    run_command.add_argument("--output", default="")
    run_command.add_argument("--format", choices=["csv", "json"], default="")
    run_command.add_argument("--max-rows", type=int)
    run_command.add_argument("--continue-on-error", action="store_true")
    run_command.add_argument("--fail-fast", action="store_true")
    run_command.set_defaults(handler=_run_command)

    streamlit_command = subparsers.add_parser("streamlit-command", help="Return the command to open the explorer.")
    streamlit_command.add_argument("--workspace-dir", required=True)
    streamlit_command.set_defaults(handler=_streamlit_command)

    return parser


def _create(args: argparse.Namespace) -> dict[str, Any]:
    pack = QueryPack(name=args.name, description=args.description)
    return save_query_pack(pack, args.output_dir, overwrite=args.overwrite)


def _add_query(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = resolve_pack_manifest(args.pack_path)
    root = manifest_path.parent
    pack = load_query_pack(manifest_path)
    stable_id = slugify_query_id(args.query_id or args.display_name or args.description)
    existing_index = next((index for index, entry in enumerate(pack.queries) if entry.id == stable_id), None)
    if existing_index is not None and not args.overwrite:
        raise ValueError(f"Query id '{stable_id}' already exists. Use --overwrite to replace it.")

    entry = QueryPackEntry(
        id=stable_id,
        display_name=args.display_name or stable_id,
        connection_name=args.connection_name,
        file=f"queries/{stable_id}.dax",
        description=args.description,
        tags=_parse_tags(args.tags),
        parameters=_parse_parameters(args.parameters_json),
        outputs=QueryOutputs(table_name=args.table_name or stable_id),
        query_text=_read_query(args),
    )
    validation = validate_query_pack(QueryPack(name=pack.name, queries=[entry]))
    if not validation["valid"]:
        raise ValueError("; ".join(validation["errors"]))

    if existing_index is None:
        pack.queries.append(entry)
    else:
        pack.queries[existing_index] = entry
    result = save_query_pack(pack, root, overwrite=True)
    return {"query_id": stable_id, **result}


def _list(args: argparse.Namespace) -> dict[str, Any]:
    return query_pack_summary(load_query_pack(args.pack_path))


def _validate(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = resolve_pack_manifest(args.pack_path)
    pack = load_query_pack(manifest_path)
    connection_names = set(load_connections(args.connections_dir).keys())
    return validate_query_pack(pack, pack_root=manifest_path.parent, connection_names=connection_names)


def _describe(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = resolve_pack_manifest(args.pack_path)
    pack = load_query_pack(manifest_path)
    validation: dict[str, Any] | None = None
    if not args.no_validation:
        connection_names = set(load_connections(args.connections_dir).keys())
        validation = validate_query_pack(pack, pack_root=manifest_path.parent, connection_names=connection_names)
    return {
        "pack_path": str(manifest_path.parent),
        "markdown": describe_query_pack_markdown(
            pack,
            validation=validation,
            pack_path=manifest_path.parent,
        ),
        "validation": validation,
    }


def _export(args: argparse.Namespace) -> dict[str, Any]:
    return export_query_pack_workspace(
        args.pack_path,
        args.output_dir or None,
        args.connections_dir,
        include_power_query=not args.no_power_query,
        include_streamlit=not args.no_streamlit,
        overwrite=args.overwrite,
    )


def _run_command(args: argparse.Namespace) -> dict[str, Any]:
    command = ["uv", "run", "run_queries.py"]
    for query_id in args.only:
        command.extend(["--only", query_id])
    for tag in args.tag:
        command.extend(["--tag", tag])
    for parameter in args.param:
        command.extend(["--param", parameter])
    if args.output:
        command.extend(["--output", args.output])
    if args.format:
        command.extend(["--format", args.format])
    if args.max_rows is not None:
        command.extend(["--max-rows", str(args.max_rows)])
    if args.continue_on_error:
        command.append("--continue-on-error")
    if args.fail_fast:
        command.append("--fail-fast")
    return _command_payload(args.workspace_dir, command)


def _streamlit_command(args: argparse.Namespace) -> dict[str, Any]:
    return _command_payload(args.workspace_dir, ["uv", "run", "streamlit", "run", "streamlit_app.py"])


def _command_payload(workspace_dir: str, command: list[str]) -> dict[str, Any]:
    return {
        "workspace_dir": workspace_dir,
        "command": command,
        "shell_command": f"cd {Path(workspace_dir)} && {subprocess.list2cmdline(command)}",
    }


def _read_query(args: argparse.Namespace) -> str:
    if args.query_file:
        return Path(args.query_file).read_text(encoding="utf-8")
    return str(args.query)


def _parse_tags(tags: str) -> list[str]:
    return [tag.strip() for tag in tags.split(",") if tag.strip()]


def _parse_parameters(parameters_json: str) -> dict[str, QueryParameter]:
    if not parameters_json.strip():
        return {}
    raw = json.loads(parameters_json)
    if not isinstance(raw, dict):
        raise ValueError("parameters-json must be a JSON object.")
    return {
        str(name): QueryParameter.from_raw(definition)
        for name, definition in raw.items()
    }


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    sys.exit(main())
