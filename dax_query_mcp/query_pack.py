from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import yaml

PACK_VERSION = "1.0"
PACK_MANIFEST = "pack.yaml"
PARAM_TYPES = frozenset({"text", "number", "date", "boolean", "list[text]"})
OUTPUT_FORMATS = frozenset({"csv", "json"})
_QUERY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_ADMIN_QUERY_PATTERNS = re.compile(
    r"\bINFO\s*\.|\$SYSTEM\.DISCOVER_|\bDBCC\b|\bALTER\b|\bCREATE\b|\bDELETE\b|\bDROP\b",
    re.IGNORECASE,
)
_SECRET_CONNECTION_KEYS = {
    "password",
    "pwd",
    "user id",
    "userid",
    "effectiveusername",
    "effective user name",
}
_NORMALIZED_SECRET_CONNECTION_KEYS = {key.replace(" ", "") for key in _SECRET_CONNECTION_KEYS}


@dataclass(slots=True)
class QueryParameter:
    type: str = "text"
    default: Any = None
    required: bool = False
    description: str = ""
    allowed_values: list[Any] = field(default_factory=list)

    @classmethod
    def from_raw(cls, raw: Any) -> "QueryParameter":
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError("Query parameter definitions must be objects.")
        parameter_type = str(raw.get("type") or "text")
        if parameter_type not in PARAM_TYPES:
            raise ValueError(f"Unsupported query parameter type: {parameter_type}")
        allowed_values = raw.get("allowed_values") or []
        if not isinstance(allowed_values, list):
            raise ValueError("allowed_values must be a list.")
        return cls(
            type=parameter_type,
            default=raw.get("default"),
            required=bool(raw.get("required", False)),
            description=str(raw.get("description") or ""),
            allowed_values=allowed_values,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type}
        if self.default is not None:
            payload["default"] = self.default
        if self.required:
            payload["required"] = self.required
        if self.description:
            payload["description"] = self.description
        if self.allowed_values:
            payload["allowed_values"] = list(self.allowed_values)
        return payload


@dataclass(slots=True)
class QueryOutputs:
    default_format: str = "csv"
    table_name: str = ""

    @classmethod
    def from_raw(cls, raw: Any) -> "QueryOutputs":
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError("outputs must be an object.")
        default_format = str(raw.get("default_format") or "csv")
        if default_format not in OUTPUT_FORMATS:
            raise ValueError(f"Unsupported output format: {default_format}")
        return cls(
            default_format=default_format,
            table_name=str(raw.get("table_name") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"default_format": self.default_format}
        if self.table_name:
            payload["table_name"] = self.table_name
        return payload


@dataclass(slots=True)
class QueryPackEntry:
    id: str
    connection_name: str
    file: str
    display_name: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    grain: str = ""
    parameters: dict[str, QueryParameter] = field(default_factory=dict)
    outputs: QueryOutputs = field(default_factory=QueryOutputs)
    source: dict[str, Any] = field(default_factory=dict)
    query_text: str = ""

    @classmethod
    def from_raw(cls, raw: Any) -> "QueryPackEntry":
        if not isinstance(raw, dict):
            raise ValueError("Each query pack entry must be an object.")
        query_id = str(raw.get("id") or "").strip()
        if not query_id:
            raise ValueError("Query pack entries require id.")
        connection_name = str(raw.get("connection_name") or raw.get("connection") or "").strip()
        if not connection_name:
            raise ValueError(f"Query pack entry '{query_id}' requires connection_name.")
        tags = raw.get("tags") or []
        if isinstance(tags, str):
            tags = [tag.strip() for tag in tags.split(",") if tag.strip()]
        if not isinstance(tags, list):
            raise ValueError(f"Query pack entry '{query_id}' tags must be a list.")
        raw_parameters = raw.get("parameters") or {}
        if not isinstance(raw_parameters, dict):
            raise ValueError(f"Query pack entry '{query_id}' parameters must be an object.")
        source = raw.get("source") or {}
        if not isinstance(source, dict):
            raise ValueError(f"Query pack entry '{query_id}' source must be an object.")
        return cls(
            id=query_id,
            display_name=str(raw.get("display_name") or raw.get("name") or query_id),
            connection_name=connection_name,
            file=str(raw.get("file") or f"queries/{query_id}.dax"),
            description=str(raw.get("description") or ""),
            tags=[str(tag) for tag in tags],
            grain=str(raw.get("grain") or ""),
            parameters={
                str(name): QueryParameter.from_raw(definition)
                for name, definition in raw_parameters.items()
            },
            outputs=QueryOutputs.from_raw(raw.get("outputs")),
            source=source,
            query_text=str(raw.get("query_text") or raw.get("query") or ""),
        )

    def to_dict(self, *, include_query_text: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.id,
            "display_name": self.display_name or self.id,
            "connection_name": self.connection_name,
            "file": self.file,
        }
        if self.description:
            payload["description"] = self.description
        if self.tags:
            payload["tags"] = list(self.tags)
        if self.grain:
            payload["grain"] = self.grain
        if self.parameters:
            payload["parameters"] = {
                name: parameter.to_dict()
                for name, parameter in sorted(self.parameters.items())
            }
        if self.outputs != QueryOutputs():
            payload["outputs"] = self.outputs.to_dict()
        if self.source:
            payload["source"] = dict(self.source)
        if include_query_text and self.query_text:
            payload["query_text"] = self.query_text
        return payload


@dataclass(slots=True)
class QueryPack:
    name: str
    description: str = ""
    pack_version: str = PACK_VERSION
    queries: list[QueryPackEntry] = field(default_factory=list)

    @classmethod
    def from_raw(cls, raw: Any) -> "QueryPack":
        if not isinstance(raw, dict):
            raise ValueError("Query pack manifest must be an object.")
        queries = raw.get("queries") or []
        if not isinstance(queries, list):
            raise ValueError("queries must be a list.")
        return cls(
            pack_version=str(raw.get("pack_version") or PACK_VERSION),
            name=str(raw.get("name") or "query-pack"),
            description=str(raw.get("description") or ""),
            queries=[QueryPackEntry.from_raw(entry) for entry in queries],
        )

    def to_dict(self, *, include_query_text: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "pack_version": self.pack_version,
            "name": self.name,
            "queries": [
                entry.to_dict(include_query_text=include_query_text)
                for entry in self.queries
            ],
        }
        if self.description:
            payload["description"] = self.description
        return payload


def slugify_query_id(text: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", text.strip().lower())
    slug = re.sub(r"[\s_-]+", "_", slug).strip("_")
    return slug or "query"


def load_query_pack(path: str | Path) -> QueryPack:
    manifest_path = resolve_pack_manifest(path)
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    return QueryPack.from_raw(raw)


def save_query_pack(pack: QueryPack, output_dir: str | Path, *, overwrite: bool = True) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    queries_dir = output / "queries"
    queries_dir.mkdir(exist_ok=True)

    files_created: list[str] = []
    normalized_entries: list[QueryPackEntry] = []
    seen_ids: set[str] = set()
    for entry in pack.queries:
        if entry.id in seen_ids:
            raise ValueError(f"Duplicate query id: {entry.id}")
        seen_ids.add(entry.id)
        if not entry.file:
            entry.file = f"queries/{entry.id}.dax"
        file_path = safe_pack_relative_path(entry.file)
        target = output / entry.file
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and not overwrite and entry.query_text:
            raise FileExistsError(f"Query file already exists: {target}")
        if entry.query_text:
            target.write_text(entry.query_text, encoding="utf-8")
            files_created.append(str(target))
        normalized_entries.append(entry)

    pack.queries = normalized_entries
    manifest_path = output / PACK_MANIFEST
    if manifest_path.exists() and not overwrite:
        raise FileExistsError(f"Query pack manifest already exists: {manifest_path}")
    manifest_path.write_text(
        yaml.safe_dump(pack.to_dict(), sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    files_created.append(str(manifest_path))
    return {
        "output_dir": str(output),
        "manifest_path": str(manifest_path),
        "query_count": len(pack.queries),
        "files_created": files_created,
    }


def resolve_pack_manifest(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_dir():
        candidate = candidate / PACK_MANIFEST
    if not candidate.exists():
        raise FileNotFoundError(f"Query pack manifest not found: {candidate}")
    return candidate


def read_query_text(pack_root: str | Path, entry: QueryPackEntry) -> str:
    if entry.query_text:
        return entry.query_text
    query_path = Path(pack_root) / safe_pack_relative_path(entry.file)
    if not query_path.exists():
        raise FileNotFoundError(f"Query file not found for '{entry.id}': {query_path}")
    return query_path.read_text(encoding="utf-8")


def safe_pack_relative_path(path: str | Path) -> Path:
    relative = Path(path)
    if relative.is_absolute() or any(part == ".." for part in relative.parts):
        raise ValueError(f"Query pack paths must stay inside the pack: {path}")
    if not str(relative):
        raise ValueError("Query pack path cannot be empty.")
    return relative


def render_dax_template(
    dax_template: str,
    parameter_defs: dict[str, QueryParameter],
    parameter_values: dict[str, Any] | None = None,
) -> str:
    values = parameter_values or {}
    placeholders = set(_PLACEHOLDER_RE.findall(dax_template))
    missing_defs = sorted(placeholders.difference(parameter_defs))
    if missing_defs:
        raise ValueError(f"Undeclared query parameter(s): {', '.join(missing_defs)}")

    def replacement(match: re.Match[str]) -> str:
        name = match.group(1)
        definition = parameter_defs[name]
        if name in values:
            value = values[name]
        elif definition.default is not None:
            value = definition.default
        elif definition.required:
            raise ValueError(f"Missing required query parameter: {name}")
        else:
            value = ""
        return render_dax_literal(value, definition)

    return _PLACEHOLDER_RE.sub(replacement, dax_template)


def render_dax_literal(value: Any, definition: QueryParameter) -> str:
    if definition.type == "text":
        _validate_allowed_value(value, definition.allowed_values)
        return dax_string_literal("" if value is None else str(value))
    if definition.type == "number":
        _validate_allowed_value(value, definition.allowed_values)
        if isinstance(value, bool):
            raise ValueError("Boolean values are not valid number parameters.")
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid number parameter: {value!r}") from exc
        return str(int(number)) if number.is_integer() else str(number)
    if definition.type == "date":
        _validate_allowed_value(value, definition.allowed_values)
        return dax_date_literal(value)
    if definition.type == "boolean":
        _validate_allowed_value(value, definition.allowed_values)
        return "TRUE()" if parse_bool(value) else "FALSE()"
    if definition.type == "list[text]":
        if isinstance(value, str):
            values = [item.strip() for item in value.split(",") if item.strip()]
        elif isinstance(value, list):
            values = value
        else:
            raise ValueError("list[text] parameters must be lists or comma-delimited strings.")
        for item in values:
            _validate_allowed_value(item, definition.allowed_values)
        return "{" + ", ".join(dax_string_literal(str(item)) for item in values) + "}"
    raise ValueError(f"Unsupported query parameter type: {definition.type}")


def _validate_allowed_value(value: Any, allowed_values: list[Any]) -> None:
    if allowed_values and value not in allowed_values:
        raise ValueError(f"Parameter value {value!r} is not in allowed_values.")


def dax_string_literal(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def dax_date_literal(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value).date()
        except ValueError as exc:
            raise ValueError(f"Date parameters must use ISO format YYYY-MM-DD: {value!r}") from exc
    else:
        raise ValueError(f"Invalid date parameter: {value!r}")
    return f"DATE({parsed.year}, {parsed.month}, {parsed.day})"


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    raise ValueError(f"Invalid boolean parameter: {value!r}")


def validate_query_pack(
    pack: QueryPack,
    *,
    pack_root: str | Path | None = None,
    connection_names: set[str] | None = None,
    dax_validator: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    seen_ids: set[str] = set()

    if pack.pack_version != PACK_VERSION:
        warnings.append(f"Pack version {pack.pack_version!r} differs from supported {PACK_VERSION!r}.")
    if not pack.name.strip():
        errors.append("Pack name is required.")

    for entry in pack.queries:
        if not _QUERY_ID_RE.match(entry.id):
            errors.append(f"Query id '{entry.id}' must contain only letters, numbers, underscores, or hyphens.")
        if entry.id in seen_ids:
            errors.append(f"Duplicate query id: {entry.id}")
        seen_ids.add(entry.id)
        if connection_names is not None and entry.connection_name not in connection_names:
            errors.append(f"Query '{entry.id}' references unknown connection '{entry.connection_name}'.")
        if entry.outputs.default_format not in OUTPUT_FORMATS:
            errors.append(
                f"Query '{entry.id}' has unsupported output format '{entry.outputs.default_format}'."
            )
        for parameter_name, parameter in entry.parameters.items():
            if parameter.type not in PARAM_TYPES:
                errors.append(
                    f"Query '{entry.id}' parameter '{parameter_name}' has unsupported type '{parameter.type}'."
                )
                continue
            if parameter.default is not None:
                try:
                    render_dax_literal(parameter.default, parameter)
                except ValueError as exc:
                    errors.append(
                        f"Query '{entry.id}' parameter '{parameter_name}' has invalid default: {exc}"
                    )
        if not entry.file:
            errors.append(f"Query '{entry.id}' is missing file.")
        else:
            try:
                safe_pack_relative_path(entry.file)
            except ValueError as exc:
                errors.append(f"Query '{entry.id}' has invalid file path: {exc}")

        query_text = entry.query_text
        if pack_root is not None and not query_text:
            try:
                query_path = Path(pack_root) / safe_pack_relative_path(entry.file)
            except ValueError:
                query_path = None
            if query_path is None:
                pass
            elif not query_path.exists():
                errors.append(f"Query '{entry.id}' file not found: {entry.file}")
            else:
                query_text = query_path.read_text(encoding="utf-8")
        if query_text:
            if "EVALUATE" not in query_text.upper():
                errors.append(f"Query '{entry.id}' should contain an EVALUATE statement.")
            if _ADMIN_QUERY_PATTERNS.search(query_text):
                errors.append(f"Query '{entry.id}' contains a blocked admin/DDL pattern.")
            placeholders = set(_PLACEHOLDER_RE.findall(query_text))
            missing_parameters = sorted(placeholders.difference(entry.parameters))
            if missing_parameters:
                errors.append(
                    f"Query '{entry.id}' references undeclared parameter(s): {', '.join(missing_parameters)}"
                )
            if dax_validator is not None:
                try:
                    dax_validator(query_text)
                except Exception as exc:  # noqa: BLE001 - validator exception is converted to validation detail.
                    errors.append(f"Query '{entry.id}' failed DAX validation: {exc}")

    return {
        "valid": not errors,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "query_count": len(pack.queries),
    }


def parse_connection_string(connection_string: str) -> dict[str, str]:
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    i = 0
    while i < len(connection_string):
        char = connection_string[i]
        if char == '"':
            if in_quotes and i + 1 < len(connection_string) and connection_string[i + 1] == '"':
                current.append('"')
                i += 2
                continue
            in_quotes = not in_quotes
            i += 1
            continue
        if char == ";" and not in_quotes:
            parts.append("".join(current).strip())
            current = []
            i += 1
            continue
        current.append(char)
        i += 1
    if current:
        parts.append("".join(current).strip())

    parsed: dict[str, str] = {}
    for part in parts:
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        parsed[key.strip().lower()] = value.strip()
    return parsed


def power_query_m_from_connection(connection_string: str, dax_query: str) -> str:
    parsed = parse_connection_string(connection_string)
    secret_keys = sorted(key for key in parsed if key.replace(" ", "") in _NORMALIZED_SECRET_CONNECTION_KEYS)
    if secret_keys:
        raise ValueError(
            "Refusing to generate shareable Power Query M from a connection string "
            f"containing secret or impersonation properties: {', '.join(secret_keys)}"
        )
    server = parsed.get("data source") or parsed.get("server")
    database = parsed.get("initial catalog") or parsed.get("catalog") or parsed.get("database")
    if not server or not database:
        raise ValueError("Power Query generation requires Data Source and Initial Catalog in the MSOLAP connection.")
    return power_query_m_from_server_database(server, database, dax_query)


def power_query_m_from_server_database(server: str, database: str, dax_query: str) -> str:
    return (
        "let\n"
        "    Source = AnalysisServices.Database(\n"
        f"        {m_string_literal(server)},\n"
        f"        {m_string_literal(database)},\n"
        f"        [Query = {m_string_literal(dax_query)}]\n"
        "    )\n"
        "in\n"
        "    Source"
    )


def m_string_literal(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def query_pack_summary(pack: QueryPack) -> dict[str, Any]:
    return {
        "name": pack.name,
        "description": pack.description,
        "pack_version": pack.pack_version,
        "query_count": len(pack.queries),
        "queries": [
            {
                "id": entry.id,
                "display_name": entry.display_name or entry.id,
                "connection_name": entry.connection_name,
                "description": entry.description,
                "tags": entry.tags,
                "file": entry.file,
                "parameters": sorted(entry.parameters),
            }
            for entry in pack.queries
        ],
    }


def describe_query_pack_markdown(
    pack: QueryPack,
    *,
    validation: dict[str, Any] | None = None,
    pack_path: str | Path | None = None,
) -> str:
    """Render a shareable markdown summary of a query pack."""
    lines = [
        f"## Query Pack: {pack.name}",
        "",
        pack.description or "Reusable DAX query pack.",
        "",
        "## Summary",
        "",
        f"- Pack version: `{pack.pack_version}`",
        f"- Queries: {len(pack.queries)}",
    ]
    if pack_path is not None:
        lines.append(f"- Pack path: `{pack_path}`")
    if validation is not None:
        status = "passed" if validation.get("valid") else "failed"
        lines.extend([
            f"- Validation: {status}",
            f"- Validation errors: {validation.get('error_count', 0)}",
            f"- Validation warnings: {validation.get('warning_count', 0)}",
        ])

    all_tags = sorted({tag for entry in pack.queries for tag in entry.tags})
    if all_tags:
        lines.extend(["", "## Tags", "", ", ".join(f"`{tag}`" for tag in all_tags)])

    lines.extend([
        "",
        "## Queries",
        "",
        "| ID | Display name | Connection | Tags | Parameters | Output table | Description |",
        "|----|--------------|------------|------|------------|--------------|-------------|",
    ])
    for entry in pack.queries:
        output_table = entry.outputs.table_name or entry.id
        parameter_names = ", ".join(sorted(entry.parameters))
        lines.append(
            "| "
            + " | ".join([
                entry.id,
                entry.display_name or entry.id,
                entry.connection_name,
                ", ".join(entry.tags),
                parameter_names,
                output_table,
                entry.description,
            ])
            + " |"
        )

    parameter_entries = [
        (entry, name, parameter)
        for entry in pack.queries
        for name, parameter in sorted(entry.parameters.items())
    ]
    if parameter_entries:
        lines.extend([
            "",
            "## Parameters",
            "",
            "| Query | Parameter | Type | Required | Default | Description |",
            "|-------|-----------|------|----------|---------|-------------|",
        ])
        for entry, name, parameter in parameter_entries:
            default = "" if parameter.default is None else str(parameter.default)
            lines.append(
                "| "
                + " | ".join([
                    entry.id,
                    name,
                    parameter.type,
                    "yes" if parameter.required else "no",
                    default,
                    parameter.description,
                ])
                + " |"
            )

    lines.extend([
        "",
        "## Run and share",
        "",
        "```bash",
        "dax-query-pack validate <pack-folder>",
        "dax-query-pack export <pack-folder> --output <workspace-folder>",
        "cd <workspace-folder>",
        "uv run run_queries.py --list",
        "uv run run_queries.py --output results",
        "uv run streamlit run streamlit_app.py",
        "```",
        "",
        "Do not share generated workspaces that contain private dataset IDs, workspace names, connection strings, or tokens.",
    ])
    return "\n".join(lines) + "\n"


def write_json(path: str | Path, payload: Any) -> None:
    Path(path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
