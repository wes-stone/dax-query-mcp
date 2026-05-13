from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from .query_pack import (
    QueryOutputs,
    QueryParameter,
    render_dax_template,
    safe_pack_relative_path,
    slugify_query_id,
)

LIBRARY_VERSION = "1.0"
VALIDATED_QUERY_LIBRARY_SUFFIX = ".validated_queries"
VALIDATION_STATUSES = frozenset({"draft", "validated", "failed", "stale"})
_QUERY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_PATH_SEPARATORS = {"/", "\\"}


@dataclass(slots=True)
class ValidationRecord:
    status: str = "draft"
    validated_at: str = ""
    row_count: int | None = None
    columns: list[str] = field(default_factory=list)
    error: str = ""
    max_rows: int | None = None
    transport: str = ""
    rendered_dax_hash: str = ""

    @classmethod
    def from_raw(cls, raw: Any) -> "ValidationRecord":
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError("validation must be an object.")
        status = str(raw.get("status") or "draft")
        if status not in VALIDATION_STATUSES:
            raise ValueError(f"Unsupported validation status: {status}")
        columns = raw.get("columns") or []
        if not isinstance(columns, list):
            raise ValueError("validation.columns must be a list.")
        row_count = raw.get("row_count")
        max_rows = raw.get("max_rows")
        return cls(
            status=status,
            validated_at=str(raw.get("validated_at") or ""),
            row_count=int(row_count) if row_count is not None else None,
            columns=[str(column) for column in columns],
            error=str(raw.get("error") or ""),
            max_rows=int(max_rows) if max_rows is not None else None,
            transport=str(raw.get("transport") or ""),
            rendered_dax_hash=str(raw.get("rendered_dax_hash") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"status": self.status}
        if self.validated_at:
            payload["validated_at"] = self.validated_at
        if self.row_count is not None:
            payload["row_count"] = self.row_count
        if self.columns:
            payload["columns"] = list(self.columns)
        if self.error:
            payload["error"] = self.error
        if self.max_rows is not None:
            payload["max_rows"] = self.max_rows
        if self.transport:
            payload["transport"] = self.transport
        if self.rendered_dax_hash:
            payload["rendered_dax_hash"] = self.rendered_dax_hash
        return payload


@dataclass(slots=True)
class ValidatedQueryEntry:
    id: str
    connection_name: str
    file: str
    display_name: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    grain: str = ""
    parameters: dict[str, QueryParameter] = field(default_factory=dict)
    sample_parameters: dict[str, Any] = field(default_factory=dict)
    outputs: QueryOutputs = field(default_factory=QueryOutputs)
    validation: ValidationRecord = field(default_factory=ValidationRecord)
    source: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""
    query_text: str = ""

    @classmethod
    def from_raw(cls, raw: Any) -> "ValidatedQueryEntry":
        if not isinstance(raw, dict):
            raise ValueError("Validated query metadata must be an object.")
        query_id = str(raw.get("id") or "").strip()
        if not query_id:
            raise ValueError("Validated query metadata requires id.")
        connection_name = str(raw.get("connection_name") or "").strip()
        if not connection_name:
            raise ValueError(f"Validated query '{query_id}' requires connection_name.")
        tags = raw.get("tags") or []
        if isinstance(tags, str):
            tags = [tag.strip() for tag in tags.split(",") if tag.strip()]
        if not isinstance(tags, list):
            raise ValueError(f"Validated query '{query_id}' tags must be a list.")
        raw_parameters = raw.get("parameters") or {}
        if not isinstance(raw_parameters, dict):
            raise ValueError(f"Validated query '{query_id}' parameters must be an object.")
        sample_parameters = raw.get("sample_parameters") or raw.get("validation_parameters") or {}
        if not isinstance(sample_parameters, dict):
            raise ValueError(f"Validated query '{query_id}' sample_parameters must be an object.")
        source = raw.get("source") or {}
        if not isinstance(source, dict):
            raise ValueError(f"Validated query '{query_id}' source must be an object.")
        return cls(
            id=query_id,
            display_name=str(raw.get("display_name") or raw.get("name") or query_id),
            connection_name=connection_name,
            file=str(raw.get("file") or f"{query_id}.dax"),
            description=str(raw.get("description") or ""),
            tags=[str(tag) for tag in tags],
            grain=str(raw.get("grain") or ""),
            parameters={
                str(name): QueryParameter.from_raw(definition)
                for name, definition in raw_parameters.items()
            },
            sample_parameters=dict(sample_parameters),
            outputs=QueryOutputs.from_raw(raw.get("outputs")),
            validation=ValidationRecord.from_raw(raw.get("validation")),
            source=source,
            created_at=str(raw.get("created_at") or ""),
            updated_at=str(raw.get("updated_at") or ""),
            query_text=str(raw.get("query_text") or raw.get("query") or ""),
        )

    def to_dict(self, *, include_query_text: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "version": LIBRARY_VERSION,
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
        if self.sample_parameters:
            payload["sample_parameters"] = dict(self.sample_parameters)
        if self.outputs != QueryOutputs():
            payload["outputs"] = self.outputs.to_dict()
        if self.validation != ValidationRecord():
            payload["validation"] = self.validation.to_dict()
        if self.source:
            payload["source"] = dict(self.source)
        if self.created_at:
            payload["created_at"] = self.created_at
        if self.updated_at:
            payload["updated_at"] = self.updated_at
        if include_query_text and self.query_text:
            payload["query_text"] = self.query_text
        return payload


def validated_query_library_dir(connections_dir: str | Path, connection_name: str) -> Path:
    _validate_connection_name(connection_name)
    return Path(connections_dir) / f"{connection_name}{VALIDATED_QUERY_LIBRARY_SUFFIX}"


def load_validated_query_library(
    connections_dir: str | Path,
    connection_name: str,
    *,
    include_query_text: bool = False,
) -> list[ValidatedQueryEntry]:
    library_dir = validated_query_library_dir(connections_dir, connection_name)
    if not library_dir.exists():
        return []
    entries: list[ValidatedQueryEntry] = []
    for metadata_path in sorted(library_dir.glob("*.yaml")):
        entry = load_validated_query_entry(metadata_path, library_dir=library_dir)
        if include_query_text:
            entry.query_text = read_validated_query_text(library_dir, entry)
        entry.validation = current_validation_record(entry)
        entries.append(entry)
    return entries


def load_validated_query_entry(
    metadata_path: str | Path,
    *,
    library_dir: str | Path | None = None,
) -> ValidatedQueryEntry:
    path = Path(metadata_path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    entry = ValidatedQueryEntry.from_raw(raw)
    root = Path(library_dir) if library_dir is not None else path.parent
    if not entry.query_text:
        entry.query_text = read_validated_query_text(root, entry)
    entry.validation = current_validation_record(entry)
    return entry


def save_validated_query_entry(
    entry: ValidatedQueryEntry,
    connections_dir: str | Path,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    stable_id = slugify_query_id(entry.id or entry.display_name or entry.description)
    _validate_query_id(stable_id)
    entry.id = stable_id
    entry.file = f"{stable_id}.dax"
    if not entry.display_name:
        entry.display_name = stable_id
    if not entry.query_text.strip():
        raise ValueError("Validated query entries require query_text.")

    now = _utc_now()
    if not entry.created_at:
        entry.created_at = now
    entry.updated_at = now
    entry.validation = _normalize_saved_validation(entry)

    library_dir = validated_query_library_dir(connections_dir, entry.connection_name)
    library_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = library_dir / f"{stable_id}.yaml"
    query_path = library_dir / entry.file
    if not overwrite and (metadata_path.exists() or query_path.exists()):
        raise FileExistsError(f"Validated query '{stable_id}' already exists.")

    query_path.write_text(entry.query_text, encoding="utf-8")
    metadata_path.write_text(
        yaml.safe_dump(entry.to_dict(), sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return {
        "connection_name": entry.connection_name,
        "query_id": stable_id,
        "library_dir": str(library_dir),
        "metadata_path": str(metadata_path),
        "query_path": str(query_path),
    }


def read_validated_query_text(library_dir: str | Path, entry: ValidatedQueryEntry) -> str:
    query_path = Path(library_dir) / safe_pack_relative_path(entry.file)
    if not query_path.exists():
        raise FileNotFoundError(f"Validated query file not found for '{entry.id}': {query_path}")
    return query_path.read_text(encoding="utf-8")


def find_validated_query_entry(
    connections_dir: str | Path,
    connection_name: str,
    query_id: str,
) -> ValidatedQueryEntry:
    stable_id = slugify_query_id(query_id)
    metadata_path = validated_query_library_dir(connections_dir, connection_name) / f"{stable_id}.yaml"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Validated query '{stable_id}' not found for connection '{connection_name}'.")
    return load_validated_query_entry(metadata_path)


def summarize_validated_query_entry(
    entry: ValidatedQueryEntry,
    *,
    include_query_text: bool = False,
) -> dict[str, Any]:
    validation = current_validation_record(entry)
    payload: dict[str, Any] = {
        "id": entry.id,
        "display_name": entry.display_name or entry.id,
        "connection_name": entry.connection_name,
        "description": entry.description,
        "tags": list(entry.tags),
        "grain": entry.grain,
        "parameters": sorted(entry.parameters),
        "sample_parameters": dict(entry.sample_parameters),
        "outputs": entry.outputs.to_dict(),
        "validation": validation.to_dict(),
        "created_at": entry.created_at,
        "updated_at": entry.updated_at,
    }
    if include_query_text:
        payload["query"] = entry.query_text
    return payload


def search_validated_query_entries(
    entries: list[ValidatedQueryEntry],
    search_term: str = "",
    *,
    tags: list[str] | None = None,
    max_results: int = 20,
    include_query_text: bool = True,
) -> list[dict[str, Any]]:
    requested_tags = {tag.lower() for tag in tags or []}
    term_lower = search_term.lower().strip()
    matches: list[tuple[tuple[int, str], dict[str, Any]]] = []
    for entry in entries:
        entry_tags = {tag.lower() for tag in entry.tags}
        if requested_tags and not requested_tags.issubset(entry_tags):
            continue
        haystacks = [
            entry.id,
            entry.display_name,
            entry.description,
            entry.grain,
            " ".join(entry.tags),
            entry.query_text,
        ]
        if term_lower and not any(term_lower in value.lower() for value in haystacks if value):
            continue
        matches.append((
            _search_relevance(entry, term_lower),
            summarize_validated_query_entry(entry, include_query_text=include_query_text),
        ))
    matches.sort(key=lambda item: item[0])
    return [payload for _, payload in matches[:max_results]]


def rendered_dax_hash(query: str) -> str:
    return sha256(query.encode("utf-8")).hexdigest()


def current_validation_record(entry: ValidatedQueryEntry) -> ValidationRecord:
    validation = entry.validation
    if not validation.rendered_dax_hash or validation.status == "draft":
        return validation
    try:
        rendered_query = render_validated_query(entry)
    except ValueError:
        return ValidationRecord(
            status="stale",
            validated_at=validation.validated_at,
            row_count=validation.row_count,
            columns=list(validation.columns),
            error=validation.error or "Cannot render query with saved sample parameters.",
            max_rows=validation.max_rows,
            transport=validation.transport,
            rendered_dax_hash=validation.rendered_dax_hash,
        )
    if rendered_dax_hash(rendered_query) == validation.rendered_dax_hash:
        return validation
    return ValidationRecord(
        status="stale",
        validated_at=validation.validated_at,
        row_count=validation.row_count,
        columns=list(validation.columns),
        error="Saved DAX no longer matches the last validated rendered hash.",
        max_rows=validation.max_rows,
        transport=validation.transport,
        rendered_dax_hash=validation.rendered_dax_hash,
    )


def render_validated_query(
    entry: ValidatedQueryEntry,
    parameter_values: dict[str, Any] | None = None,
) -> str:
    values = dict(entry.sample_parameters)
    if parameter_values:
        values.update(parameter_values)
    return render_dax_template(entry.query_text, entry.parameters, values)


def validation_record_from_result(
    *,
    rendered_query: str,
    row_count: int,
    columns: list[str],
    max_rows: int,
    transport: str,
) -> ValidationRecord:
    return ValidationRecord(
        status="validated",
        validated_at=_utc_now(),
        row_count=row_count,
        columns=columns,
        max_rows=max_rows,
        transport=transport,
        rendered_dax_hash=rendered_dax_hash(rendered_query),
    )


def failed_validation_record(
    *,
    rendered_query: str = "",
    error: str,
    max_rows: int,
    transport: str = "",
) -> ValidationRecord:
    return ValidationRecord(
        status="failed",
        validated_at=_utc_now(),
        error=error,
        max_rows=max_rows,
        transport=transport,
        rendered_dax_hash=rendered_dax_hash(rendered_query) if rendered_query else "",
    )


def update_validation_record(
    entry: ValidatedQueryEntry,
    connections_dir: str | Path,
    validation: ValidationRecord,
) -> None:
    library_dir = validated_query_library_dir(connections_dir, entry.connection_name)
    metadata_path = library_dir / f"{entry.id}.yaml"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Validated query metadata not found: {metadata_path}")
    entry.validation = validation
    entry.updated_at = _utc_now()
    metadata_path.write_text(
        yaml.safe_dump(entry.to_dict(), sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )


def _normalize_saved_validation(entry: ValidatedQueryEntry) -> ValidationRecord:
    validation = current_validation_record(entry)
    if validation.status in {"validated", "failed"}:
        return validation
    if validation.status == "stale":
        return ValidationRecord(status="stale", error=validation.error)
    return ValidationRecord(status="draft")


def _search_relevance(entry: ValidatedQueryEntry, term_lower: str) -> tuple[int, str]:
    name_lower = (entry.display_name or entry.id).lower()
    id_lower = entry.id.lower()
    if not term_lower:
        return (0, id_lower)
    if id_lower == term_lower or name_lower == term_lower:
        return (0, id_lower)
    if id_lower.startswith(term_lower) or name_lower.startswith(term_lower):
        return (1, id_lower)
    return (2, id_lower)


def _validate_query_id(query_id: str) -> None:
    if not _QUERY_ID_RE.match(query_id):
        raise ValueError("Query id must contain only letters, numbers, underscores, or hyphens.")


def _validate_connection_name(connection_name: str) -> None:
    if not connection_name.strip():
        raise ValueError("connection_name is required.")
    if any(separator in connection_name for separator in _PATH_SEPARATORS) or ".." in Path(connection_name).parts:
        raise ValueError("connection_name must not contain path separators.")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
