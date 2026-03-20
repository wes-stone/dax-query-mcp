from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


QUERY_BUILDER_SUFFIX = ".dax.queryBuilder"
QUERY_FILE_SUFFIX = ".dax"
QUERY_BUILDER_VERSION = 1


@dataclass(slots=True, frozen=True)
class QueryBuilderMeasure:
    caption: str
    expression: str


@dataclass(slots=True, frozen=True)
class QueryBuilderFilter:
    expression: str
    operator: str
    value: Any = None
    values: tuple[Any, ...] = ()
    value2: Any = None


@dataclass(slots=True, frozen=True)
class QueryBuilderOrderBy:
    expression: str
    direction: str = "ASC"


@dataclass(slots=True, frozen=True)
class QueryBuilderDefinition:
    name: str
    connection_name: str
    columns: tuple[str, ...] = ()
    measures: tuple[QueryBuilderMeasure, ...] = ()
    filters: tuple[QueryBuilderFilter, ...] = ()
    order_by: tuple[QueryBuilderOrderBy, ...] = ()
    description: str | None = None
    output_filename: str | None = None
    command_timeout_seconds: int | None = None
    max_rows: int | None = None
    version: int = QUERY_BUILDER_VERSION


def query_builder_from_dict(payload: dict[str, Any]) -> QueryBuilderDefinition:
    if not isinstance(payload, dict):
        raise ValueError("Query builder payload must be a JSON object")

    name = _require_non_empty_string(payload.get("name"), "name")
    connection_name = _require_non_empty_string(payload.get("connection_name"), "connection_name")
    columns = tuple(_require_non_empty_string(item, "columns[]") for item in payload.get("columns", []))
    measures = tuple(_measure_from_dict(item) for item in payload.get("measures", []))
    filters = tuple(_filter_from_dict(item) for item in payload.get("filters", []))
    order_by = tuple(_order_by_from_dict(item) for item in payload.get("order_by", []))
    description = _optional_string(payload.get("description"), "description")
    output_filename = _optional_string(payload.get("output_filename"), "output_filename")
    command_timeout_seconds = _optional_int(payload.get("command_timeout_seconds"), "command_timeout_seconds", minimum=0)
    max_rows = _optional_int(payload.get("max_rows"), "max_rows", minimum=1)
    version = int(payload.get("version", QUERY_BUILDER_VERSION))

    if not columns and not measures:
        raise ValueError("Query builder definition must include at least one column or measure")

    return QueryBuilderDefinition(
        name=name,
        connection_name=connection_name,
        columns=columns,
        measures=measures,
        filters=filters,
        order_by=order_by,
        description=description,
        output_filename=output_filename,
        command_timeout_seconds=command_timeout_seconds,
        max_rows=max_rows,
        version=version,
    )


def query_builder_to_payload(definition: QueryBuilderDefinition) -> dict[str, Any]:
    return {
        "version": definition.version,
        "name": definition.name,
        "connection_name": definition.connection_name,
        "description": definition.description,
        "output_filename": definition.output_filename,
        "command_timeout_seconds": definition.command_timeout_seconds,
        "max_rows": definition.max_rows,
        "columns": list(definition.columns),
        "measures": [
            {"caption": measure.caption, "expression": measure.expression}
            for measure in definition.measures
        ],
        "filters": [
            {
                "expression": filter_item.expression,
                "operator": filter_item.operator,
                "value": filter_item.value,
                "values": list(filter_item.values),
                "value2": filter_item.value2,
            }
            for filter_item in definition.filters
        ],
        "order_by": [
            {"expression": order.expression, "direction": order.direction}
            for order in definition.order_by
        ],
    }


def build_query_builder_dax(definition: QueryBuilderDefinition) -> str:
    arguments: list[str] = []
    arguments.extend(definition.columns)
    arguments.extend(_render_filter(filter_item) for filter_item in definition.filters)
    arguments.extend(
        f'"{measure.caption}", {measure.expression}'
        for measure in definition.measures
    )

    query_lines = [
        "EVALUATE",
        "SUMMARIZECOLUMNS(",
        _indent_arguments(arguments),
        ")",
    ]

    if definition.order_by:
        order_clause = ", ".join(
            f"{item.expression} {item.direction}"
            for item in definition.order_by
        )
        query_lines.append(f"ORDER BY {order_clause}")

    return "\n".join(query_lines)


def save_query_builder_artifacts(
    definition: QueryBuilderDefinition,
    queries_dir: str | Path = "queries",
    *,
    overwrite: bool = False,
) -> dict[str, str]:
    directory = Path(queries_dir)
    directory.mkdir(parents=True, exist_ok=True)
    dax_path = directory / f"{definition.name}{QUERY_FILE_SUFFIX}"
    builder_path = directory / f"{definition.name}{QUERY_BUILDER_SUFFIX}"

    if not overwrite:
        for path in (dax_path, builder_path):
            if path.exists():
                raise FileExistsError(f"Refusing to overwrite existing file: {path}")

    dax_path.write_text(build_query_builder_dax(definition), encoding="utf-8")
    builder_path.write_text(json.dumps(query_builder_to_payload(definition), indent=2), encoding="utf-8")

    return {
        "query_name": definition.name,
        "queries_dir": str(directory),
        "dax_path": str(dax_path),
        "query_builder_path": str(builder_path),
    }


def load_query_builder_artifacts(
    query_name: str,
    queries_dir: str | Path = "queries",
) -> tuple[QueryBuilderDefinition, str]:
    directory = Path(queries_dir)
    dax_path = directory / f"{query_name}{QUERY_FILE_SUFFIX}"
    builder_path = directory / f"{query_name}{QUERY_BUILDER_SUFFIX}"

    if not dax_path.exists():
        raise FileNotFoundError(f"Query file not found: {dax_path}")
    if not builder_path.exists():
        raise FileNotFoundError(f"Query builder sidecar not found: {builder_path}")

    payload = json.loads(builder_path.read_text(encoding="utf-8"))
    definition = query_builder_from_dict(payload)
    dax_query = dax_path.read_text(encoding="utf-8").strip()
    return definition, dax_query


def load_query_builder_definition_file(path: str | Path) -> QueryBuilderDefinition:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return query_builder_from_dict(payload)


def _measure_from_dict(payload: Any) -> QueryBuilderMeasure:
    if not isinstance(payload, dict):
        raise ValueError("Each measure must be an object")
    return QueryBuilderMeasure(
        caption=_require_non_empty_string(payload.get("caption"), "measure.caption"),
        expression=_require_non_empty_string(payload.get("expression"), "measure.expression"),
    )


def _filter_from_dict(payload: Any) -> QueryBuilderFilter:
    if not isinstance(payload, dict):
        raise ValueError("Each filter must be an object")

    expression = _require_non_empty_string(payload.get("expression"), "filter.expression")
    operator = _require_non_empty_string(payload.get("operator"), "filter.operator").lower()
    value = payload.get("value")
    values = tuple(payload.get("values", []) or [])
    value2 = payload.get("value2")

    if operator in {"=", "==", "!=", "<>", ">", ">=", "<", "<=", "is", "is_not", "contains", "starts_with"} and value is None:
        raise ValueError(f"Filter '{expression}' requires a single 'value'")
    if operator in {"in", "not_in"} and not values:
        raise ValueError(f"Filter '{expression}' requires a non-empty 'values' array")
    if operator == "between" and (value is None or value2 is None):
        raise ValueError(f"Filter '{expression}' requires both 'value' and 'value2'")
    if operator not in {"=", "==", "!=", "<>", ">", ">=", "<", "<=", "is", "is_not", "in", "not_in", "between", "contains", "starts_with", "is_blank", "is_not_blank"}:
        raise ValueError(f"Unsupported filter operator: {operator}")

    return QueryBuilderFilter(
        expression=expression,
        operator=operator,
        value=value,
        values=values,
        value2=value2,
    )


def _order_by_from_dict(payload: Any) -> QueryBuilderOrderBy:
    if not isinstance(payload, dict):
        raise ValueError("Each order_by item must be an object")
    direction = _require_non_empty_string(payload.get("direction", "ASC"), "order_by.direction").upper()
    if direction not in {"ASC", "DESC"}:
        raise ValueError("order_by.direction must be ASC or DESC")
    return QueryBuilderOrderBy(
        expression=_require_non_empty_string(payload.get("expression"), "order_by.expression"),
        direction=direction,
    )


def _render_filter(filter_item: QueryBuilderFilter) -> str:
    expression = filter_item.expression
    operator = filter_item.operator

    if operator in {"=", "==", "is"}:
        predicate = f"{expression} = {_format_literal(filter_item.value)}"
    elif operator in {"!=", "<>", "is_not"}:
        predicate = f"{expression} <> {_format_literal(filter_item.value)}"
    elif operator in {">", ">=", "<", "<="}:
        predicate = f"{expression} {operator} {_format_literal(filter_item.value)}"
    elif operator == "between":
        predicate = (
            f"{expression} >= {_format_literal(filter_item.value)} && "
            f"{expression} <= {_format_literal(filter_item.value2)}"
        )
    elif operator == "in":
        predicate = f"{expression} IN {_format_set_literal(filter_item.values)}"
    elif operator == "not_in":
        predicate = f"NOT ({expression} IN {_format_set_literal(filter_item.values)})"
    elif operator == "contains":
        predicate = f"CONTAINSSTRING({expression}, {_format_literal(filter_item.value)})"
    elif operator == "starts_with":
        predicate = f"STARTSWITH({expression}, {_format_literal(filter_item.value)})"
    elif operator == "is_blank":
        predicate = f"ISBLANK({expression})"
    elif operator == "is_not_blank":
        predicate = f"NOT ISBLANK({expression})"
    else:
        raise ValueError(f"Unsupported filter operator: {operator}")

    return f"KEEPFILTERS(FILTER(ALL({expression}), {predicate}))"


def _format_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE()" if value else "FALSE()"
    if value is None:
        return "BLANK()"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    escaped = str(value).replace('"', '""')
    return f'"{escaped}"'


def _format_set_literal(values: tuple[Any, ...]) -> str:
    return "{ " + ", ".join(_format_literal(value) for value in values) + " }"


def _indent_arguments(arguments: list[str]) -> str:
    return ",\n".join(f"    {argument}" for argument in arguments)


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{field_name}' must be a non-empty string")
    return value.strip()


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"'{field_name}' must be a string")
    stripped = value.strip()
    return stripped or None


def _optional_int(value: Any, field_name: str, *, minimum: int) -> int | None:
    if value is None:
        return None
    parsed = int(value)
    if parsed < minimum:
        raise ValueError(f"'{field_name}' must be >= {minimum}")
    return parsed
