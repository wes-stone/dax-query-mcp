from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


QUERY_BUILDER_SUFFIX = ".dax.queryBuilder"
QUERY_FILE_SUFFIX = ".dax"
QUERY_BUILDER_VERSION = 1
_TABLE_COLUMN_PATTERN = re.compile(r"^'(?P<table>.+)'\[(?P<name>.+)\]$")
_MEASURE_PATTERN = re.compile(r"^\[(?P<name>.+)\]$")
SUPPORTED_FILTER_OPERATORS = (
    "=",
    "==",
    "!=",
    "<>",
    ">",
    ">=",
    "<",
    "<=",
    "is",
    "is_not",
    "in",
    "not_in",
    "between",
    "contains",
    "starts_with",
    "is_blank",
    "is_not_blank",
)


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


def query_builder_schema_payload(connection_name: str = "your_connection") -> dict[str, Any]:
    example_payload = {
        "name": "monthly_revenue",
        "connection_name": connection_name,
        "description": "Monthly revenue by top parent",
        "columns": [
            "'Calendar'[Fiscal Month]",
            "'Account Information'[Top Parent]",
        ],
        "measures": [
            {
                "caption": "Revenue",
                "expression": "[Total Revenue]",
            }
        ],
        "filters": [
            {
                "expression": "'Calendar'[Fiscal Year]",
                "operator": "=",
                "value": 2026,
            }
        ],
        "order_by": [
            {
                "expression": "'Calendar'[Fiscal Month]",
                "direction": "ASC",
            }
        ],
        "command_timeout_seconds": 1800,
        "max_rows": 5000,
        "version": QUERY_BUILDER_VERSION,
    }
    return {
        "required_fields": {
            "name": "Non-empty string query name",
            "connection_name": "Non-empty string matching a configured connection",
        },
        "notes": [
            "Include at least one item in columns or measures.",
            "columns must be an array of non-empty strings.",
            "measures must be objects with caption and expression strings.",
            "filters must be objects with expression and operator; some operators require value, values, or value2.",
            "order_by items must be objects with expression and direction (ASC or DESC).",
        ],
        "supported_filter_operators": list(SUPPORTED_FILTER_OPERATORS),
        "example_payload": example_payload,
        "example_json": json.dumps(example_payload, indent=2),
    }


def query_builder_to_dax_studio_payload(definition: QueryBuilderDefinition) -> dict[str, Any]:
    default_table = _default_table_name(definition)
    sort_lookup = {item.expression: item.direction for item in definition.order_by}

    columns = [
        _build_dax_studio_column_payload(
            expression=expression,
            sort_direction=sort_lookup.get(expression, "None"),
        )
        for expression in definition.columns
    ]
    columns.extend(
        _build_dax_studio_measure_payload(
            measure=measure,
            default_table=default_table,
            sort_direction=sort_lookup.get(measure.expression, "None"),
        )
        for measure in definition.measures
    )

    filters = [
        _build_dax_studio_filter_payload(filter_item, default_table=default_table)
        for filter_item in definition.filters
    ]

    return {
        "AutoGenerate": False,
        "Columns": columns,
        "Filters": {
            "Items": filters,
        },
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
    saved_payload = query_builder_to_payload(definition)
    saved_payload.update(query_builder_to_dax_studio_payload(definition))
    builder_path.write_text(json.dumps(saved_payload, indent=2), encoding="utf-8")

    return {
        "query_name": definition.name,
        "queries_dir": str(directory),
        "dax_path": str(dax_path),
        "query_builder_path": str(builder_path),
        "dax_studio_open_path": str(dax_path),
        "dax_studio_note": (
            "You can open the generated .dax file in DAX Studio. "
            "The paired .dax.queryBuilder file is retained by dax-query-mcp as structured builder metadata."
        ),
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
    if operator not in SUPPORTED_FILTER_OPERATORS:
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


def _build_dax_studio_column_payload(*, expression: str, sort_direction: str) -> dict[str, Any]:
    reference = _parse_reference(expression)
    if reference["object_type"] != "Column":
        raise ValueError(f"Unsupported query builder column expression for DAX Studio export: {expression}")

    return {
        "TabularObject": _build_tabular_object_stub(
            caption=reference["name"],
            dax_name=expression,
            table_name=reference["table_name"],
            object_type="Column",
            metadata_image="Column",
        ),
        "SelectedTable": _build_table_stub(reference["table_name"]),
        "IsModelItem": True,
        "Caption": reference["name"],
        "IsOverriden": False,
        "MeasureExpression": "",
        "SortDirection": _normalize_sort_direction(sort_direction),
    }


def _build_dax_studio_measure_payload(
    *,
    measure: QueryBuilderMeasure,
    default_table: str,
    sort_direction: str,
) -> dict[str, Any]:
    reference = _parse_reference(measure.expression)
    table_name = reference["table_name"] or default_table
    return {
        "TabularObject": _build_tabular_object_stub(
            caption=measure.caption,
            dax_name=f"[{measure.caption}]",
            table_name=table_name,
            object_type="Measure",
            metadata_image="Measure",
            measure_expression=measure.expression,
        ),
        "SelectedTable": _build_table_stub(table_name),
        "IsModelItem": False,
        "Caption": measure.caption,
        "IsOverriden": True,
        "MeasureExpression": measure.expression,
        "SortDirection": _normalize_sort_direction(sort_direction),
    }


def _build_dax_studio_filter_payload(filter_item: QueryBuilderFilter, *, default_table: str) -> dict[str, Any]:
    reference = _parse_reference(filter_item.expression)
    table_name = reference["table_name"] or default_table
    return {
        "TabularObject": _build_tabular_object_stub(
            caption=reference["name"],
            dax_name=filter_item.expression,
            table_name=table_name,
            object_type=reference["object_type"],
            metadata_image="Measure" if reference["object_type"] == "Measure" else "Column",
        ),
        "ModelCapabilities": {
            "Variables": True,
            "TableConstructor": True,
            "DAXFunctions": {
                "SummarizeColumns": True,
                "SubstituteWithIndex": False,
                "TreatAs": True,
            },
        },
        "FilterType": _map_filter_type(filter_item.operator),
        "FilterValue": _format_filter_value(filter_item),
        "FilterValueIsParameter": False,
        "FilterValue2": _format_filter_value2(filter_item),
        "FilterValue2IsParameter": False,
    }


def _build_tabular_object_stub(
    *,
    caption: str,
    dax_name: str,
    table_name: str | None,
    object_type: str,
    metadata_image: str,
    measure_expression: str = "",
) -> dict[str, Any]:
    # DataType uses the integer form of Microsoft.AnalysisServices.Tabular.DataType
    # so DAX Studio's QueryBuilderColumnDataTypeConverter takes the non-string branch.
    # 2 = String (safe default when actual type is unknown).
    return {
        "Caption": caption,
        "DaxName": dax_name,
        "Description": "",
        "IsVisible": True,
        "ObjectType": object_type,
        "MetadataImage": metadata_image,
        "MeasureExpression": measure_expression,
        "TableName": table_name,
        "DataType": 2,
        "SystemType": "System.String, mscorlib",
        "ImageResource": "",
    }


def _build_table_stub(table_name: str) -> dict[str, Any]:
    dax_name = table_name if table_name.startswith("'") else f"'{table_name}'"
    clean_name = table_name.strip("'")
    return {
        "Caption": clean_name,
        "DaxName": dax_name,
        "Name": clean_name,
        "Description": "",
        "IsVisible": True,
        "ObjectType": "Table",
    }


def _default_table_name(definition: QueryBuilderDefinition) -> str:
    for expression in definition.columns:
        reference = _parse_reference(expression)
        if reference["table_name"]:
            return reference["table_name"]
    return "QueryBuilder"


def _parse_reference(expression: str) -> dict[str, str | None]:
    column_match = _TABLE_COLUMN_PATTERN.match(expression)
    if column_match:
        return {
            "object_type": "Column",
            "table_name": column_match.group("table"),
            "name": column_match.group("name"),
        }

    measure_match = _MEASURE_PATTERN.match(expression)
    if measure_match:
        return {
            "object_type": "Measure",
            "table_name": None,
            "name": measure_match.group("name"),
        }

    return {
        "object_type": "Measure",
        "table_name": None,
        "name": expression,
    }


def _normalize_sort_direction(direction: str) -> str:
    normalized = direction.upper()
    if normalized in {"ASC", "DESC", "NONE"}:
        return "None" if normalized == "NONE" else normalized
    return "None"


def _map_filter_type(operator: str) -> str:
    return {
        "=": "Is",
        "==": "Is",
        "is": "Is",
        "!=": "IsNot",
        "<>": "IsNot",
        "is_not": "IsNot",
        ">": "GreaterThan",
        ">=": "GreaterThanOrEqual",
        "<": "LessThan",
        "<=": "LessThanOrEqual",
        "in": "In",
        "not_in": "NotIn",
        "between": "Between",
        "contains": "Contains",
        "starts_with": "StartsWith",
        "is_blank": "IsBlank",
        "is_not_blank": "IsNotBlank",
    }[operator]


def _format_filter_value(filter_item: QueryBuilderFilter) -> str:
    if filter_item.operator in {"in", "not_in"}:
        return "\n".join(_stringify_filter_scalar(value) for value in filter_item.values)
    if filter_item.operator in {"is_blank", "is_not_blank"}:
        return ""
    return _stringify_filter_scalar(filter_item.value)


def _format_filter_value2(filter_item: QueryBuilderFilter) -> str:
    if filter_item.operator != "between":
        return ""
    return _stringify_filter_scalar(filter_item.value2)


def _stringify_filter_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


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
