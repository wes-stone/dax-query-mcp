"""Pydantic models and YAML I/O for semantic-model data dictionaries."""

from __future__ import annotations

from pathlib import Path
from collections.abc import Callable
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field


# ── Schema models ────────────────────────────────────────────────────────────


class ColumnDef(BaseModel):
    """A column inside a table."""

    name: str
    data_type: str = "string"
    description: str = ""
    sample_values: list[str] = Field(default_factory=list)


class TableDef(BaseModel):
    """A table (fact or dimension) in the semantic model."""

    name: str
    description: str = ""
    columns: list[ColumnDef] = Field(default_factory=list)


class MeasureDef(BaseModel):
    """A DAX measure."""

    name: str
    expression: str
    description: str = ""
    format_string: str = ""


class FilterDef(BaseModel):
    """A suggested filter for querying the model."""

    name: str
    column: str
    description: str = ""
    suggested_values: list[str] = Field(default_factory=list)


class RelationshipDef(BaseModel):
    """A relationship between two semantic model columns."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: Literal["many-to-one", "one-to-many", "one-to-one", "many-to-many"] = "many-to-one"
    cross_filter_direction: Literal["single", "both"] = "single"
    is_active: bool = True
    description: str = ""
    source: Literal["curated", "tmschema", "tmsl", "mdschema-inferred"] = "curated"
    confidence: Literal["high", "medium", "low"] = "high"


class DataDictionary(BaseModel):
    """Top-level data dictionary describing a semantic model."""

    version: str = "1.0"
    tables: list[TableDef] = Field(default_factory=list)
    measures: list[MeasureDef] = Field(default_factory=list)
    filters: list[FilterDef] = Field(default_factory=list)
    relationships: list[RelationshipDef] = Field(default_factory=list)


# ── YAML I/O ─────────────────────────────────────────────────────────────────


def load_data_dictionary(path: str | Path) -> DataDictionary:
    """Load a DataDictionary from a YAML file."""
    with open(path, encoding="utf-8") as fh:
        data: dict[str, Any] = yaml.safe_load(fh) or {}
    return DataDictionary.model_validate(data)


def save_data_dictionary(dd: DataDictionary, path: str | Path) -> None:
    """Save a DataDictionary to a YAML file."""
    data = dd.model_dump(exclude_defaults=True)
    with open(path, "w", encoding="utf-8") as fh:
        yaml.dump(
            data,
            fh,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )


def find_data_dictionary(
    connection_name: str,
    connections_dir: str | Path,
) -> DataDictionary | None:
    """Look for ``{connection_name}.data_dictionary.yaml`` in *connections_dir*.

    Returns the parsed :class:`DataDictionary` if the file exists, otherwise ``None``.
    """
    path = Path(connections_dir) / f"{connection_name}.data_dictionary.yaml"
    if not path.is_file():
        return None
    return load_data_dictionary(path)


# ── Lifecycle helpers ────────────────────────────────────────────────────────


def diff_data_dictionaries(base: DataDictionary, candidate: DataDictionary) -> dict[str, Any]:
    """Return added/removed/changed entity names between two dictionaries."""
    base_tables = {table.name: table for table in base.tables}
    candidate_tables = {table.name: table for table in candidate.tables}
    base_measures = {measure.name: measure for measure in base.measures}
    candidate_measures = {measure.name: measure for measure in candidate.measures}
    base_filters = {filter_def.name: filter_def for filter_def in base.filters}
    candidate_filters = {filter_def.name: filter_def for filter_def in candidate.filters}
    base_relationships = {_relationship_key(rel): rel for rel in base.relationships}
    candidate_relationships = {_relationship_key(rel): rel for rel in candidate.relationships}

    table_column_changes: dict[str, dict[str, list[str]]] = {}
    for table_name in sorted(set(base_tables).intersection(candidate_tables)):
        base_columns = {column.name for column in base_tables[table_name].columns}
        candidate_columns = {column.name for column in candidate_tables[table_name].columns}
        added = sorted(candidate_columns - base_columns)
        removed = sorted(base_columns - candidate_columns)
        if added or removed:
            table_column_changes[table_name] = {"added": added, "removed": removed}

    return {
        "tables": _name_diff(base_tables, candidate_tables),
        "columns": table_column_changes,
        "measures": _name_diff(base_measures, candidate_measures),
        "filters": _name_diff(base_filters, candidate_filters),
        "relationships": _name_diff(base_relationships, candidate_relationships),
    }


def merge_data_dictionaries(generated: DataDictionary, curated: DataDictionary) -> DataDictionary:
    """Merge regenerated metadata while preserving curated descriptions and notes."""
    curated_tables = {table.name: table for table in curated.tables}
    merged_tables: list[TableDef] = []
    for generated_table in generated.tables:
        curated_table = curated_tables.get(generated_table.name)
        if curated_table is None:
            merged_tables.append(generated_table)
            continue
        curated_columns = {column.name: column for column in curated_table.columns}
        merged_columns = []
        for generated_column in generated_table.columns:
            curated_column = curated_columns.get(generated_column.name)
            merged_columns.append(_merge_column(generated_column, curated_column))
        generated_column_names = {column.name for column in generated_table.columns}
        merged_columns.extend(
            column for column in curated_table.columns if column.name not in generated_column_names
        )
        merged_tables.append(
            TableDef(
                name=generated_table.name,
                description=curated_table.description or generated_table.description,
                columns=merged_columns,
            )
        )
    generated_table_names = {table.name for table in generated.tables}
    merged_tables.extend(table for table in curated.tables if table.name not in generated_table_names)

    merged_measures = _merge_named_lists(
        generated.measures,
        curated.measures,
        lambda generated_measure, curated_measure: MeasureDef(
            name=generated_measure.name,
            expression=curated_measure.expression or generated_measure.expression,
            description=curated_measure.description or generated_measure.description,
            format_string=curated_measure.format_string or generated_measure.format_string,
        ),
    )
    merged_filters = _merge_named_lists(
        generated.filters,
        curated.filters,
        lambda generated_filter, curated_filter: FilterDef(
            name=generated_filter.name,
            column=curated_filter.column or generated_filter.column,
            description=curated_filter.description or generated_filter.description,
            suggested_values=curated_filter.suggested_values or generated_filter.suggested_values,
        ),
    )
    merged_relationships = _merge_relationships(generated.relationships, curated.relationships)

    return DataDictionary(
        version=curated.version or generated.version,
        tables=merged_tables,
        measures=merged_measures,
        filters=merged_filters,
        relationships=merged_relationships,
    )


def review_data_dictionary_update(curated: DataDictionary, generated: DataDictionary) -> dict[str, Any]:
    """Return a review payload for a regenerated data dictionary before saving."""
    merged = merge_data_dictionaries(generated, curated)
    return {
        "diff": diff_data_dictionaries(curated, generated),
        "merged": merged.model_dump(exclude_defaults=True),
        "summary": {
            "tables": len(merged.tables),
            "measures": len(merged.measures),
            "filters": len(merged.filters),
            "relationships": len(merged.relationships),
        },
    }


def _name_diff(base: dict[Any, Any], candidate: dict[Any, Any]) -> dict[str, list[str]]:
    base_keys = set(base)
    candidate_keys = set(candidate)
    return {
        "added": sorted(str(key) for key in candidate_keys - base_keys),
        "removed": sorted(str(key) for key in base_keys - candidate_keys),
        "unchanged": sorted(str(key) for key in base_keys.intersection(candidate_keys)),
    }


def _merge_column(generated: ColumnDef, curated: ColumnDef | None) -> ColumnDef:
    if curated is None:
        return generated
    return ColumnDef(
        name=generated.name,
        data_type=generated.data_type or curated.data_type,
        description=curated.description or generated.description,
        sample_values=curated.sample_values or generated.sample_values,
    )


def _merge_named_lists(
    generated: list[Any],
    curated: list[Any],
    merge_item: Callable[[Any, Any], Any],
) -> list[Any]:
    curated_by_name = {item.name: item for item in curated}
    merged = [
        merge_item(item, curated_by_name[item.name]) if item.name in curated_by_name else item
        for item in generated
    ]
    generated_names = {item.name for item in generated}
    merged.extend(item for item in curated if item.name not in generated_names)
    return merged


def _relationship_key(relationship: RelationshipDef) -> tuple[str, str, str, str]:
    return (
        relationship.from_table,
        relationship.from_column,
        relationship.to_table,
        relationship.to_column,
    )


def _merge_relationships(
    generated: list[RelationshipDef],
    curated: list[RelationshipDef],
) -> list[RelationshipDef]:
    curated_by_key = {_relationship_key(relationship): relationship for relationship in curated}
    merged: list[RelationshipDef] = []
    for relationship in generated:
        curated_relationship = curated_by_key.get(_relationship_key(relationship))
        if curated_relationship is None:
            merged.append(relationship)
            continue
        merged.append(
            RelationshipDef(
                from_table=relationship.from_table,
                from_column=relationship.from_column,
                to_table=relationship.to_table,
                to_column=relationship.to_column,
                cardinality=curated_relationship.cardinality or relationship.cardinality,
                cross_filter_direction=(
                    curated_relationship.cross_filter_direction or relationship.cross_filter_direction
                ),
                is_active=curated_relationship.is_active,
                description=curated_relationship.description or relationship.description,
                source=curated_relationship.source or relationship.source,
                confidence=curated_relationship.confidence or relationship.confidence,
            )
        )
    generated_keys = {_relationship_key(relationship) for relationship in generated}
    merged.extend(relationship for relationship in curated if _relationship_key(relationship) not in generated_keys)
    return merged
