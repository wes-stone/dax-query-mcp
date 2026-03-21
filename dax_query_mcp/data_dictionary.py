"""Pydantic models and YAML I/O for semantic-model data dictionaries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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


class DataDictionary(BaseModel):
    """Top-level data dictionary describing a semantic model."""

    version: str = "1.0"
    tables: list[TableDef] = Field(default_factory=list)
    measures: list[MeasureDef] = Field(default_factory=list)
    filters: list[FilterDef] = Field(default_factory=list)


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
