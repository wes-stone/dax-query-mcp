from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class DAXQueryConfig:
    name: str
    connection_string: str
    dax_query: str
    description: str | None = None
    output_filename: str | None = None
    connection_timeout_seconds: int = 300
    command_timeout_seconds: int = 1800
    max_rows: int | None = None

    @property
    def export_name(self) -> str:
        return self.output_filename or self.name


@dataclass(slots=True, frozen=True)
class DAXConnectionConfig:
    name: str
    connection_string: str
    description: str | None = None
    suggested_skill: str | None = None
    suggested_skill_reason: str | None = None
    connection_timeout_seconds: int = 300
    command_timeout_seconds: int = 1800
    max_rows: int | None = None
    context_markdown: str | None = None
    context_path: str | None = None

