from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from .exceptions import ConfigurationError
from .models import DAXConnectionConfig

PREFERRED_CONNECTIONS_DIR = "Connections"
LEGACY_CONNECTIONS_DIR = "queries"
SAMPLE_CONNECTION_STEM = "sample_connection"


def resolve_connections_dir(connections_dir: str | Path | None = None) -> Path:
    if connections_dir is not None:
        return Path(connections_dir)

    preferred = Path(PREFERRED_CONNECTIONS_DIR)
    if preferred.exists():
        return preferred

    legacy = Path(LEGACY_CONNECTIONS_DIR)
    if legacy.exists():
        return legacy

    return preferred


def create_sample_connection_config(connections_dir: str | Path) -> Path:
    directory = Path(connections_dir)
    directory.mkdir(parents=True, exist_ok=True)
    sample_path = directory / "sample_connection.yaml"

    if sample_path.exists():
        return sample_path

    sample_yaml = """# Sample DAX Connection Configuration
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/YourWorkspace?readonly;
  Initial Catalog=YourDataset

description: "Semantic model connection for ad hoc DAX queries"
# Optional safety controls
# connection_timeout_seconds: 300
# command_timeout_seconds: 1800
# max_rows: 50000
"""

    sample_path.write_text(sample_yaml, encoding="utf-8")
    context_path = directory / "sample_connection.md"
    if not context_path.exists():
        context_path.write_text(
            "# Sample connection context\n\nUse this markdown file to store notes about the semantic model, important tables, naming conventions, or prompt context for MCP clients.\n",
            encoding="utf-8",
        )
    logger.info(f"Created sample connection configuration in {directory}")
    return sample_path


def load_connections(
    connections_dir: str | Path | None = None,
    *,
    include_placeholders: bool = False,
) -> dict[str, DAXConnectionConfig]:
    directory = resolve_connections_dir(connections_dir)

    if not directory.exists():
        logger.info(f"Creating {directory} directory...")
        create_sample_connection_config(directory)
        return {}

    config_files = sorted(directory.glob("*.yaml")) + sorted(directory.glob("*.yml"))
    if not config_files:
        logger.warning(f"No YAML connection files found in {directory}")
        create_sample_connection_config(directory)
        return {}

    if not include_placeholders:
        config_files = [config_file for config_file in config_files if not _is_placeholder_connection_file(config_file)]

    if not config_files:
        logger.info(
            f"No user connection files found in {directory}. "
            f"Copy or rename {SAMPLE_CONNECTION_STEM}.yaml to create a real connection."
        )
        return {}

    connections: dict[str, DAXConnectionConfig] = {}
    for config_file in config_files:
        try:
            raw_config = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
            if not isinstance(raw_config, dict):
                raise ConfigurationError("Top-level YAML must be a mapping")

            connection_name = config_file.stem
            connections[connection_name] = _build_connection_config(
                connection_name=connection_name,
                raw_config=raw_config,
                source=config_file,
            )
        except Exception as exc:
            logger.error(f"Error loading {config_file}: {exc}")

    logger.success(f"Loaded {len(connections)} connections: {list(connections.keys())}")
    return connections


def _is_placeholder_connection_file(config_file: Path) -> bool:
    return config_file.stem == SAMPLE_CONNECTION_STEM


def _build_connection_config(
    connection_name: str,
    raw_config: dict[str, Any],
    source: Path,
) -> DAXConnectionConfig:
    connection_string = _require_string(raw_config, "connection_string", connection_name, source)
    description = _optional_string(raw_config.get("description"), "description", connection_name, source)
    connection_timeout_seconds = _coerce_int(
        raw_config.get("connection_timeout_seconds", 300),
        "connection_timeout_seconds",
        connection_name,
        source,
        minimum=1,
    )
    command_timeout_seconds = _coerce_int(
        raw_config.get("command_timeout_seconds", 1800),
        "command_timeout_seconds",
        connection_name,
        source,
        minimum=0,
    )
    max_rows = raw_config.get("max_rows")
    if max_rows is not None:
        max_rows = _coerce_int(max_rows, "max_rows", connection_name, source, minimum=1)

    context_path = source.with_suffix(".md")
    context_markdown = context_path.read_text(encoding="utf-8") if context_path.exists() else None

    return DAXConnectionConfig(
        name=connection_name,
        connection_string=connection_string,
        description=description,
        connection_timeout_seconds=connection_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        max_rows=max_rows,
        context_markdown=context_markdown,
        context_path=str(context_path) if context_path.exists() else None,
    )


def _require_string(raw_config: dict[str, Any], key: str, connection_name: str, source: Path) -> str:
    value = raw_config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(
            f"Connection '{connection_name}' in {source.name} must define a non-empty '{key}' string"
        )
    return value.strip()


def _optional_string(value: Any, key: str, connection_name: str, source: Path) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigurationError(f"Connection '{connection_name}' in {source.name} has non-string '{key}'")
    stripped = value.strip()
    return stripped or None


def _coerce_int(
    value: Any,
    key: str,
    connection_name: str,
    source: Path,
    *,
    minimum: int,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            f"Connection '{connection_name}' in {source.name} has invalid integer '{key}'"
        ) from exc

    if parsed < minimum:
        raise ConfigurationError(
            f"Connection '{connection_name}' in {source.name} must set '{key}' >= {minimum}"
        )
    return parsed

