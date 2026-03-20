from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from .connections import load_connections
from .exceptions import ConfigurationError
from .models import DAXQueryConfig
from .query_builder import QUERY_BUILDER_SUFFIX, load_query_builder_artifacts


def create_sample_config(config_dir: str | Path) -> Path:
    """Create a sample query configuration file if none exists."""
    directory = Path(config_dir)
    directory.mkdir(parents=True, exist_ok=True)
    sample_path = directory / "sample_query.yaml"

    if sample_path.exists():
        return sample_path

    sample_yaml = """# Sample DAX Pipeline Configuration
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/YourWorkspace?readonly;
  Initial Catalog=YourDataset

dax_query: |
  EVALUATE
  SUMMARIZECOLUMNS(
      'Calendar'[Fiscal Month],
      'Account Information'[TPID],
      "Revenue", [Total Revenue]
  )
  ORDER BY 'Calendar'[Fiscal Month] ASC

description: "Monthly revenue by account"
output_filename: "monthly_revenue"
# Optional safety controls
# connection_timeout_seconds: 300
# command_timeout_seconds: 1800
# max_rows: 50000
"""

    sample_path.write_text(sample_yaml, encoding="utf-8")
    logger.info(f"Created sample configuration file in {directory}")
    logger.info("Edit sample_query.yaml with your connection string and DAX query")
    return sample_path


def load_queries(config_dir: str | Path) -> dict[str, DAXQueryConfig]:
    """Load query configurations from YAML files."""
    directory = Path(config_dir)

    if not directory.exists():
        logger.info(f"Creating {directory} directory...")
        create_sample_config(directory)
        return {}

    yaml_files = sorted(directory.glob("*.yaml")) + sorted(directory.glob("*.yml"))
    dax_builder_files = sorted(directory.glob(f"*{QUERY_BUILDER_SUFFIX}"))
    if not yaml_files and not dax_builder_files:
        logger.warning(f"No YAML configuration files found in {directory}")
        create_sample_config(directory)
        return {}

    queries: dict[str, DAXQueryConfig] = {}

    for config_file in yaml_files:
        try:
            raw_config = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
            if not isinstance(raw_config, dict):
                raise ConfigurationError("Top-level YAML must be a mapping")

            if "queries" in raw_config:
                raw_queries = raw_config["queries"]
                if not isinstance(raw_queries, dict):
                    raise ConfigurationError("'queries' must be a mapping of query names to config blocks")

                for query_name, query_config in raw_queries.items():
                    queries[query_name] = _build_query_config(
                        query_name=query_name,
                        raw_config=query_config,
                        source=config_file,
                    )
            else:
                query_name = config_file.stem
                queries[query_name] = _build_query_config(
                    query_name=query_name,
                    raw_config=raw_config,
                    source=config_file,
                )
        except Exception as exc:
            logger.error(f"Error loading {config_file}: {exc}")

    for builder_file in dax_builder_files:
        try:
            query_name = builder_file.name[: -len(QUERY_BUILDER_SUFFIX)]
            definition, dax_query = load_query_builder_artifacts(query_name, directory)
            connection = load_connections().get(definition.connection_name)
            if connection is None:
                raise ConfigurationError(
                    f"Query builder '{query_name}' references missing connection '{definition.connection_name}'"
                )
            queries[query_name] = DAXQueryConfig(
                name=query_name,
                connection_string=connection.connection_string,
                dax_query=dax_query,
                description=definition.description,
                output_filename=definition.output_filename,
                connection_timeout_seconds=connection.connection_timeout_seconds,
                command_timeout_seconds=(
                    definition.command_timeout_seconds
                    if definition.command_timeout_seconds is not None
                    else connection.command_timeout_seconds
                ),
                max_rows=definition.max_rows if definition.max_rows is not None else connection.max_rows,
            )
        except Exception as exc:
            logger.error(f"Error loading {builder_file}: {exc}")

    logger.success(f"Loaded {len(queries)} queries: {list(queries.keys())}")
    return queries


def _build_query_config(query_name: str, raw_config: Any, source: Path) -> DAXQueryConfig:
    if not isinstance(raw_config, dict):
        raise ConfigurationError(f"Query '{query_name}' in {source.name} must be a mapping")

    connection_string = _require_string(raw_config, "connection_string", query_name, source)
    dax_query = _require_string(raw_config, "dax_query", query_name, source)

    description = _optional_string(raw_config.get("description"), "description", query_name, source)
    output_filename = _optional_string(raw_config.get("output_filename"), "output_filename", query_name, source)
    connection_timeout_seconds = _coerce_int(
        raw_config.get("connection_timeout_seconds", 300),
        "connection_timeout_seconds",
        query_name,
        source,
        minimum=1,
    )
    command_timeout_seconds = _coerce_int(
        raw_config.get("command_timeout_seconds", 1800),
        "command_timeout_seconds",
        query_name,
        source,
        minimum=0,
    )
    max_rows = raw_config.get("max_rows")
    if max_rows is not None:
        max_rows = _coerce_int(max_rows, "max_rows", query_name, source, minimum=1)

    return DAXQueryConfig(
        name=query_name,
        connection_string=connection_string,
        dax_query=dax_query,
        description=description,
        output_filename=output_filename,
        connection_timeout_seconds=connection_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        max_rows=max_rows,
    )


def _require_string(raw_config: dict[str, Any], key: str, query_name: str, source: Path) -> str:
    value = raw_config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(
            f"Query '{query_name}' in {source.name} must define a non-empty '{key}' string"
        )
    return value.strip()


def _optional_string(value: Any, key: str, query_name: str, source: Path) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigurationError(f"Query '{query_name}' in {source.name} has non-string '{key}'")
    stripped = value.strip()
    return stripped or None


def _coerce_int(
    value: Any,
    key: str,
    query_name: str,
    source: Path,
    *,
    minimum: int,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            f"Query '{query_name}' in {source.name} has invalid integer '{key}'"
        ) from exc

    if parsed < minimum:
        raise ConfigurationError(
            f"Query '{query_name}' in {source.name} must set '{key}' >= {minimum}"
        )
    return parsed

