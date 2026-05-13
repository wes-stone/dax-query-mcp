from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from .exceptions import ConfigurationError
from .models import (
    AUTH_AZURE_CLI,
    DEFAULT_POWERBI_API_BASE_URL,
    DEFAULT_POWERBI_TOKEN_ENV,
    SUPPORTED_AUTH_MODES,
    SUPPORTED_TRANSPORTS,
    TRANSPORT_MSOLAP,
    TRANSPORT_POWERBI_REST,
    DAXConnectionConfig,
)

PREFERRED_CONNECTIONS_DIR = "Connections"
LEGACY_CONNECTIONS_DIR = "queries"
SAMPLE_CONNECTION_STEM = "sample_connection"


def default_user_connections_dir() -> Path:
    return Path.home() / ".copilot" / "dax-query-mcp" / "Connections"


def resolve_connections_dir(connections_dir: str | Path | None = None) -> Path:
    if connections_dir is not None:
        return Path(connections_dir)

    preferred = Path(PREFERRED_CONNECTIONS_DIR)
    if preferred.exists():
        return preferred

    legacy = Path(LEGACY_CONNECTIONS_DIR)
    if legacy.exists():
        return legacy

    return default_user_connections_dir()


def create_sample_connection_config(connections_dir: str | Path) -> Path:
    directory = Path(connections_dir)
    directory.mkdir(parents=True, exist_ok=True)
    sample_path = directory / "sample_connection.yaml"

    if sample_path.exists():
        return sample_path

    sample_yaml = """# Sample DAX Connection Configuration
# transport defaults to "msolap" when omitted
connection_string: |
  Provider=MSOLAP.8;
  Data Source=powerbi://api.powerbi.com/v1.0/myorg/YourWorkspace?readonly;
  Initial Catalog=YourDataset

description: "Semantic model connection for ad hoc DAX queries"
# Optional MCP workflow hint
# suggested_skill: "enrollment-skills"
# suggested_skill_reason: "Use this when you want help drafting KQL against canonical enrollment data."
# Optional safety controls
# connection_timeout_seconds: 300
# command_timeout_seconds: 1800
# max_rows: 50000
#
# Optional Power BI REST transport example:
# transport: powerbi_rest
# dataset_id: "00000000-0000-0000-0000-000000000000"
# auth_mode: azure_cli
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

    config_files = [
        config_file
        for config_file in sorted(directory.glob("*.yaml")) + sorted(directory.glob("*.yml"))
        if _is_connection_config_file(config_file)
    ]
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


def _is_connection_config_file(config_file: Path) -> bool:
    return not config_file.name.endswith(".data_dictionary.yaml")


def _build_connection_config(
    connection_name: str,
    raw_config: dict[str, Any],
    source: Path,
) -> DAXConnectionConfig:
    transport = _coerce_transport(raw_config.get("transport", TRANSPORT_MSOLAP), connection_name, source)
    if transport == TRANSPORT_POWERBI_REST:
        connection_string = _optional_string(raw_config.get("connection_string"), "connection_string", connection_name, source) or ""
        dataset_id = _require_string(raw_config, "dataset_id", connection_name, source)
    else:
        connection_string = _require_string(raw_config, "connection_string", connection_name, source)
        dataset_id = _optional_string(raw_config.get("dataset_id"), "dataset_id", connection_name, source)

    description = _optional_string(raw_config.get("description"), "description", connection_name, source)
    suggested_skill = _optional_string(raw_config.get("suggested_skill"), "suggested_skill", connection_name, source)
    suggested_skill_reason = _optional_string(
        raw_config.get("suggested_skill_reason"),
        "suggested_skill_reason",
        connection_name,
        source,
    )
    auth_mode = _coerce_auth_mode(raw_config.get("auth_mode", AUTH_AZURE_CLI), connection_name, source)
    access_token_env = (
        _optional_string(raw_config.get("access_token_env"), "access_token_env", connection_name, source)
        or DEFAULT_POWERBI_TOKEN_ENV
    )
    api_base_url = (
        _optional_string(raw_config.get("api_base_url"), "api_base_url", connection_name, source)
        or DEFAULT_POWERBI_API_BASE_URL
    )
    impersonated_user_name = _optional_string(
        raw_config.get("impersonated_user_name"),
        "impersonated_user_name",
        connection_name,
        source,
    )
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

    overview_path = source.parent / f"{connection_name}_overview.md"
    overview_markdown = overview_path.read_text(encoding="utf-8") if overview_path.exists() else None

    return DAXConnectionConfig(
        name=connection_name,
        connection_string=connection_string,
        description=description,
        suggested_skill=suggested_skill,
        suggested_skill_reason=suggested_skill_reason,
        transport=transport,
        dataset_id=dataset_id,
        auth_mode=auth_mode,
        access_token_env=access_token_env,
        api_base_url=api_base_url.rstrip("/"),
        impersonated_user_name=impersonated_user_name,
        connection_timeout_seconds=connection_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        max_rows=max_rows,
        context_markdown=context_markdown,
        context_path=str(context_path) if context_path.exists() else None,
        overview_markdown=overview_markdown,
        overview_path=str(overview_path) if overview_path.exists() else None,
    )


def _require_string(raw_config: dict[str, Any], key: str, connection_name: str, source: Path) -> str:
    value = raw_config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(
            f"Connection '{connection_name}' in {source.name} must define a non-empty '{key}' string"
        )
    return value.strip()


def _coerce_transport(value: Any, connection_name: str, source: Path) -> str:
    transport = _optional_string(value, "transport", connection_name, source) or TRANSPORT_MSOLAP
    normalized = transport.lower()
    if normalized not in SUPPORTED_TRANSPORTS:
        supported = ", ".join(sorted(SUPPORTED_TRANSPORTS))
        raise ConfigurationError(
            f"Connection '{connection_name}' in {source.name} has unsupported transport "
            f"'{transport}'. Supported transports: {supported}"
        )
    return normalized


def _coerce_auth_mode(value: Any, connection_name: str, source: Path) -> str:
    auth_mode = _optional_string(value, "auth_mode", connection_name, source) or AUTH_AZURE_CLI
    normalized = auth_mode.lower()
    if normalized not in SUPPORTED_AUTH_MODES:
        supported = ", ".join(sorted(SUPPORTED_AUTH_MODES))
        raise ConfigurationError(
            f"Connection '{connection_name}' in {source.name} has unsupported auth_mode "
            f"'{auth_mode}'. Supported auth modes: {supported}"
        )
    return normalized


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

