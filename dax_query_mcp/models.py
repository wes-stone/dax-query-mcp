from __future__ import annotations

from dataclasses import dataclass

TRANSPORT_MSOLAP = "msolap"
TRANSPORT_POWERBI_REST = "powerbi_rest"
SUPPORTED_TRANSPORTS = frozenset({TRANSPORT_MSOLAP, TRANSPORT_POWERBI_REST})
AUTH_AZURE_CLI = "azure_cli"
AUTH_ENV = "env"
SUPPORTED_AUTH_MODES = frozenset({AUTH_AZURE_CLI, AUTH_ENV})
DEFAULT_POWERBI_API_BASE_URL = "https://api.powerbi.com/v1.0/myorg"
DEFAULT_POWERBI_TOKEN_ENV = "POWERBI_ACCESS_TOKEN"


@dataclass(slots=True, frozen=True)
class DAXQueryConfig:
    name: str
    dax_query: str
    connection_string: str = ""
    description: str | None = None
    output_filename: str | None = None
    transport: str = TRANSPORT_MSOLAP
    dataset_id: str | None = None
    auth_mode: str = AUTH_AZURE_CLI
    access_token_env: str | None = None
    api_base_url: str = DEFAULT_POWERBI_API_BASE_URL
    impersonated_user_name: str | None = None
    connection_timeout_seconds: int = 300
    command_timeout_seconds: int = 1800
    max_rows: int | None = None

    @property
    def export_name(self) -> str:
        return self.output_filename or self.name


@dataclass(slots=True, frozen=True)
class DAXConnectionConfig:
    name: str
    connection_string: str = ""
    description: str | None = None
    suggested_skill: str | None = None
    suggested_skill_reason: str | None = None
    transport: str = TRANSPORT_MSOLAP
    dataset_id: str | None = None
    auth_mode: str = AUTH_AZURE_CLI
    access_token_env: str | None = None
    api_base_url: str = DEFAULT_POWERBI_API_BASE_URL
    impersonated_user_name: str | None = None
    connection_timeout_seconds: int = 300
    command_timeout_seconds: int = 1800
    max_rows: int | None = None
    context_markdown: str | None = None
    context_path: str | None = None
    overview_markdown: str | None = None
    overview_path: str | None = None

