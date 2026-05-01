from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from contextlib import suppress
from datetime import datetime
from typing import Callable, Iterator

import pandas as pd
from loguru import logger

from .exceptions import DAXExecutionError
from .models import (
    AUTH_AZURE_CLI,
    AUTH_ENV,
    DEFAULT_POWERBI_API_BASE_URL,
    DEFAULT_POWERBI_TOKEN_ENV,
    TRANSPORT_MSOLAP,
    TRANSPORT_POWERBI_REST,
    DAXQueryConfig,
)
from .profiling import QueryProfiler

DispatchFn = Callable[[str], object]
POWERBI_API_RESOURCE = "https://analysis.windows.net/powerbi/api"
WINDOWS_AZURE_CLI_PATHS = (
    r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
    r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
)
MSOLAP_INSTALL_URL = (
    "https://learn.microsoft.com/en-us/analysis-services/client-libraries?view=sql-analysis-services-2025"
)

_SENSITIVE_CONNECTION_KEYS = {
    "password",
    "pwd",
    "token",
    "access token",
    "client secret",
    "secret",
    "user id",
    "uid",
}


def dax_to_pandas(
    dax_query: str,
    conn_str: str,
    *,
    transport: str = TRANSPORT_MSOLAP,
    dataset_id: str | None = None,
    auth_mode: str = AUTH_AZURE_CLI,
    access_token_env: str | None = None,
    api_base_url: str = DEFAULT_POWERBI_API_BASE_URL,
    impersonated_user_name: str | None = None,
    connection_timeout_seconds: int = 300,
    command_timeout_seconds: int = 1800,
    max_rows: int | None = None,
    profile: bool = False,
) -> pd.DataFrame:
    """Execute an ad hoc DAX query and return a DataFrame.

    If conn_str starts with MOCK://, uses the mock cube dispatcher for testing.
    When *profile* is True, phase timings are attached to ``df.attrs["profiling"]``.
    """
    config = DAXQueryConfig(
        name="adhoc_query",
        connection_string=conn_str,
        dax_query=dax_query,
        transport=transport,
        dataset_id=dataset_id,
        auth_mode=auth_mode,
        access_token_env=access_token_env,
        api_base_url=api_base_url,
        impersonated_user_name=impersonated_user_name,
        connection_timeout_seconds=connection_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        max_rows=max_rows,
    )
    if transport == TRANSPORT_POWERBI_REST:
        return PowerBIRestExecutor().execute(config, profile=profile)
    return DAXExecutor(connection_string=conn_str).execute(config, profile=profile)


def redact_connection_string(connection_string: str) -> str:
    redacted_parts: list[str] = []
    for raw_part in connection_string.split(";"):
        part = raw_part.strip()
        if not part:
            continue
        if "=" not in part:
            redacted_parts.append(part)
            continue

        key, value = part.split("=", 1)
        if key.strip().lower() in _SENSITIVE_CONNECTION_KEYS:
            redacted_parts.append(f"{key}=***")
        else:
            redacted_parts.append(f"{key}={value}")
    return ";".join(redacted_parts)


class DAXExecutor:
    """Reusable ADODB-backed DAX executor."""

    def __init__(self, dispatcher: DispatchFn | None = None, connection_string: str | None = None):
        if dispatcher is not None:
            self._dispatcher = dispatcher
        elif connection_string is not None:
            self._dispatcher = get_dispatcher_for_connection(connection_string)
        else:
            self._dispatcher = None

    def execute(self, query: DAXQueryConfig, *, profile: bool = False) -> pd.DataFrame:
        if query.transport == TRANSPORT_POWERBI_REST:
            return PowerBIRestExecutor().execute(query, profile=profile)

        profiler = QueryProfiler(query_name=query.name, enabled=profile)
        conn = None
        cmd = None
        recordset = None

        try:
            dispatcher = self._dispatcher or get_dispatcher_for_connection(query.connection_string)
            profiler.start_phase("connect")
            logger.debug(
                "Opening ADODB connection for query '{}' using {}",
                query.name,
                redact_connection_string(query.connection_string),
            )
            conn = dispatcher("ADODB.Connection")
            conn.ConnectionTimeout = query.connection_timeout_seconds
            conn.CommandTimeout = query.command_timeout_seconds
            conn.Open(query.connection_string)
            profiler.stop_phase("connect")

            profiler.start_phase("execute")
            cmd = dispatcher("ADODB.Command")
            cmd.ActiveConnection = conn
            cmd.CommandText = query.dax_query
            cmd.CommandTimeout = query.command_timeout_seconds

            logger.debug(
                "Executing query '{}' (command timeout={}s, max_rows={})",
                query.name,
                query.command_timeout_seconds,
                query.max_rows,
            )
            recordset = cmd.Execute()[0]
            profiler.stop_phase("execute")

            profiler.start_phase("fetch")
            dataframe = _recordset_to_dataframe(recordset, max_rows=query.max_rows)
            profiler.stop_phase("fetch")

            logger.debug("Query '{}' returned shape {}", query.name, dataframe.shape)

            if profile:
                profiler.finalize()
                dataframe.attrs["profiling"] = profiler.to_response_field()

            return dataframe
        except Exception as exc:
            raise DAXExecutionError(_format_execution_error(query.name, exc)) from exc
        finally:
            _release_command(cmd)
            _safe_close(recordset)
            _safe_close(conn)


class PowerBIRestExecutor:
    """Power BI REST executeQueries-backed DAX executor."""

    def __init__(
        self,
        *,
        token_getter: Callable[[DAXQueryConfig], str] | None = None,
        opener: Callable[..., object] | None = None,
    ):
        self._token_getter = token_getter or _get_powerbi_access_token
        self._opener = opener or urllib.request.urlopen

    def execute(self, query: DAXQueryConfig, *, profile: bool = False) -> pd.DataFrame:
        profiler = QueryProfiler(query_name=query.name, enabled=profile)
        try:
            _powerbi_rest_dataset_only_base_url(query.api_base_url or DEFAULT_POWERBI_API_BASE_URL)

            profiler.start_phase("connect")
            token = self._token_getter(query)
            profiler.stop_phase("connect")

            profiler.start_phase("execute")
            payload = _execute_powerbi_rest_request(query, token, self._opener)
            profiler.stop_phase("execute")

            profiler.start_phase("fetch")
            dataframe = _powerbi_rest_payload_to_dataframe(payload, max_rows=query.max_rows)
            profiler.stop_phase("fetch")

            profiler.start_phase("normalize")
            dataframe = _normalize_dataframe(dataframe)
            profiler.stop_phase("normalize")

            logger.debug("REST query '{}' returned shape {}", query.name, dataframe.shape)

            if profile:
                profiler.finalize()
                dataframe.attrs["profiling"] = profiler.to_response_field()

            return dataframe
        except DAXExecutionError:
            raise
        except Exception as exc:
            raise DAXExecutionError(_format_execution_error(query.name, exc)) from exc


def _get_powerbi_access_token(query: DAXQueryConfig) -> str:
    if query.auth_mode == AUTH_ENV:
        env_name = query.access_token_env or DEFAULT_POWERBI_TOKEN_ENV
        token = os.getenv(env_name)
        if not token:
            raise DAXExecutionError(
                f"Power BI REST auth_mode='env' requires environment variable '{env_name}' to contain an access token."
            )
        return token

    if query.auth_mode != AUTH_AZURE_CLI:
        raise DAXExecutionError(f"Unsupported Power BI REST auth_mode '{query.auth_mode}'.")

    az_executable = _resolve_azure_cli_executable()
    completed = subprocess.run(
        [
            az_executable,
            "account",
            "get-access-token",
            "--resource",
            POWERBI_API_RESOURCE,
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        timeout=query.connection_timeout_seconds,
        check=False,
    )
    token = completed.stdout.strip()
    if completed.returncode != 0 or not token:
        detail = completed.stderr.strip() or completed.stdout.strip() or "Azure CLI returned no token."
        raise DAXExecutionError(
            "Failed to get a Power BI REST access token from Azure CLI. "
            "Run `az login --allow-no-subscriptions` or set auth_mode='env'. "
            f"Azure CLI detail: {detail}"
        )
    return token


def _resolve_azure_cli_executable() -> str:
    configured_path = os.getenv("AZURE_CLI_PATH")
    if configured_path:
        return configured_path

    if sys.platform == "win32":
        for executable_name in ("az.cmd", "az.exe"):
            path_executable = shutil.which(executable_name)
            if path_executable:
                return path_executable

        for candidate in WINDOWS_AZURE_CLI_PATHS:
            if os.path.isfile(candidate):
                return candidate

    else:
        path_executable = shutil.which("az")
        if path_executable:
            return path_executable

    path_executable = shutil.which("az")
    if path_executable and sys.platform != "win32":
        return path_executable

    raise DAXExecutionError(
        "Azure CLI executable not found. Install Azure CLI, add `az` to PATH, "
        "or set AZURE_CLI_PATH to the full az executable path."
    )


def _execute_powerbi_rest_request(
    query: DAXQueryConfig,
    token: str,
    opener: Callable[..., object],
) -> dict[str, object]:
    if not query.dataset_id:
        raise DAXExecutionError("Power BI REST transport requires dataset_id in the connection configuration.")

    api_base_url = _powerbi_rest_dataset_only_base_url(query.api_base_url or DEFAULT_POWERBI_API_BASE_URL)
    url = f"{api_base_url}/datasets/{query.dataset_id}/executeQueries"
    body: dict[str, object] = {
        "queries": [{"query": query.dax_query}],
        "serializerSettings": {"includeNulls": True},
    }
    if query.impersonated_user_name:
        body["impersonatedUserName"] = query.impersonated_user_name

    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with opener(request, timeout=query.command_timeout_seconds) as response:
            response_body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise DAXExecutionError(_format_rest_http_error(exc.code, error_body)) from exc
    except urllib.error.URLError as exc:
        raise DAXExecutionError(f"Power BI REST request failed: {exc.reason}") from exc

    try:
        payload = json.loads(response_body or "{}")
    except json.JSONDecodeError as exc:
        raise DAXExecutionError("Power BI REST returned a non-JSON response.") from exc
    if not isinstance(payload, dict):
        raise DAXExecutionError("Power BI REST returned an unexpected response shape.")
    return payload


def _powerbi_rest_dataset_only_base_url(api_base_url: str) -> str:
    api_base_url = api_base_url.rstrip("/")
    if "/groups/" in api_base_url.lower():
        raise DAXExecutionError(
            "Power BI REST transport uses the dataset-only executeQueries endpoint. "
            "Set api_base_url to 'https://api.powerbi.com/v1.0/myorg' and configure dataset_id separately; "
            "do not include '/groups/{workspace_id}' in api_base_url."
        )
    return api_base_url


def _powerbi_rest_payload_to_dataframe(payload: dict[str, object], *, max_rows: int | None) -> pd.DataFrame:
    _raise_rest_payload_error(payload.get("error"))

    results = payload.get("results")
    if not isinstance(results, list) or not results:
        return pd.DataFrame()

    result = results[0]
    if not isinstance(result, dict):
        raise DAXExecutionError("Power BI REST returned an unexpected query result shape.")
    _raise_rest_payload_error(result.get("error"))

    tables = result.get("tables")
    if not isinstance(tables, list) or not tables:
        return pd.DataFrame()
    if len(tables) > 1:
        raise DAXExecutionError("Power BI REST returned more than one result table.")

    table = tables[0]
    if not isinstance(table, dict):
        raise DAXExecutionError("Power BI REST returned an unexpected table result shape.")
    _raise_rest_payload_error(table.get("error"))

    rows = table.get("rows", [])
    if not isinstance(rows, list):
        raise DAXExecutionError("Power BI REST returned an unexpected rows shape.")

    dataframe = pd.DataFrame(rows)
    if max_rows is not None:
        dataframe = dataframe.head(max_rows)
    return dataframe


def _raise_rest_payload_error(error: object) -> None:
    if not error:
        return
    if isinstance(error, dict):
        code = error.get("code", "PowerBIRestError")
        message = error.get("message", "Power BI REST query failed.")
        raise DAXExecutionError(f"{code}: {message}")
    raise DAXExecutionError(f"Power BI REST query failed: {error}")


def _format_rest_http_error(status_code: int, body: str) -> str:
    if not body:
        return f"Power BI REST request failed with HTTP {status_code}."
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return f"Power BI REST request failed with HTTP {status_code}: {body}"
    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict):
        code = error.get("code", f"HTTP {status_code}")
        message = error.get("message", body)
        return f"Power BI REST request failed with HTTP {status_code} ({code}): {message}"
    return f"Power BI REST request failed with HTTP {status_code}: {body}"


def _ensure_com_initialized() -> None:
    """Call CoInitialize on the current thread if not already done.

    FastMCP dispatches sync tool functions on arbitrary anyio worker threads
    via ``anyio.to_thread.run_sync``.  Each worker thread needs its own COM
    initialisation — pywin32 only auto-initialises on the *first* thread that
    touches COM.  Without this, the second (and subsequent) tool calls land on
    a fresh thread and fail with ``CoInitialize has not been called``.

    This is safe to call repeatedly: ``CoInitialize`` is a no-op if the
    thread's apartment is already initialised (returns ``S_FALSE``).
    """
    if sys.platform != "win32":
        return
    try:
        import pythoncom

        pythoncom.CoInitialize()
    except Exception:
        pass


def _com_dispatch(prog_id: str) -> object:
    """Create a COM object, ensuring the current thread is COM-initialised."""
    _ensure_com_initialized()
    import win32com.client

    return win32com.client.Dispatch(prog_id)


def _default_dispatcher() -> DispatchFn:
    try:
        import win32com.client  # noqa: F401 – validate availability
    except ImportError as exc:
        raise DAXExecutionError(
            "pywin32 is required to execute DAX queries. Install the project dependencies on Windows. "
            f"If ADODB/MSOLAP errors continue, install the Analysis Services client libraries: {MSOLAP_INSTALL_URL}"
        ) from exc

    return _com_dispatch


def get_dispatcher_for_connection(connection_string: str) -> DispatchFn:
    """Return the appropriate dispatcher for a connection string.

    If the connection string starts with MOCK://, returns a mock dispatcher
    for testing and remote development. Otherwise returns the real COM dispatcher.
    """
    from .mock_cube import create_mock_dispatcher, is_mock_connection

    if is_mock_connection(connection_string):
        return create_mock_dispatcher()
    return _default_dispatcher()


def _iter_recordset_rows(
    recordset: object, fields: object, num_fields: int, *, max_rows: int | None
) -> Iterator[list[object]]:
    """Yield one row at a time from a Recordset using incremental MoveNext()."""
    count = 0
    while not recordset.EOF:
        if max_rows is not None and count >= max_rows:
            break
        yield [_strip_timezone(fields[i].Value) for i in range(num_fields)]
        count += 1
        recordset.MoveNext()


def _recordset_to_dataframe(recordset: object, *, max_rows: int | None) -> pd.DataFrame:
    fields = getattr(recordset, "Fields")
    columns = [field.Name for field in fields]
    rows = list(_iter_recordset_rows(recordset, fields, len(columns), max_rows=max_rows))
    dataframe = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
    return _normalize_dataframe(dataframe)


def _normalize_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.copy()
    dataframe.columns = [_normalize_column_name(column_name) for column_name in dataframe.columns]

    for column_name in dataframe.select_dtypes(include=["object"]).columns:
        non_null = dataframe[column_name].dropna()
        if non_null.empty:
            continue

        first_valid = non_null.iloc[0]
        if _is_timezone_aware(first_valid):
            dataframe[column_name] = dataframe[column_name].apply(_strip_timezone)
            dataframe[column_name] = pd.to_datetime(dataframe[column_name])
            continue

        numeric_values = pd.to_numeric(non_null, errors="coerce")
        if numeric_values.notna().all():
            dataframe[column_name] = pd.to_numeric(dataframe[column_name], errors="coerce")

    return dataframe


def _normalize_column_name(column_name: str) -> str:
    if "[" in column_name and "]" in column_name:
        column_name = column_name[column_name.find("[") + 1 : column_name.find("]")]
    return column_name.replace(" ", "_")


def _is_timezone_aware(value: object) -> bool:
    return hasattr(value, "tzinfo") and getattr(value, "tzinfo") is not None


def _strip_timezone(value: object) -> object:
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value


def _safe_close(obj: object | None) -> None:
    if obj is None:
        return
    close_method = getattr(obj, "Close", None)
    if callable(close_method):
        with suppress(Exception):
            close_method()


def _release_command(cmd: object | None) -> None:
    if cmd is None:
        return
    with suppress(Exception):
        cmd.ActiveConnection = None


def _format_execution_error(query_name: str, exc: Exception) -> str:
    base_message = f"Failed to execute query '{query_name}': {exc}"
    if _looks_like_missing_msolap(exc):
        return (
            f"{base_message} Possible issue: the MSOLAP / Analysis Services client libraries may not be "
            f"installed on this machine. Install them here: {MSOLAP_INSTALL_URL}"
        )
    return base_message


def _looks_like_missing_msolap(exc: Exception) -> bool:
    message = str(exc).lower()
    match_terms = [
        "msolap",
        "provider is not registered",
        "class not registered",
        "provider cannot be found",
        "cannot be found. it may not be properly installed",
    ]
    return any(term in message for term in match_terms)

