from __future__ import annotations

from contextlib import suppress
from datetime import datetime
from typing import Callable, Iterator

import pandas as pd
from loguru import logger

from .exceptions import DAXExecutionError
from .models import DAXQueryConfig
from .profiling import QueryProfiler

DispatchFn = Callable[[str], object]
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
    connection_timeout_seconds: int = 300,
    command_timeout_seconds: int = 1800,
    max_rows: int | None = None,
    profile: bool = False,
) -> pd.DataFrame:
    """Execute an ad hoc DAX query and return a DataFrame.

    If conn_str starts with MOCK://, uses the mock cube dispatcher for testing.
    When *profile* is True, phase timings are logged to stderr via loguru and
    attached to the returned DataFrame as ``df.attrs["profiling"]``.
    """
    config = DAXQueryConfig(
        name="adhoc_query",
        connection_string=conn_str,
        dax_query=dax_query,
        connection_timeout_seconds=connection_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        max_rows=max_rows,
    )
    executor = DAXExecutor(connection_string=conn_str)
    return executor.execute(config, profile=profile)


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
            self._dispatcher = _default_dispatcher()

    def execute(self, query: DAXQueryConfig, *, profile: bool = False) -> pd.DataFrame:
        profiler = QueryProfiler(query_name=query.name, enabled=profile)
        conn = None
        cmd = None
        recordset = None

        try:
            with profiler:
                with profiler.phase("connect"):
                    logger.debug(
                        "Opening ADODB connection for query '{}' using {}",
                        query.name,
                        redact_connection_string(query.connection_string),
                    )
                    conn = self._dispatcher("ADODB.Connection")
                    conn.ConnectionTimeout = query.connection_timeout_seconds
                    conn.CommandTimeout = query.command_timeout_seconds
                    conn.Open(query.connection_string)

                with profiler.phase("execute"):
                    cmd = self._dispatcher("ADODB.Command")
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

                with profiler.phase("fetch"):
                    dataframe = _recordset_to_dataframe(recordset, max_rows=query.max_rows)

                with profiler.phase("normalize"):
                    dataframe = _normalize_dataframe(dataframe)

                logger.debug("Query '{}' returned shape {}", query.name, dataframe.shape)

            if profile:
                dataframe.attrs["profiling"] = profiler.to_response_field()

            return dataframe
        except Exception as exc:
            raise DAXExecutionError(_format_execution_error(query.name, exc)) from exc
        finally:
            _release_command(cmd)
            _safe_close(recordset)
            _safe_close(conn)


def _default_dispatcher() -> DispatchFn:
    try:
        import win32com.client
    except ImportError as exc:
        raise DAXExecutionError(
            "pywin32 is required to execute DAX queries. Install the project dependencies on Windows. "
            f"If ADODB/MSOLAP errors continue, install the Analysis Services client libraries: {MSOLAP_INSTALL_URL}"
        ) from exc

    return win32com.client.Dispatch


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
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


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

