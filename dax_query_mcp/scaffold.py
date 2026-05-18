"""Scaffold a portable DAX query workspace.

Creates a standalone folder with a saved .dax query, a bare-bones executor
script (notebook-friendly), and a pyproject.toml so users can `uv run` it
independently.
"""

from __future__ import annotations

import json
import shutil
import textwrap
from pathlib import Path
from typing import Any

from .models import (
    AUTH_AZURE_CLI,
    DEFAULT_POWERBI_API_BASE_URL,
    DEFAULT_POWERBI_TOKEN_ENV,
    SUPPORTED_AUTH_MODES,
    SUPPORTED_TRANSPORTS,
    TRANSPORT_MSOLAP,
    TRANSPORT_POWERBI_REST,
)


_SCAFFOLD_EXECUTOR_HELPERS = textwrap.dedent("""\
    POWERBI_API_RESOURCE = "https://analysis.windows.net/powerbi/api"
    WINDOWS_AZURE_CLI_PATHS = (
        r"C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
        r"C:\\Program Files (x86)\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
    )
    _ANSI_RE = re.compile(r'\\x1b\\[[0-9;]*m')


    def execute_dax(
        dax_query: str,
        connection: Mapping[str, Any],
        *,
        max_rows: int | None = None,
    ) -> pd.DataFrame:
        \"\"\"Execute DAX using the connection transport in CONNECTION.\"\"\"
        transport = str(connection.get("transport") or "msolap").lower()
        if transport == "powerbi_rest":
            return powerbi_rest_to_pandas(dax_query, connection, max_rows=max_rows)
        if transport != "msolap":
            raise RuntimeError(f"Unsupported DAX transport: {transport}")

        conn_str = str(connection.get("connection_string") or "")
        return dax_to_pandas(
            dax_query,
            conn_str,
            connection_timeout=_configured_int(connection, "connection_timeout_seconds", 300),
            timeout=_configured_int(connection, "command_timeout_seconds", 1800),
            max_rows=_effective_max_rows(connection, max_rows),
        )


    def dax_to_pandas(
        dax_query: str,
        conn_str: str,
        *,
        connection_timeout: int = 300,
        timeout: int = 1800,
        max_rows: int | None = None,
    ) -> pd.DataFrame:
        \"\"\"Execute a DAX query via COM/ADODB or MOCK:// and return a DataFrame.\"\"\"
        if _is_mock_connection(conn_str):
            return _normalize_dataframe(_mock_to_pandas(dax_query, max_rows=max_rows))

        try:
            import win32com.client  # Windows-only
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "pywin32 is required for MSOLAP/ADODB connections (it provides win32com). "
                "Run this generated workspace with uv so its dependencies are installed."
            ) from exc

        conn = None
        cmd = None
        recordset = None
        try:
            conn = win32com.client.Dispatch("ADODB.Connection")
            conn.ConnectionTimeout = connection_timeout
            conn.CommandTimeout = timeout
            conn.Open(conn_str)

            cmd = win32com.client.Dispatch("ADODB.Command")
            cmd.ActiveConnection = conn
            cmd.CommandText = dax_query
            cmd.CommandTimeout = timeout

            recordset = cmd.Execute()[0]
            fields = [recordset.Fields(i).Name for i in range(recordset.Fields.Count)]
            rows = recordset.GetRows(max_rows) if max_rows else recordset.GetRows()
        finally:
            if cmd is not None:
                with suppress(Exception):
                    cmd.ActiveConnection = None
            for obj in (recordset, conn):
                close = getattr(obj, "Close", None)
                if callable(close):
                    with suppress(Exception):
                        close()

        data: dict[str, list[Any]] = {}
        for i, name in enumerate(fields):
            vals = [_strip_tz(v) for v in rows[i]] if rows and i < len(rows) else []
            data[str(name)] = list(vals)

        return _normalize_dataframe(pd.DataFrame(data))


    def powerbi_rest_to_pandas(
        dax_query: str,
        connection: Mapping[str, Any],
        *,
        max_rows: int | None = None,
    ) -> pd.DataFrame:
        \"\"\"Execute a DAX query through Power BI REST executeQueries.\"\"\"
        dataset_id = str(connection.get("dataset_id") or "").strip()
        if not dataset_id or dataset_id == "YOUR_DATASET_ID_HERE":
            raise RuntimeError("Power BI REST transport requires dataset_id in CONNECTION.")

        api_base_url = _powerbi_rest_dataset_only_base_url(
            str(connection.get("api_base_url") or "https://api.powerbi.com/v1.0/myorg")
        )
        token = _get_powerbi_access_token(connection)
        url = f"{api_base_url}/datasets/{dataset_id}/executeQueries"
        body: dict[str, Any] = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True},
        }
        impersonated_user_name = connection.get("impersonated_user_name")
        if impersonated_user_name:
            body["impersonatedUserName"] = impersonated_user_name

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
            with urllib.request.urlopen(
                request,
                timeout=_configured_int(connection, "command_timeout_seconds", 1800),
            ) as response:
                response_body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(_format_rest_http_error(exc.code, error_body)) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Power BI REST request failed: {exc.reason}") from exc

        try:
            payload = json.loads(response_body or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError("Power BI REST returned a non-JSON response.") from exc

        if not isinstance(payload, dict):
            raise RuntimeError("Power BI REST returned an unexpected response shape.")
        return _normalize_dataframe(
            _powerbi_rest_payload_to_dataframe(payload, max_rows=_effective_max_rows(connection, max_rows))
        )


    def _get_powerbi_access_token(connection: Mapping[str, Any]) -> str:
        auth_mode = str(connection.get("auth_mode") or "azure_cli").lower()
        if auth_mode == "env":
            env_name = str(connection.get("access_token_env") or "POWERBI_ACCESS_TOKEN")
            token = os.getenv(env_name)
            if not token:
                raise RuntimeError(
                    f"Power BI REST auth_mode='env' requires environment variable '{env_name}'."
                )
            return token

        if auth_mode != "azure_cli":
            raise RuntimeError(f"Unsupported Power BI REST auth_mode: {auth_mode}")

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
            timeout=_configured_int(connection, "connection_timeout_seconds", 300),
            check=False,
        )
        token = completed.stdout.strip()
        if completed.returncode != 0 or not token:
            detail = completed.stderr.strip() or completed.stdout.strip() or "Azure CLI returned no token."
            raise RuntimeError(
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

        raise RuntimeError(
            "Azure CLI executable not found. Install Azure CLI, add `az` to PATH, "
            "or set AZURE_CLI_PATH to the full az executable path."
        )


    def _powerbi_rest_payload_to_dataframe(payload: dict[str, Any], *, max_rows: int | None) -> pd.DataFrame:
        _raise_rest_payload_error(payload.get("error"))

        results = payload.get("results")
        if not isinstance(results, list) or not results:
            return pd.DataFrame()

        result = results[0]
        if not isinstance(result, dict):
            raise RuntimeError("Power BI REST returned an unexpected query result shape.")
        _raise_rest_payload_error(result.get("error"))

        tables = result.get("tables")
        if not isinstance(tables, list) or not tables:
            return pd.DataFrame()
        if len(tables) > 1:
            raise RuntimeError("Power BI REST returned more than one result table.")

        table = tables[0]
        if not isinstance(table, dict):
            raise RuntimeError("Power BI REST returned an unexpected table result shape.")
        _raise_rest_payload_error(table.get("error"))

        rows = table.get("rows", [])
        if not isinstance(rows, list):
            raise RuntimeError("Power BI REST returned an unexpected rows shape.")

        dataframe = pd.DataFrame(rows)
        if max_rows is not None:
            dataframe = dataframe.head(max_rows)
        return dataframe


    def _raise_rest_payload_error(error: Any) -> None:
        if not error:
            return
        if isinstance(error, dict):
            code = error.get("code", "PowerBIRestError")
            message = error.get("message", "Power BI REST query failed.")
            raise RuntimeError(f"{code}: {message}")
        raise RuntimeError(f"Power BI REST query failed: {error}")


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


    def _powerbi_rest_dataset_only_base_url(api_base_url: str) -> str:
        api_base_url = api_base_url.rstrip("/")
        if "/groups/" in api_base_url.lower():
            raise RuntimeError(
                "Power BI REST transport uses the dataset-only executeQueries endpoint. "
                "Set api_base_url to 'https://api.powerbi.com/v1.0/myorg' and configure dataset_id separately; "
                "do not include '/groups/{workspace_id}' in api_base_url."
            )
        return api_base_url


    def _normalize_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe = dataframe.copy()
        dataframe.columns = [_clean_column_name(str(col)) for col in dataframe.columns]

        for column_name in dataframe.select_dtypes(include=["object"]).columns:
            non_null = dataframe[column_name].dropna()
            if non_null.empty:
                continue
            numeric_values = pd.to_numeric(non_null, errors="coerce")
            if numeric_values.notna().all():
                dataframe[column_name] = pd.to_numeric(dataframe[column_name], errors="coerce")

        return dataframe


    def _clean_column_name(name: str) -> str:
        \"\"\"Strip table prefixes like 'Calendar[Fiscal Month]' -> 'Fiscal_Month'.\"\"\"
        name = _ANSI_RE.sub("", name)
        if "[" in name and "]" in name:
            name = name[name.find("[") + 1 : name.find("]")]
        return name.replace(" ", "_")


    def _strip_tz(value: object) -> object:
        if isinstance(value, datetime) and getattr(value, "tzinfo", None) is not None:
            return value.replace(tzinfo=None)
        return value


    def _configured_int(connection: Mapping[str, Any], key: str, default: int) -> int:
        value = connection.get(key)
        if value is None:
            return default
        return int(value)


    def _effective_max_rows(connection: Mapping[str, Any], max_rows: int | None) -> int | None:
        if max_rows is not None:
            return max_rows
        configured = connection.get("max_rows")
        if configured is None:
            return None
        return int(configured)


    def _is_mock_connection(connection_string: str) -> bool:
        return connection_string.strip().upper().startswith("MOCK://")


    def _mock_products() -> list[dict[str, Any]]:
        return [
            {"ProductKey": 1, "ProductName": "Mountain Bike", "Category": "Bikes", "Price": 1500.00},
            {"ProductKey": 2, "ProductName": "Road Bike", "Category": "Bikes", "Price": 1200.00},
            {"ProductKey": 3, "ProductName": "Helmet", "Category": "Accessories", "Price": 50.00},
            {"ProductKey": 4, "ProductName": "Gloves", "Category": "Accessories", "Price": 25.00},
            {"ProductKey": 5, "ProductName": "Water Bottle", "Category": "Accessories", "Price": 10.00},
        ]


    def _mock_calendar() -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for month in range(1, 13):
            for day in range(1, 29):
                d = date(2025, month, day)
                rows.append({
                    "DateKey": int(d.strftime("%Y%m%d")),
                    "Date": datetime(d.year, d.month, d.day),
                    "Month": d.strftime("%B"),
                    "MonthNum": month,
                    "Year": 2025,
                    "Weekday": d.strftime("%A"),
                })
        return rows


    def _mock_sales() -> list[dict[str, Any]]:
        products = _mock_products()
        calendar = _mock_calendar()
        random.seed(42)
        rows: list[dict[str, Any]] = []
        for i in range(100):
            product = random.choice(products)
            cal = random.choice(calendar)
            qty = random.randint(1, 5)
            rows.append({
                "SalesKey": i + 1,
                "ProductKey": product["ProductKey"],
                "DateKey": cal["DateKey"],
                "Quantity": qty,
                "Amount": round(product["Price"] * qty, 2),
            })
        return rows


    def _mock_to_pandas(dax_query: str, *, max_rows: int | None = None) -> pd.DataFrame:
        upper = dax_query.upper().strip()
        if "ROW(" in upper:
            dataframe = _mock_row_to_pandas(dax_query)
        elif "SUMMARIZE" in upper or "SUMX" in upper or "SUM(" in upper:
            sales = _mock_sales()
            dataframe = pd.DataFrame({
                "Total Sales": [sum(row["Amount"] for row in sales)],
                "Total Quantity": [sum(row["Quantity"] for row in sales)],
            })
        elif re.search(r"\\bPRODUCTS\\b", upper) and "SALES" not in upper:
            dataframe = pd.DataFrame(_mock_products())
        elif re.search(r"\\bCALENDAR\\b", upper) and "SALES" not in upper:
            dataframe = pd.DataFrame(_mock_calendar())
        else:
            dataframe = pd.DataFrame(_mock_sales())

        if max_rows is not None:
            dataframe = dataframe.head(max_rows)
        return dataframe


    def _mock_row_to_pandas(dax_query: str) -> pd.DataFrame:
        match = re.search(r"ROW\\s*\\((.*)\\)", dax_query, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return pd.DataFrame({"Value": [1]})

        parts = [part.strip() for part in match.group(1).split(",")]
        row: dict[str, Any] = {}
        for i in range(0, len(parts) - 1, 2):
            name = parts[i].strip().strip('"').strip("'")
            row[name or f"Value_{i // 2 + 1}"] = _parse_mock_literal(parts[i + 1])
        return pd.DataFrame([row or {"Value": 1}])


    def _parse_mock_literal(value: str) -> Any:
        value = value.strip().rstrip(")")
        if value.startswith(("\"", "'")) and value.endswith(("\"", "'")):
            return value[1:-1]
        if value.upper() == "BLANK()":
            return None
        if value.upper() == "TRUE()":
            return True
        if value.upper() == "FALSE()":
            return False
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            return value
""")


_RUN_QUERY_TEMPLATE = textwrap.dedent("""\
    \"\"\"Bare-bones DAX executor - run with `uv run run_query.py`.

    Supports MSOLAP/ADODB, Power BI REST executeQueries, and MOCK:// demo connections.
    \"\"\"

    from __future__ import annotations

    import json
    import os
    import random
    import re
    import shutil
    import subprocess
    import sys
    import urllib.error
    import urllib.request
    from contextlib import suppress
    from datetime import date, datetime
    from pathlib import Path
    from typing import Any, Mapping

    import pandas as pd

    # -- Connection ---------------------------------------------------------
    # Edit this dict with your Power BI / SSAS connection details.
    CONNECTION = json.loads(__CONNECTION_CONFIG_JSON__)
    CONNECTION_STRING = str(CONNECTION.get("connection_string") or "")

    # -- Query --------------------------------------------------------------
    QUERY_FILE = Path(__file__).parent / "queries" / "__QUERY_FILENAME__"


    __EXECUTOR_HELPERS__


    if __name__ == "__main__":
        from rich.console import Console
        from rich.table import Table

        if not QUERY_FILE.exists():
            print(f"Query file not found: {QUERY_FILE}")
            sys.exit(1)

        dax = QUERY_FILE.read_text(encoding="utf-8")
        console = Console()
        console.print(f"[bold]Running {QUERY_FILE.name} via {CONNECTION.get('transport', 'msolap')} ...[/bold]")
        df = execute_dax(dax, CONNECTION)
        console.print(f"[green]{len(df)} rows x {len(df.columns)} cols[/green]\\n")

        table = Table(show_lines=True, title=QUERY_FILE.stem)
        for col in df.columns:
            table.add_column(str(col), header_style="bold cyan", style="white")
        for _, row in df.head(50).iterrows():
            table.add_row(*[str(v) for v in row])
        if len(df) > 50:
            table.caption = f"Showing 50 of {len(df)} rows"
        console.print(table)
""")


_RUN_QUERIES_TEMPLATE = textwrap.dedent("""\
    \"\"\"Run a DAX query pack with transport-aware execution.

    Supports MSOLAP/ADODB, Power BI REST executeQueries, and MOCK:// demo connections.
    \"\"\"

    from __future__ import annotations

    import argparse
    import json
    import os
    import random
    import re
    import shutil
    import subprocess
    import sys
    import time
    import urllib.error
    import urllib.request
    import uuid
    from contextlib import suppress
    from datetime import date, datetime
    from pathlib import Path
    from typing import Any, Mapping

    import pandas as pd

    # -- Connections --------------------------------------------------------
    # Edit these dicts with your Power BI / SSAS connection details.
    CONNECTIONS = json.loads(__CONNECTIONS_CONFIG_JSON__)


    __EXECUTOR_HELPERS__


    QUERIES = json.loads(__QUERIES_JSON__)
    _PLACEHOLDER_RE = re.compile(r"\\{\\{\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\}\\}")


    def build_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Run exported DAX query-pack queries.")
        parser.add_argument("--list", action="store_true", help="List queries and exit.")
        parser.add_argument("--only", action="append", default=[], help="Run only a query id. Repeatable.")
        parser.add_argument("--tag", action="append", default=[], help="Run queries matching any tag. Repeatable.")
        parser.add_argument("--param", action="append", default=[], help="Parameter override as name=value. Repeatable.")
        parser.add_argument("--output", default="results", help="Output directory for result files and run_log.json.")
        parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Result file format.")
        parser.add_argument("--continue-on-error", action="store_true", help="Continue running after a query fails.")
        parser.add_argument("--fail-fast", action="store_true", help="Stop on first failure (default unless --continue-on-error).")
        parser.add_argument("--max-rows", type=int, help="Override connection max_rows for every query.")
        return parser


    def parse_params(raw_params: list[str]) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for raw in raw_params:
            if "=" not in raw:
                raise SystemExit(f"Invalid --param value '{raw}'. Use name=value.")
            name, value = raw.split("=", 1)
            parsed[name.strip()] = value
        return parsed


    def selected_queries(args: argparse.Namespace) -> list[dict[str, Any]]:
        queries = list(QUERIES)
        if args.only:
            wanted = set(args.only)
            queries = [entry for entry in queries if query_id(entry) in wanted]
        if args.tag:
            wanted_tags = set(args.tag)
            queries = [
                entry for entry in queries
                if wanted_tags.intersection(set(entry.get("tags", [])))
            ]
        return queries


    def query_id(entry: Mapping[str, Any]) -> str:
        return str(entry.get("id") or entry.get("name") or "").strip()


    def query_connection(entry: Mapping[str, Any]) -> str:
        return str(entry.get("connection_name") or entry.get("connection") or "").strip()


    def query_description(entry: Mapping[str, Any]) -> str:
        return str(entry.get("description") or "")


    def render_query_template(dax: str, parameters: Mapping[str, Any], values: Mapping[str, str]) -> str:
        placeholders = set(_PLACEHOLDER_RE.findall(dax))
        missing_defs = sorted(placeholders.difference(parameters))
        if missing_defs:
            raise RuntimeError(f"Undeclared query parameter(s): {', '.join(missing_defs)}")

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            definition = parameters[name]
            if name in values:
                value: Any = values[name]
            elif "default" in definition:
                value = definition["default"]
            elif definition.get("required"):
                raise RuntimeError(f"Missing required query parameter: {name}")
            else:
                value = ""
            return render_dax_literal(value, definition)

        return _PLACEHOLDER_RE.sub(replace, dax)


    def render_dax_literal(value: Any, definition: Mapping[str, Any]) -> str:
        param_type = str(definition.get("type") or "text")
        allowed_values = definition.get("allowed_values") or []
        if param_type == "text":
            validate_allowed_value(value, allowed_values)
            return dax_string(str(value))
        if param_type == "number":
            validate_allowed_value(value, allowed_values)
            if isinstance(value, bool):
                raise RuntimeError("Boolean values are not valid number parameters.")
            number = float(value)
            return str(int(number)) if number.is_integer() else str(number)
        if param_type == "date":
            validate_allowed_value(value, allowed_values)
            parsed = datetime.fromisoformat(str(value)).date()
            return f"DATE({parsed.year}, {parsed.month}, {parsed.day})"
        if param_type == "boolean":
            validate_allowed_value(value, allowed_values)
            return "TRUE()" if parse_bool(value) else "FALSE()"
        if param_type == "list[text]":
            values = value if isinstance(value, list) else [item.strip() for item in str(value).split(",") if item.strip()]
            for item in values:
                validate_allowed_value(item, allowed_values)
            return "{" + ", ".join(dax_string(str(item)) for item in values) + "}"
        raise RuntimeError(f"Unsupported query parameter type: {param_type}")


    def validate_allowed_value(value: Any, allowed_values: list[Any]) -> None:
        if allowed_values and value not in allowed_values:
            raise RuntimeError(f"Parameter value {value!r} is not in allowed_values.")


    def parse_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
        raise RuntimeError(f"Invalid boolean parameter: {value!r}")


    def dax_string(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'


    def write_result(df: pd.DataFrame, output_dir: Path, query_name: str, result_format: str) -> str:
        output_dir.mkdir(parents=True, exist_ok=True)
        if result_format == "csv":
            result_path = output_dir / f"{query_name}.csv"
            df.to_csv(result_path, index=False)
        else:
            result_path = output_dir / f"{query_name}.json"
            df.to_json(result_path, orient="records", indent=2, date_format="iso")
        schema_path = output_dir / f"{query_name}.schema.json"
        schema_path.write_text(
            json.dumps(
                {
                    "columns": [
                        {"name": str(column), "pandas_dtype": str(dtype)}
                        for column, dtype in zip(df.columns, df.dtypes)
                    ]
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        return str(result_path)


    def print_preview(console: Any, query_name: str, df: pd.DataFrame) -> None:
        from rich.table import Table

        table = Table(show_lines=True, title=query_name)
        for col in df.columns:
            table.add_column(str(col), header_style="bold cyan")
        for _, row in df.head(20).iterrows():
            table.add_row(*[str(v) for v in row])
        if len(df) > 20:
            table.caption = f"Showing 20 of {len(df)} rows"
        console.print(table)


    def main() -> int:
        from rich.console import Console

        args = build_parser().parse_args()
        console = Console()

        if args.list:
            for entry in QUERIES:
                tags = ", ".join(entry.get("tags", []))
                console.print(f"{query_id(entry)}\\t{query_connection(entry)}\\t{tags}\\t{query_description(entry)}")
            return 0

        output_dir = Path(args.output)
        param_values = parse_params(args.param)
        run_log: dict[str, Any] = {
            "run_log_version": "1.0",
            "run_id": str(uuid.uuid4()),
            "started_at": datetime.now().isoformat(),
            "output_dir": str(output_dir),
            "queries": [],
        }
        failures = 0

        for entry in selected_queries(args):
            entry_id = query_id(entry)
            entry_connection = query_connection(entry)
            started = time.perf_counter()
            log_entry: dict[str, Any] = {
                "query_id": entry_id,
                "connection_name": entry_connection,
                "status": "running",
                "started_at": datetime.now().isoformat(),
            }
            try:
                qfile = Path(entry["file"])
                if not qfile.is_absolute():
                    qfile = Path(__file__).parent / qfile
                connection = CONNECTIONS.get(entry_connection)
                if connection is None:
                    raise RuntimeError(f"Missing connection config: {entry_connection}")
                if not qfile.exists():
                    raise RuntimeError(f"Missing query file: {qfile}")

                dax = render_query_template(
                    qfile.read_text(encoding="utf-8"),
                    entry.get("parameters", {}),
                    param_values,
                )
                console.print(f"\\n[bold]Running {entry_id} via {connection.get('transport', 'msolap')} ...[/bold]")
                console.print(f"  [dim]{query_description(entry)}[/dim]")
                df = execute_dax(dax, connection, max_rows=args.max_rows)
                output_path = write_result(df, output_dir, entry_id, args.format)
                console.print(f"  [green]{len(df)} rows x {len(df.columns)} cols[/green] -> {output_path}")
                print_preview(console, entry_id, df)
                log_entry.update({
                    "status": "success",
                    "row_count": int(len(df)),
                    "column_count": int(len(df.columns)),
                    "output_path": output_path,
                })
            except Exception as exc:  # noqa: BLE001 - generated runner records failures in run_log.
                failures += 1
                console.print(f"[red]Query failed: {entry_id}[/red] {exc}")
                log_entry.update({
                    "status": "failure",
                    "error_class": exc.__class__.__name__,
                    "error_message": str(exc),
                })
                if not args.continue_on_error:
                    run_log["queries"].append(log_entry)
                    break
            finally:
                log_entry["duration_ms"] = round((time.perf_counter() - started) * 1000, 2)
                log_entry["ended_at"] = datetime.now().isoformat()
                if log_entry not in run_log["queries"]:
                    run_log["queries"].append(log_entry)

        run_log["ended_at"] = datetime.now().isoformat()
        run_log["failure_count"] = failures
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "run_log.json").write_text(json.dumps(run_log, indent=2), encoding="utf-8")
        return 1 if failures else 0


    if __name__ == "__main__":
        raise SystemExit(main())
""")


_STREAMLIT_QUERY_PACK_TEMPLATE = textwrap.dedent("""\
    \"\"\"Interactive DAX query-pack explorer.

    Run with: streamlit run streamlit_app.py
    \"\"\"

    from __future__ import annotations

    import hashlib
    import json
    import os
    import random
    import re
    import shutil
    import subprocess
    import sys
    import time
    import urllib.error
    import urllib.request
    from contextlib import suppress
    from datetime import date, datetime
    from pathlib import Path
    from typing import Any, Mapping

    import pandas as pd
    import streamlit as st

    APP_TITLE = __APP_TITLE__
    CONNECTIONS = json.loads(__CONNECTIONS_CONFIG_JSON__)
    QUERIES = json.loads(__QUERIES_JSON__)
    _PLACEHOLDER_RE = re.compile(r"\\{\\{\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\}\\}")
    _CATEGORY_FILTER_LIMIT = 100
    _HISTORY_LIMIT = 8
    _NONE = "(none)"


    __EXECUTOR_HELPERS__


    def query_id(entry: Mapping[str, Any]) -> str:
        return str(entry.get("id") or entry.get("name") or "").strip()


    def query_connection(entry: Mapping[str, Any]) -> str:
        return str(entry.get("connection_name") or entry.get("connection") or "").strip()


    def query_display_name(entry: Mapping[str, Any]) -> str:
        return str(entry.get("display_name") or entry.get("description") or query_id(entry)).strip()


    def query_label(entry: Mapping[str, Any]) -> str:
        return f"{query_id(entry)} - {query_display_name(entry)}"


    def query_state_key(entry_id: str, suffix: str) -> str:
        return f"query:{entry_id}:{suffix}"


    def global_state_key(suffix: str) -> str:
        return f"query-pack:{suffix}"


    def widget_key(entry_id: str, suffix: str) -> str:
        return query_state_key(entry_id, suffix)


    def safe_file_name(value: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip()).strip("._")
        return safe or "query"


    def short_hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


    def all_tags() -> list[str]:
        return sorted({str(tag) for entry in QUERIES for tag in entry.get("tags", [])})


    def all_connections() -> list[str]:
        return sorted({query_connection(entry) for entry in QUERIES if query_connection(entry)})


    def filtered_query_entries(
        *,
        search_text: str,
        selected_tags: list[str],
        selected_connections: list[str],
    ) -> list[Mapping[str, Any]]:
        search = search_text.strip().lower()
        selected_tag_set = set(selected_tags)
        selected_connection_set = set(selected_connections)
        filtered: list[Mapping[str, Any]] = []
        for entry in QUERIES:
            haystack = " ".join(
                [
                    query_id(entry),
                    query_display_name(entry),
                    str(entry.get("description") or ""),
                    " ".join(str(tag) for tag in entry.get("tags", [])),
                    query_connection(entry),
                ]
            ).lower()
            if search and search not in haystack:
                continue
            if selected_tag_set and not selected_tag_set.intersection(set(entry.get("tags", []))):
                continue
            if selected_connection_set and query_connection(entry) not in selected_connection_set:
                continue
            filtered.append(entry)
        return filtered


    def prune_stale_query_state(active_entry_id: str) -> None:
        active_prefix = f"query:{active_entry_id}:"
        for key in list(st.session_state.keys()):
            if isinstance(key, str) and key.startswith("query:") and not key.startswith(active_prefix):
                del st.session_state[key]


    def load_query_text(entry: Mapping[str, Any]) -> str:
        inline_dax = entry.get("dax_query")
        if inline_dax:
            return str(inline_dax)

        query_file = str(entry.get("file") or "").strip()
        if not query_file:
            raise RuntimeError(f"Query '{query_id(entry)}' has neither dax_query nor file.")
        query_path = Path(__file__).parent / query_file
        if not query_path.exists():
            raise RuntimeError(f"Query file not found: {query_path}")
        return query_path.read_text(encoding="utf-8")


    def render_query_template(dax: str, parameters: Mapping[str, Any], values: Mapping[str, Any]) -> str:
        placeholders = set(_PLACEHOLDER_RE.findall(dax))
        missing_defs = sorted(placeholders.difference(parameters))
        if missing_defs:
            raise RuntimeError(f"Undeclared query parameter(s): {', '.join(missing_defs)}")

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            definition = parameters[name]
            if name in values:
                value: Any = values[name]
            elif "default" in definition:
                value = definition["default"]
            elif definition.get("required"):
                raise RuntimeError(f"Missing required query parameter: {name}")
            else:
                value = [] if str(definition.get("type") or "text") == "list[text]" else ""
            return render_dax_literal(value, definition)

        return _PLACEHOLDER_RE.sub(replace, dax)


    def render_dax_literal(value: Any, definition: Mapping[str, Any]) -> str:
        param_type = str(definition.get("type") or "text")
        allowed_values = definition.get("allowed_values") or []
        if param_type == "text":
            validate_allowed_value(value, allowed_values)
            return dax_string(str(value))
        if param_type == "number":
            validate_allowed_value(value, allowed_values)
            if isinstance(value, bool):
                raise RuntimeError("Boolean values are not valid number parameters.")
            number = float(value)
            return str(int(number)) if number.is_integer() else str(number)
        if param_type == "date":
            validate_allowed_value(value, allowed_values)
            parsed = datetime.fromisoformat(str(value)).date()
            return f"DATE({parsed.year}, {parsed.month}, {parsed.day})"
        if param_type == "boolean":
            validate_allowed_value(value, allowed_values)
            return "TRUE()" if parse_bool(value) else "FALSE()"
        if param_type == "list[text]":
            if isinstance(value, list):
                values = value
            else:
                values = [item.strip() for item in str(value).split(",") if item.strip()]
            validate_allowed_value(values, allowed_values)
            return "{" + ", ".join(dax_string(str(item)) for item in values) + "}"
        raise RuntimeError(f"Unsupported query parameter type: {param_type}")


    def validate_allowed_value(value: Any, allowed_values: list[Any]) -> None:
        if not allowed_values:
            return
        values = value if isinstance(value, list) else [value]
        invalid = [item for item in values if item not in allowed_values]
        if invalid:
            raise RuntimeError(f"Parameter value(s) {invalid!r} are not in allowed_values.")


    def parse_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
        raise RuntimeError(f"Invalid boolean parameter: {value!r}")


    def dax_string(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'


    def parameter_inputs(entry_id: str, entry: Mapping[str, Any]) -> dict[str, Any]:
        values: dict[str, Any] = {}
        parameters = entry.get("parameters", {})
        if not parameters:
            st.info("This query has no parameters.")
            return values

        st.subheader("Parameters")
        for name, definition in parameters.items():
            param_type = str(definition.get("type") or "text")
            default = definition.get("default")
            allowed_values = definition.get("allowed_values") or []
            help_text = definition.get("description") or None
            label = f"{name} *" if definition.get("required") else name
            key = widget_key(entry_id, f"param:{name}")
            if param_type == "list[text]" and allowed_values:
                default_values = default if isinstance(default, list) else []
                values[name] = st.multiselect(label, allowed_values, default=default_values, help=help_text, key=key)
            elif allowed_values:
                index = allowed_values.index(default) if default in allowed_values else 0
                values[name] = st.selectbox(label, allowed_values, index=index, help=help_text, key=key)
            elif param_type == "number":
                values[name] = st.number_input(label, value=float(default or 0), help=help_text, key=key)
            elif param_type == "boolean":
                values[name] = st.checkbox(
                    label,
                    value=parse_bool(default) if default is not None else False,
                    help=help_text,
                    key=key,
                )
            elif param_type == "date":
                parsed_default = datetime.fromisoformat(str(default)).date() if default else date.today()
                values[name] = st.date_input(label, value=parsed_default, help=help_text, key=key).isoformat()
            elif param_type == "list[text]":
                if isinstance(default, list):
                    default_text = ", ".join(str(item) for item in default)
                else:
                    default_text = str(default or "")
                typed_value = st.text_input(label, value=default_text, help=help_text, key=key)
                values[name] = [item.strip() for item in typed_value.split(",") if item.strip()]
            else:
                values[name] = st.text_input(label, value=str(default or ""), help=help_text, key=key)
        return values


    @st.cache_data(show_spinner=False)
    def run_cached_query(connection_name: str, rendered_dax: str, max_rows: int | None) -> pd.DataFrame:
        connection = CONNECTIONS[connection_name]
        return execute_dax(rendered_dax, connection, max_rows=max_rows)


    def run_uncached_query(connection_name: str, rendered_dax: str, max_rows: int | None) -> pd.DataFrame:
        connection = CONNECTIONS[connection_name]
        return execute_dax(rendered_dax, connection, max_rows=max_rows)


    def format_execution_error(connection_name: str, exc: Exception) -> str:
        connection = CONNECTIONS.get(connection_name, {})
        transport = str(connection.get("transport") or "msolap")
        hint = ""
        if transport == "msolap" and sys.platform != "win32":
            hint = " MSOLAP/ADODB execution requires Windows with the provider installed."
        elif transport == "powerbi_rest":
            hint = " Power BI REST execution needs Azure CLI login or the configured token environment variable."
        return f"{exc.__class__.__name__}: {exc}{hint}"


    def remember_run(
        *,
        entry_id: str,
        connection_name: str,
        rendered_dax: str,
        parameters: Mapping[str, Any],
        status: str,
        row_count: int = 0,
        column_count: int = 0,
        duration_ms: float | None = None,
        error: str = "",
    ) -> None:
        history = list(st.session_state.get(global_state_key("history"), []))
        history.insert(
            0,
            {
                "time": datetime.now().isoformat(timespec="seconds"),
                "query_id": entry_id,
                "connection": connection_name,
                "status": status,
                "rows": row_count,
                "columns": column_count,
                "duration_ms": duration_ms,
                "dax_hash": short_hash(rendered_dax),
                "parameters": dict(parameters),
                "error": error,
            },
        )
        st.session_state[global_state_key("history")] = history[:_HISTORY_LIMIT]


    def dataframe_profile(df: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for column in df.columns:
            series = df[column]
            non_null = int(series.notna().sum())
            nulls = int(series.isna().sum())
            try:
                unique_count = int(series.nunique(dropna=True))
            except TypeError:
                unique_count = int(series.dropna().astype(str).nunique())
            sample_values = series.dropna().head(3).astype(str).tolist()
            rows.append(
                {
                    "column": column,
                    "dtype": str(series.dtype),
                    "non_null": non_null,
                    "nulls": nulls,
                    "null_pct": round((nulls / len(df) * 100), 2) if len(df) else 0,
                    "unique": unique_count,
                    "sample": ", ".join(sample_values),
                }
            )
        return pd.DataFrame(rows)


    def schema_payload(df: pd.DataFrame) -> dict[str, Any]:
        return {
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": [
                {
                    "name": str(column),
                    "dtype": str(df[column].dtype),
                    "non_null": int(df[column].notna().sum()),
                    "nulls": int(df[column].isna().sum()),
                }
                for column in df.columns
            ],
        }


    def numeric_columns(df: pd.DataFrame) -> list[str]:
        return df.select_dtypes(include="number").columns.tolist()


    def datetime_columns(df: pd.DataFrame) -> list[str]:
        return df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()


    def bounded_category_columns(df: pd.DataFrame, *, limit: int = _CATEGORY_FILTER_LIMIT) -> list[str]:
        columns: list[str] = []
        for column in df.columns:
            if column in numeric_columns(df) or column in datetime_columns(df):
                continue
            try:
                cardinality = int(df[column].dropna().astype(str).nunique())
            except TypeError:
                cardinality = limit + 1
            if 0 < cardinality <= limit:
                columns.append(column)
        return columns


    def render_result_metrics(df: pd.DataFrame) -> None:
        metric_cols = st.columns(4)
        metric_cols[0].metric("Rows", f"{len(df):,}")
        metric_cols[1].metric("Columns", f"{len(df.columns):,}")
        metric_cols[2].metric("Numeric columns", f"{len(numeric_columns(df)):,}")
        metric_cols[3].metric("Null cells", f"{int(df.isna().sum().sum()):,}")


    def render_filtered_dataframe(entry_id: str, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            st.info("The query returned no rows.")
            return df

        render_result_metrics(df)
        selected_columns = st.multiselect(
            "Visible columns",
            df.columns.tolist(),
            default=df.columns.tolist(),
            key=widget_key(entry_id, "visible-columns"),
        )
        filtered = df[selected_columns].copy() if selected_columns else df.copy()
        with st.expander("Column filters", expanded=False):
            for column in selected_columns:
                series = filtered[column]
                key_part = safe_file_name(column)
                if pd.api.types.is_numeric_dtype(series) and series.notna().any():
                    min_value = float(series.min())
                    max_value = float(series.max())
                    if min_value == max_value:
                        st.caption(f"{column}: single numeric value {min_value:g}; range filter skipped.")
                        continue
                    selected_range = st.slider(
                        f"{column} range",
                        min_value=min_value,
                        max_value=max_value,
                        value=(min_value, max_value),
                        key=widget_key(entry_id, f"filter:numeric:{key_part}"),
                    )
                    filtered = filtered[filtered[column].between(selected_range[0], selected_range[1])]
                elif pd.api.types.is_datetime64_any_dtype(series) and series.notna().any():
                    min_date = series.min().date()
                    max_date = series.max().date()
                    selected_dates = st.date_input(
                        f"{column} date range",
                        value=(min_date, max_date),
                        key=widget_key(entry_id, f"filter:date:{key_part}"),
                    )
                    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                        start_date, end_date = selected_dates
                        filtered = filtered[
                            (filtered[column].dt.date >= start_date)
                            & (filtered[column].dt.date <= end_date)
                        ]
                else:
                    unique_values = sorted(series.dropna().astype(str).unique().tolist())
                    if 0 < len(unique_values) <= _CATEGORY_FILTER_LIMIT:
                        selected_values = st.multiselect(
                            f"{column} values",
                            unique_values,
                            default=unique_values,
                            key=widget_key(entry_id, f"filter:category:{key_part}"),
                        )
                        if selected_values:
                            filtered = filtered[filtered[column].astype(str).isin(selected_values)]
                    elif len(unique_values) > _CATEGORY_FILTER_LIMIT:
                        st.caption(
                            f"{column}: {len(unique_values):,} distinct values; "
                            f"filter disabled above {_CATEGORY_FILTER_LIMIT:,} values."
                        )
        st.dataframe(filtered, use_container_width=True)
        st.caption(f"Showing {len(filtered):,} of {len(df):,} rows after filters.")
        return filtered


    def aggregate_chart_data(
        df: pd.DataFrame,
        *,
        x_col: str,
        y_col: str,
        series_col: str,
        agg: str,
    ) -> pd.DataFrame:
        aggfunc = "size" if agg == "count" else agg
        if series_col != _NONE:
            values = y_col if agg != "count" else None
            pivot = pd.pivot_table(
                df,
                index=x_col,
                columns=series_col,
                values=values,
                aggfunc=aggfunc,
                fill_value=0,
                sort=False,
            )
            return pivot
        if agg == "count":
            return df.groupby(x_col, dropna=False, sort=False)[y_col].count().to_frame(y_col)
        return df.groupby(x_col, dropna=False, sort=False)[y_col].agg(agg).to_frame(y_col)


    def render_chart_builder(entry_id: str, df: pd.DataFrame) -> None:
        if df.empty:
            st.info("Run a query with rows before building charts.")
            return
        numeric = numeric_columns(df)
        if not numeric:
            st.info("No numeric columns are available for charts.")
            return

        x_options = df.columns.tolist()
        default_x_index = next(
            (index for index, column in enumerate(x_options) if column not in numeric),
            0,
        )
        chart_cols = st.columns(4)
        chart_type = chart_cols[0].selectbox(
            "Chart type",
            ["bar", "line", "area", "scatter"],
            key=widget_key(entry_id, "chart:type"),
        )
        x_col = chart_cols[1].selectbox(
            "X axis",
            x_options,
            index=default_x_index,
            key=widget_key(entry_id, "chart:x"),
        )
        default_y_index = next(
            (index for index, column in enumerate(numeric) if column != x_col),
            0,
        )
        y_col = chart_cols[2].selectbox(
            "Y axis",
            numeric,
            index=default_y_index,
            key=widget_key(entry_id, "chart:y"),
        )
        agg = chart_cols[3].selectbox(
            "Aggregation",
            ["sum", "mean", "median", "min", "max", "count"],
            key=widget_key(entry_id, "chart:agg"),
        )
        possible_series = [_NONE] + [
            column for column in bounded_category_columns(df) if column not in {x_col, y_col}
        ]
        default_series_index = 1 if len(possible_series) > 1 else 0
        series_col = st.selectbox(
            "Series / color",
            possible_series,
            index=default_series_index,
            key=widget_key(entry_id, "chart:series"),
        )

        chart_data = df[[col for col in {x_col, y_col, series_col} if col != _NONE]].dropna()
        if chart_data.empty:
            st.info("No chartable rows remain after dropping nulls.")
            return
        if chart_type == "scatter":
            st.scatter_chart(chart_data, x=x_col, y=y_col)
            return

        aggregated = aggregate_chart_data(chart_data, x_col=x_col, y_col=y_col, series_col=series_col, agg=agg)
        if chart_type == "line":
            st.line_chart(aggregated)
        elif chart_type == "area":
            st.area_chart(aggregated)
        else:
            st.bar_chart(aggregated)
        preview = aggregated
        if aggregated.index.name in aggregated.columns:
            preview = aggregated.rename(columns={aggregated.index.name: f"{aggregated.index.name}_{agg}"})
        st.dataframe(preview.reset_index(), use_container_width=True)


    def render_pivot_builder(entry_id: str, df: pd.DataFrame) -> None:
        if df.empty:
            st.info("Run a query with rows before building a pivot.")
            return
        numeric = numeric_columns(df)
        if not numeric:
            st.info("No numeric columns are available for pivot values.")
            return

        bounded_columns = bounded_category_columns(df) + datetime_columns(df)
        row_fields = st.multiselect(
            "Rows",
            bounded_columns,
            default=bounded_columns[:1],
            key=widget_key(entry_id, "pivot:rows"),
        )
        value_col = st.selectbox("Value", numeric, key=widget_key(entry_id, "pivot:value"))
        column_options = [_NONE] + [column for column in bounded_columns if column not in row_fields]
        column_field = st.selectbox("Columns", column_options, key=widget_key(entry_id, "pivot:columns"))
        agg = st.selectbox(
            "Aggregation",
            ["sum", "mean", "median", "min", "max", "count"],
            key=widget_key(entry_id, "pivot:agg"),
        )
        if not row_fields:
            st.info("Choose at least one row field.")
            return
        values = value_col if agg != "count" else None
        pivot = pd.pivot_table(
            df,
            index=row_fields,
            columns=None if column_field == _NONE else column_field,
            values=values,
            aggfunc="size" if agg == "count" else agg,
            fill_value=0,
        )
        st.dataframe(pivot, use_container_width=True)
        st.download_button(
            "Download pivot CSV",
            pivot.reset_index().to_csv(index=False).encode("utf-8"),
            file_name=f"{entry_id}_pivot.csv",
            mime="text/csv",
            key=widget_key(entry_id, "download:pivot-csv"),
        )


    def render_downloads(entry_id: str, df: pd.DataFrame, filtered_df: pd.DataFrame, rendered_dax: str) -> None:
        if df.empty:
            st.info("Run a query with rows before downloading results.")
            return
        schema = schema_payload(filtered_df)
        st.download_button(
            "Download filtered CSV",
            filtered_df.to_csv(index=False).encode("utf-8"),
            file_name=f"{entry_id}.csv",
            mime="text/csv",
            key=widget_key(entry_id, "download:filtered-csv"),
        )
        st.download_button(
            "Download filtered JSON",
            filtered_df.to_json(orient="records", date_format="iso", indent=2).encode("utf-8"),
            file_name=f"{entry_id}.json",
            mime="application/json",
            key=widget_key(entry_id, "download:filtered-json"),
        )
        st.download_button(
            "Download schema JSON",
            json.dumps(schema, indent=2).encode("utf-8"),
            file_name=f"{entry_id}.schema.json",
            mime="application/json",
            key=widget_key(entry_id, "download:schema-json"),
        )
        st.download_button(
            "Download runnable DAX",
            rendered_dax.encode("utf-8"),
            file_name=f"{entry_id}.dax",
            mime="text/plain",
            key=widget_key(entry_id, "download:dax"),
        )
        with st.expander("Schema preview", expanded=False):
            st.json(schema)


    def render_history() -> None:
        st.subheader("Run history")
        history = st.session_state.get(global_state_key("history"), [])
        if not history:
            st.info("No query runs in this Streamlit session yet.")
            return
        history_df = pd.DataFrame(history)
        st.dataframe(history_df, use_container_width=True)


    def load_uploaded_dataframe(uploaded_file: Any) -> pd.DataFrame:
        file_name = str(getattr(uploaded_file, "name", "") or "").lower()
        if file_name.endswith(".json"):
            return pd.read_json(uploaded_file)
        return pd.read_csv(uploaded_file)


    def render_upload_workspace() -> None:
        st.subheader("Drag-and-drop data explorer")
        st.caption("Drop a CSV or JSON export here to use the same filters, chart builder, and pivot builder without rerunning DAX.")
        uploaded_file = st.file_uploader(
            "Drop CSV or JSON data",
            type=["csv", "json"],
            key=global_state_key("upload:file"),
        )
        if uploaded_file is None:
            st.info("Upload a CSV or JSON file to explore external or previously exported results.")
            return
        try:
            uploaded_df = load_uploaded_dataframe(uploaded_file)
        except Exception as exc:  # noqa: BLE001 - generated UI surfaces upload parsing issues directly.
            st.error(f"Could not read uploaded data: {exc}")
            return
        if uploaded_df.empty:
            st.warning("The uploaded file had no rows.")
            return
        uploaded_id = "uploaded-data"
        filtered_upload = render_filtered_dataframe(uploaded_id, uploaded_df)
        st.subheader("Uploaded data charts")
        render_chart_builder(uploaded_id, filtered_upload)
        st.subheader("Uploaded data pivot")
        render_pivot_builder(uploaded_id, filtered_upload)
        st.download_button(
            "Download filtered upload CSV",
            filtered_upload.to_csv(index=False).encode("utf-8"),
            file_name="uploaded_filtered.csv",
            mime="text/csv",
            key=global_state_key("upload:download-csv"),
        )


    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    if not QUERIES:
        st.warning("This query pack has no queries.")
        st.stop()

    st.sidebar.header("Query catalog")
    search_text = st.sidebar.text_input("Search", value="", key=global_state_key("search"))
    selected_tags = st.sidebar.multiselect("Tags", all_tags(), default=[], key=global_state_key("tags"))
    selected_connections = st.sidebar.multiselect(
        "Connections",
        all_connections(),
        default=[],
        key=global_state_key("connections"),
    )
    max_rows = st.sidebar.number_input("Max rows", min_value=1, value=1000, step=100, key=global_state_key("max-rows"))
    use_cache = st.sidebar.checkbox("Use cached query results", value=True, key=global_state_key("use-cache"))
    if st.sidebar.button("Clear cached query results", key=global_state_key("clear-cache")):
        run_cached_query.clear()
        st.sidebar.success("Query cache cleared.")

    filtered_queries = filtered_query_entries(
        search_text=search_text,
        selected_tags=selected_tags,
        selected_connections=selected_connections,
    )
    st.sidebar.caption(f"{len(filtered_queries):,} of {len(QUERIES):,} queries match.")
    if not filtered_queries:
        st.warning("No queries match the selected filters.")
        st.stop()

    labels = {query_label(entry): entry for entry in filtered_queries}
    selected_label = st.sidebar.selectbox("Query", list(labels), key=global_state_key("selected-query"))
    entry = labels[selected_label]
    entry_id = query_id(entry)
    prune_stale_query_state(entry_id)
    connection_name = query_connection(entry)
    if connection_name not in CONNECTIONS:
        st.error(f"Connection '{connection_name}' is not present in connections.json.")
        st.stop()

    try:
        dax_template = load_query_text(entry)
    except Exception as exc:  # noqa: BLE001 - generated UI surfaces file/config issues directly.
        st.error(str(exc))
        st.stop()

    st.caption(f"Connection: `{connection_name}`")
    summary_cols = st.columns(4)
    summary_cols[0].metric("Queries in pack", f"{len(QUERIES):,}")
    summary_cols[1].metric("Filtered queries", f"{len(filtered_queries):,}")
    summary_cols[2].metric("Tags", f"{len(entry.get('tags', [])):,}")
    summary_cols[3].metric("Parameters", f"{len(entry.get('parameters', {})):,}")

    explore_tab, profile_tab, downloads_tab, history_tab, catalog_tab, upload_tab = st.tabs(
        ["Explore", "Profile", "Downloads", "History", "Catalog", "Upload"]
    )

    result_df = st.session_state.get(query_state_key(entry_id, "last-result"))
    result_dax = st.session_state.get(query_state_key(entry_id, "last-dax"), "")
    filtered_df = result_df

    with explore_tab:
        detail_cols = st.columns([2, 1])
        with detail_cols[0]:
            st.subheader(query_display_name(entry))
            if entry.get("description"):
                st.write(entry["description"])
            if entry.get("tags"):
                st.caption("Tags: " + ", ".join(str(tag) for tag in entry.get("tags", [])))
            values = parameter_inputs(entry_id, entry)
        with detail_cols[1]:
            st.subheader("Query metadata")
            st.json(
                {
                    "id": entry_id,
                    "connection": connection_name,
                    "file": entry.get("file"),
                    "grain": entry.get("grain"),
                    "outputs": entry.get("outputs", {}),
                }
            )

        try:
            rendered_dax = render_query_template(dax_template, entry.get("parameters", {}), values)
        except Exception as exc:  # noqa: BLE001 - generated UI surfaces parameter issues directly.
            st.error(str(exc))
            st.stop()

        edit_dax = st.checkbox(
            "Edit rendered DAX before running",
            value=False,
            key=widget_key(entry_id, "edit-dax"),
            help="Edited DAX is treated as literal text. Parameter widgets will not be re-applied until you reset it.",
        )
        editor_key = widget_key(entry_id, "dax-editor")
        if edit_dax:
            if st.button("Reset editor from parameters", key=widget_key(entry_id, "reset-dax")):
                st.session_state[editor_key] = rendered_dax
            final_dax = st.text_area(
                "Runnable DAX",
                value=st.session_state.get(editor_key, rendered_dax),
                height=280,
                key=editor_key,
            )
            st.warning("DAX editor mode bypasses parameter rendering until you reset the editor.")
        else:
            final_dax = rendered_dax
            with st.expander("Rendered DAX", expanded=True):
                st.code(final_dax, language="dax")

        run_cols = st.columns([1, 1, 4])
        if run_cols[0].button("Run query", type="primary", key=widget_key(entry_id, "run")):
            started = time.perf_counter()
            try:
                with st.spinner(f"Running {entry_id} ..."):
                    runner = run_cached_query if use_cache else run_uncached_query
                    result_df = runner(connection_name, final_dax, int(max_rows))
            except Exception as exc:  # noqa: BLE001 - generated UI surfaces execution failures directly.
                duration_ms = round((time.perf_counter() - started) * 1000, 2)
                error = format_execution_error(connection_name, exc)
                remember_run(
                    entry_id=entry_id,
                    connection_name=connection_name,
                    rendered_dax=final_dax,
                    parameters=values,
                    status="failure",
                    duration_ms=duration_ms,
                    error=error,
                )
                st.error(error)
            else:
                duration_ms = round((time.perf_counter() - started) * 1000, 2)
                st.session_state[query_state_key(entry_id, "last-result")] = result_df
                st.session_state[query_state_key(entry_id, "last-dax")] = final_dax
                remember_run(
                    entry_id=entry_id,
                    connection_name=connection_name,
                    rendered_dax=final_dax,
                    parameters=values,
                    status="success",
                    row_count=len(result_df),
                    column_count=len(result_df.columns),
                    duration_ms=duration_ms,
                )
                st.success(f"{len(result_df):,} rows x {len(result_df.columns):,} columns in {duration_ms:,.0f} ms")

        if run_cols[1].button("Clear displayed result", key=widget_key(entry_id, "clear-result")):
            st.session_state.pop(query_state_key(entry_id, "last-result"), None)
            st.session_state.pop(query_state_key(entry_id, "last-dax"), None)
            result_df = None
            result_dax = ""
            filtered_df = None
            st.success("Displayed result cleared.")

        result_df = st.session_state.get(query_state_key(entry_id, "last-result"))
        result_dax = st.session_state.get(query_state_key(entry_id, "last-dax"), "")
        filtered_df = result_df
        st.markdown("---")
        if result_df is None:
            st.info("Run the query to see results, charts, and pivots here.")
        else:
            st.subheader("Results")
            filtered_df = render_filtered_dataframe(entry_id, result_df)
            st.subheader("Charts")
            render_chart_builder(entry_id, filtered_df)
            st.subheader("Pivot")
            render_pivot_builder(entry_id, filtered_df)

    with profile_tab:
        if result_df is None:
            st.info("Run a query to profile columns.")
        else:
            st.subheader("Column profile")
            st.dataframe(dataframe_profile(result_df), use_container_width=True)
            st.subheader("Raw schema")
            st.json(schema_payload(result_df))

    with downloads_tab:
        if result_df is None:
            st.info("Run a query before downloading artifacts.")
        else:
            render_downloads(entry_id, result_df, filtered_df if filtered_df is not None else result_df, result_dax)

    with history_tab:
        render_history()

    with catalog_tab:
        catalog_rows = [
            {
                "id": query_id(item),
                "display_name": query_display_name(item),
                "connection": query_connection(item),
                "tags": ", ".join(str(tag) for tag in item.get("tags", [])),
                "parameters": ", ".join(item.get("parameters", {}).keys()),
                "description": item.get("description", ""),
            }
            for item in filtered_queries
        ]
        st.dataframe(pd.DataFrame(catalog_rows), use_container_width=True)

    with upload_tab:
        render_upload_workspace()
""")


_PYPROJECT_TEMPLATE = textwrap.dedent("""\
    [project]
    name = "{project_name}"
    version = "0.1.0"
    description = "Portable DAX query workspace"
    requires-python = ">=3.12"
    dependencies = [
        "ipykernel>=6.29.0",
        "pandas>=2.3.0",
        "pywin32>=310; sys_platform == 'win32'",
        "rich>=13.0.0",
        "streamlit>=1.37.0",
    ]
""")


_STREAMLIT_PYPROJECT_TEMPLATE = textwrap.dedent("""\
    [project]
    name = "{project_name}"
    version = "0.1.0"
    description = "DAX Streamlit explorer"
    requires-python = ">=3.12"
    dependencies = [
        "pandas>=2.3.0",
        "pywin32>=310; sys_platform == 'win32'",
        "streamlit>=1.37.0",
    ]
""")


_STREAMLIT_UV_DEPENDENCY_ARGS = (
    '--with "streamlit>=1.37.0" '
    '--with "pandas>=2.3.0" '
    '--with "pywin32>=310; sys_platform == \'win32\'"'
)


_README_TEMPLATE = textwrap.dedent("""\
    # {project_name}

    Portable DAX query workspace scaffolded by **dax-query-mcp**.

    ## Quick start

    ```bash
    # run the script with uv (no install needed)
    uv run run_query.py

    # optional: materialize the uv-managed environment before repeated runs
    uv sync
    uv run --no-sync run_query.py

    # optional: register a kernel before opening notebook.ipynb in VS Code / Jupyter
    uv run python -m ipykernel install --user --name {project_name}
    ```

    ## Files

    | File | Purpose |
    |------|---------|
    | `run_query.py` | Bare-bones DAX executor for MSOLAP, Power BI REST, and MOCK:// connections |
    | `notebook.ipynb` | Jupyter notebook with executor + query pre-loaded |
    | `queries/{query_filename}` | Saved DAX query |
    | `pyproject.toml` | uv dependency manifest for the generated Python tools |

    ## Usage in a notebook

    Open `notebook.ipynb` directly. It has the same `CONNECTION` config,
    `execute_dax()` dispatcher, and query ready to run. You can also copy
    `execute_dax()` / `dax_to_pandas()` from `run_query.py` into another
    notebook.

    ## Connection config

    Edit `CONNECTION` in `run_query.py` or `notebook.ipynb`.

    - `transport: "msolap"` uses a Power BI / SSAS connection string through ADODB.
    - `transport: "powerbi_rest"` uses Power BI REST `executeQueries`; run
      `az login --allow-no-subscriptions` first or set `auth_mode: "env"` with
      an access token environment variable.
    - `connection_string: "MOCK://contoso"` runs the built-in demo data path.

    Do not commit generated workspaces that contain private dataset IDs,
    workspace names, connection strings, or tokens.
""")


def build_scaffold_connection_config(
    *,
    connection_string: str = "",
    transport: str = TRANSPORT_MSOLAP,
    dataset_id: str | None = None,
    auth_mode: str = AUTH_AZURE_CLI,
    access_token_env: str | None = None,
    api_base_url: str = DEFAULT_POWERBI_API_BASE_URL,
    impersonated_user_name: str | None = None,
    connection_timeout_seconds: int = 300,
    command_timeout_seconds: int = 1800,
    max_rows: int | None = None,
) -> dict[str, Any]:
    """Build the portable CONNECTION dict embedded in generated scaffolds."""
    if transport not in SUPPORTED_TRANSPORTS:
        raise ValueError(f"Unsupported transport: {transport}")
    if auth_mode not in SUPPORTED_AUTH_MODES:
        raise ValueError(f"Unsupported auth_mode: {auth_mode}")

    normalized_connection_string = " ".join((connection_string or "").split())
    if transport == TRANSPORT_POWERBI_REST:
        dataset_id = dataset_id or "YOUR_DATASET_ID_HERE"
    else:
        normalized_connection_string = normalized_connection_string or "YOUR_CONNECTION_STRING_HERE"

    return {
        "transport": transport,
        "connection_string": normalized_connection_string,
        "dataset_id": dataset_id,
        "auth_mode": auth_mode,
        "access_token_env": access_token_env or DEFAULT_POWERBI_TOKEN_ENV,
        "api_base_url": (api_base_url or DEFAULT_POWERBI_API_BASE_URL).rstrip("/"),
        "impersonated_user_name": impersonated_user_name,
        "connection_timeout_seconds": connection_timeout_seconds,
        "command_timeout_seconds": command_timeout_seconds,
        "max_rows": max_rows,
    }


def scaffold_json_literal(value: Any) -> str:
    """Return a safe Python string literal containing JSON."""
    return repr(json.dumps(value, indent=4))


def _safe_project_name(project_name: str) -> str:
    """Return a simple PEP 508-compatible project name for generated pyprojects."""
    return (project_name or "dax-streamlit-app").replace(" ", "-").lower()


def quote_shell_arg(value: str) -> str:
    if any(char.isspace() for char in value):
        return f'"{value}"'
    return value


def render_streamlit_pyproject(*, project_name: str) -> str:
    """Render a uv-compatible pyproject for a generated Streamlit app."""
    return _STREAMLIT_PYPROJECT_TEMPLATE.format(project_name=_safe_project_name(project_name))


def streamlit_uv_run_command(script_name: str = "app.py", *, include_dependencies: bool = False) -> str:
    """Return a uv command that launches a generated Streamlit app."""
    dependency_args = f"{_STREAMLIT_UV_DEPENDENCY_ARGS} " if include_dependencies else ""
    return f"uv run {dependency_args}streamlit run {quote_shell_arg(script_name)}"


def render_run_query_script(*, connection_config: dict[str, Any], query_filename: str) -> str:
    """Render the standalone single-query Python script."""
    script = (
        _RUN_QUERY_TEMPLATE
        .replace("__CONNECTION_CONFIG_JSON__", scaffold_json_literal(connection_config))
        .replace("__QUERY_FILENAME__", query_filename)
        .replace("__EXECUTOR_HELPERS__", _SCAFFOLD_EXECUTOR_HELPERS.rstrip())
    )
    compile(script, "run_query.py", "exec")
    return script


def render_run_queries_script(
    *,
    connections_config: dict[str, dict[str, Any]],
    queries: list[dict[str, Any]],
) -> str:
    """Render the standalone multi-query workstation Python script."""
    script = (
        _RUN_QUERIES_TEMPLATE
        .replace("__CONNECTIONS_CONFIG_JSON__", scaffold_json_literal(connections_config))
        .replace("__QUERIES_JSON__", scaffold_json_literal(queries))
        .replace("__EXECUTOR_HELPERS__", _SCAFFOLD_EXECUTOR_HELPERS.rstrip())
    )
    compile(script, "run_queries.py", "exec")
    return script


def render_streamlit_query_pack_app(
    *,
    connections_config: dict[str, dict[str, Any]],
    queries: list[dict[str, Any]],
    title: str = "DAX Query Pack Explorer",
) -> str:
    """Render the standalone Streamlit app for a query pack."""
    script = (
        _STREAMLIT_QUERY_PACK_TEMPLATE
        .replace("__APP_TITLE__", repr(title))
        .replace("__CONNECTIONS_CONFIG_JSON__", scaffold_json_literal(connections_config))
        .replace("__QUERIES_JSON__", scaffold_json_literal(queries))
        .replace("__EXECUTOR_HELPERS__", _SCAFFOLD_EXECUTOR_HELPERS.rstrip())
    )
    compile(script, "streamlit_app.py", "exec")
    return script


def render_streamlit_single_query_app(
    *,
    connection_name: str,
    connection_config: dict[str, Any],
    query: str,
    title: str = "DAX Query Results",
) -> str:
    """Render the full Streamlit explorer for one embedded DAX query."""
    return render_streamlit_query_pack_app(
        connections_config={connection_name: connection_config},
        queries=[
            {
                "id": "query",
                "name": "query",
                "display_name": title,
                "description": "One-off DAX query scaffolded by dax-query-mcp.",
                "connection_name": connection_name,
                "connection": connection_name,
                "dax_query": query,
                "tags": ["one-off"],
                "parameters": {},
                "outputs": {"default_format": "csv", "table_name": "QueryResult"},
            }
        ],
        title=title,
    )


def _build_notebook(connection_config: dict[str, Any], query_filename: str, query_text: str) -> dict[str, Any]:
    """Build a Jupyter .ipynb dict with the executor function and query pre-loaded."""

    def _code_cell(source: str) -> dict[str, Any]:
        return {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [line + "\n" for line in source.splitlines()],
        }

    def _md_cell(source: str) -> dict[str, Any]:
        return {
            "cell_type": "markdown",
            "metadata": {},
            "source": [line + "\n" for line in source.splitlines()],
        }

    cells = [
        _md_cell("# DAX Query Workspace\n\nAuto-generated by **dax-query-mcp**. "
                 "Edit the connection config below and run all cells."),
        _md_cell("## Setup"),
        _code_cell(
            "from __future__ import annotations\n"
            "import json\n"
            "import os\n"
            "import random\n"
            "import re\n"
            "import shutil\n"
            "import subprocess\n"
            "import sys\n"
            "import urllib.error\n"
            "import urllib.request\n"
            "from contextlib import suppress\n"
            "from datetime import date, datetime\n"
            "from pathlib import Path\n"
            "from typing import Any, Mapping\n"
            "import pandas as pd"
        ),
        _md_cell("## Connection Config\n\nEdit this dict for MSOLAP, Power BI REST, or MOCK://."),
        _code_cell(
            f"CONNECTION = json.loads({scaffold_json_literal(connection_config)})\n"
            "CONNECTION_STRING = str(CONNECTION.get(\"connection_string\") or \"\")"
        ),
        _md_cell("## DAX Executor\n\nSelf-contained transport-aware executor."),
        _code_cell(_SCAFFOLD_EXECUTOR_HELPERS.rstrip()),
        _md_cell(f"## Run Query\n\nLoaded from `queries/{query_filename}`"),
        _code_cell(
            f'DAX_FILE = Path("queries/{query_filename}")\n'
            "DAX_QUERY = DAX_FILE.read_text(encoding=\"utf-8\").strip()\n"
            "print(f\"Loaded {len(DAX_QUERY)} chars from {DAX_FILE}\")"
        ),
        _code_cell(
            "df = execute_dax(DAX_QUERY, CONNECTION)\n"
            "print(f\"{len(df)} rows x {len(df.columns)} cols\")\n"
            "df.head(20)"
        ),
    ]

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.12.0"},
        },
        "cells": cells,
    }


def scaffold_workspace(
    output_dir: str | Path,
    *,
    query_file: str | Path | None = None,
    query_text: str | None = None,
    query_name: str = "query",
    connection_string: str = "",
    transport: str = TRANSPORT_MSOLAP,
    dataset_id: str | None = None,
    auth_mode: str = AUTH_AZURE_CLI,
    access_token_env: str | None = None,
    api_base_url: str = DEFAULT_POWERBI_API_BASE_URL,
    impersonated_user_name: str | None = None,
    connection_timeout_seconds: int = 300,
    command_timeout_seconds: int = 1800,
    max_rows: int | None = None,
    project_name: str | None = None,
    overwrite: bool = False,
) -> dict[str, str]:
    """Create a portable DAX workspace folder.

    Provide either *query_file* (path to an existing ``.dax`` file) or
    *query_text* (raw DAX string).  Returns a dict summarising what was
    created.
    """
    if query_file is None and query_text is None:
        raise ValueError("Provide either query_file or query_text")

    output = Path(output_dir)
    if output.exists() and not overwrite:
        raise FileExistsError(
            f"Output directory already exists: {output}. Pass overwrite=True to replace it."
        )

    # Resolve query content & filename
    if query_file is not None:
        src = Path(query_file)
        if not src.exists():
            raise FileNotFoundError(f"Query file not found: {src}")
        query_text = src.read_text(encoding="utf-8")
        query_filename = src.name
    else:
        query_filename = f"{query_name}.dax" if not query_name.endswith(".dax") else query_name

    if project_name is None:
        project_name = output.name

    # Sanitize project name for pyproject
    safe_project = _safe_project_name(project_name)
    connection_config = build_scaffold_connection_config(
        connection_string=connection_string,
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

    # Create folder structure
    output.mkdir(parents=True, exist_ok=True)
    queries_dir = output / "queries"
    queries_dir.mkdir(exist_ok=True)

    # Write query
    query_path = queries_dir / query_filename
    query_path.write_text(query_text or "", encoding="utf-8")

    # Copy .queryBuilder sidecar if it exists alongside the source file
    if query_file is not None:
        sidecar = Path(query_file).with_suffix(".dax.queryBuilder")
        if sidecar.exists():
            shutil.copy2(sidecar, queries_dir / sidecar.name)

    # Write run_query.py
    run_script = output / "run_query.py"
    run_script.write_text(
        render_run_query_script(
            connection_config=connection_config,
            query_filename=query_filename,
        ),
        encoding="utf-8",
    )

    # Write pyproject.toml
    pyproject = output / "pyproject.toml"
    pyproject.write_text(
        _PYPROJECT_TEMPLATE.format(project_name=safe_project),
        encoding="utf-8",
    )

    # Write Jupyter notebook
    notebook_path = output / "notebook.ipynb"
    nb = _build_notebook(connection_config, query_filename, query_text or "")
    notebook_path.write_text(json.dumps(nb, indent=1), encoding="utf-8")

    # Write README
    readme = output / "README.md"
    readme.write_text(
        _README_TEMPLATE.format(
            project_name=safe_project,
            query_filename=query_filename,
        ),
        encoding="utf-8",
    )

    created_files = [
        str(run_script),
        str(notebook_path),
        str(pyproject),
        str(readme),
        str(query_path),
    ]

    return {
        "output_dir": str(output),
        "project_name": safe_project,
        "query_filename": query_filename,
        "files_created": created_files,
        "next_steps": f"cd {output} && uv run run_query.py  # or open notebook.ipynb in VS Code / Jupyter",
    }
