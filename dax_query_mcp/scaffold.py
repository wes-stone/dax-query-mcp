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

        import win32com.client  # Windows-only

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
    \"\"\"Run all workstation queries with transport-aware DAX execution.

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

    # -- Connections --------------------------------------------------------
    # Edit these dicts with your Power BI / SSAS connection details.
    CONNECTIONS = json.loads(__CONNECTIONS_CONFIG_JSON__)


    __EXECUTOR_HELPERS__


    QUERIES = json.loads(__QUERIES_JSON__)


    if __name__ == "__main__":
        from rich.console import Console
        from rich.table import Table

        console = Console()

        for entry in QUERIES:
            qfile = Path(entry["file"])
            if not qfile.is_absolute():
                qfile = Path(__file__).parent / qfile
            connection = CONNECTIONS.get(entry["connection"])
            if connection is None:
                console.print(f"[red]Missing connection config: {entry['connection']}[/red]")
                continue
            if not qfile.exists():
                console.print(f"[red]Missing query file: {qfile}[/red]")
                continue

            dax = qfile.read_text(encoding="utf-8")
            console.print(f"\\n[bold]Running {entry['name']} via {connection.get('transport', 'msolap')} ...[/bold]")
            console.print(f"  [dim]{entry['description']}[/dim]")
            df = execute_dax(dax, connection)
            console.print(f"  [green]{len(df)} rows x {len(df.columns)} cols[/green]")

            table = Table(show_lines=True, title=entry["name"])
            for col in df.columns:
                table.add_column(str(col), header_style="bold cyan")
            for _, row in df.head(20).iterrows():
                table.add_row(*[str(v) for v in row])
            if len(df) > 20:
                table.caption = f"Showing 20 of {len(df)} rows"
            console.print(table)
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
    ]
""")


_README_TEMPLATE = textwrap.dedent("""\
    # {project_name}

    Portable DAX query workspace scaffolded by **dax-query-mcp**.

    ## Quick start

    ```bash
    # run the script with uv (no install needed)
    uv run run_query.py

    # or open the notebook in VS Code / Jupyter
    jupyter notebook notebook.ipynb

    # or install deps and run directly
    pip install pandas ipykernel rich
    pip install pywin32  # only needed for MSOLAP/ADODB connections on Windows
    python run_query.py
    ```

    ## Files

    | File | Purpose |
    |------|---------|
    | `run_query.py` | Bare-bones DAX executor for MSOLAP, Power BI REST, and MOCK:// connections |
    | `notebook.ipynb` | Jupyter notebook with executor + query pre-loaded |
    | `queries/{query_filename}` | Saved DAX query |
    | `pyproject.toml` | Dependency manifest for `uv run` |

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
    queries: list[dict[str, str]],
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
    safe_project = project_name.replace(" ", "-").lower()
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
