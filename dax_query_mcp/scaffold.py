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


_RUN_QUERY_TEMPLATE = textwrap.dedent("""\
    \"\"\"Bare-bones DAX executor — paste into a notebook or run with `uv run run_query.py`.

    Requirements (Windows only):
        pip install pywin32 pandas   # or use the generated pyproject.toml with uv
    \"\"\"

    from __future__ import annotations

    import sys
    from contextlib import suppress
    from datetime import datetime
    from pathlib import Path

    import pandas as pd

    # ── Connection ────────────────────────────────────────────────────────
    # Paste your Power BI / SSAS connection string here.
    CONNECTION_STRING = "{connection_string}"

    # ── Query ─────────────────────────────────────────────────────────────
    QUERY_FILE = Path(__file__).parent / "queries" / "{query_filename}"


    def dax_to_pandas(
        dax_query: str,
        conn_str: str,
        *,
        timeout: int = 1800,
        max_rows: int | None = None,
    ) -> pd.DataFrame:
        \"\"\"Execute a DAX query via COM/ADODB and return a pandas DataFrame.\"\"\"
        import win32com.client  # Windows-only

        conn = win32com.client.Dispatch("ADODB.Connection")
        conn.ConnectionTimeout = 300
        conn.CommandTimeout = timeout
        conn.Open(conn_str)

        cmd = win32com.client.Dispatch("ADODB.Command")
        cmd.ActiveConnection = conn
        cmd.CommandText = dax_query
        cmd.CommandTimeout = timeout

        try:
            recordset = cmd.Execute()[0]
            fields = [recordset.Fields(i).Name for i in range(recordset.Fields.Count)]
            rows = recordset.GetRows(max_rows) if max_rows else recordset.GetRows()
        finally:
            with suppress(Exception):
                cmd.ActiveConnection = None
            for obj in (recordset, conn):
                close = getattr(obj, "Close", None)
                if callable(close):
                    with suppress(Exception):
                        close()

        data = {{}}
        for i, name in enumerate(fields):
            vals = [_strip_tz(v) for v in rows[i]] if rows and i < len(rows) else []
            data[name] = list(vals)

        return pd.DataFrame(data)


    def _strip_tz(value: object) -> object:
        if isinstance(value, datetime) and getattr(value, "tzinfo", None) is not None:
            return value.replace(tzinfo=None)
        return value


    def _clean_column_name(name: str) -> str:
        \"\"\"Strip table prefixes like 'Calendar[Fiscal Month]' → 'Fiscal_Month'.\"\"\"
        if "[" in name and "]" in name:
            name = name[name.find("[") + 1 : name.find("]")]
        return name.replace(" ", "_")


    if __name__ == "__main__":
        from rich.console import Console
        from rich.table import Table

        if not QUERY_FILE.exists():
            print(f"Query file not found: {{QUERY_FILE}}")
            sys.exit(1)

        dax = QUERY_FILE.read_text(encoding="utf-8")
        console = Console()
        console.print(f"[bold]Running {{QUERY_FILE.name}} ...[/bold]")
        df = dax_to_pandas(dax, CONNECTION_STRING)
        df.columns = [_clean_column_name(c) for c in df.columns]
        console.print(f"[green]{{len(df)}} rows x {{len(df.columns)}} cols[/green]\\n")

        table = Table(show_lines=True, title=QUERY_FILE.stem)
        for col in df.columns:
            table.add_column(col, header_style="bold cyan", style="white")
        for _, row in df.head(50).iterrows():
            table.add_row(*[str(v) for v in row])
        if len(df) > 50:
            table.caption = f"Showing 50 of {{len(df)}} rows"
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
        "pywin32>=310",
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
    pip install pywin32 pandas ipykernel rich
    python run_query.py
    ```

    ## Files

    | File | Purpose |
    |------|---------|
    | `run_query.py` | Bare-bones DAX executor (terminal, rich tables) |
    | `notebook.ipynb` | Jupyter notebook with executor + query pre-loaded |
    | `queries/{query_filename}` | Saved DAX query |
    | `pyproject.toml` | Dependency manifest for `uv run` |

    ## Usage in a notebook

    Open `notebook.ipynb` directly — it has the `dax_to_pandas()` function,
    connection string, and query ready to run. Or copy the function into any
    other Jupyter / Fabric notebook cell.

    ## Connection string

    Edit `CONNECTION_STRING` in `run_query.py` with your Power BI / SSAS
    connection string. The placeholder is left blank for safety — never
    commit real credentials.
""")


def _build_notebook(connection_string: str, query_filename: str, query_text: str) -> dict:
    """Build a Jupyter .ipynb dict with the executor function and query pre-loaded."""

    def _code_cell(source: str) -> dict:
        return {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [line + "\n" for line in source.splitlines()],
        }

    def _md_cell(source: str) -> dict:
        return {
            "cell_type": "markdown",
            "metadata": {},
            "source": [line + "\n" for line in source.splitlines()],
        }

    cells = [
        _md_cell("# DAX Query Workspace\n\nAuto-generated by **dax-query-mcp**. "
                 "Edit the connection string below and run all cells."),
        _md_cell("## Setup"),
        _code_cell(
            "from __future__ import annotations\n"
            "from contextlib import suppress\n"
            "from datetime import datetime\n"
            "from pathlib import Path\n"
            "import pandas as pd"
        ),
        _md_cell("## Connection String\n\nPaste your Power BI / SSAS connection string here."),
        _code_cell(f'CONNECTION_STRING = "{connection_string}"'),
        _md_cell("## DAX Executor\n\nSelf-contained function — no external packages beyond `pywin32` + `pandas`."),
        _code_cell(
            "def dax_to_pandas(\n"
            "    dax_query: str,\n"
            "    conn_str: str,\n"
            "    *,\n"
            "    timeout: int = 1800,\n"
            "    max_rows: int | None = None,\n"
            ") -> pd.DataFrame:\n"
            '    """Execute a DAX query via COM/ADODB and return a pandas DataFrame."""\n'
            "    import win32com.client\n"
            "\n"
            '    conn = win32com.client.Dispatch("ADODB.Connection")\n'
            "    conn.ConnectionTimeout = 300\n"
            "    conn.CommandTimeout = timeout\n"
            "    conn.Open(conn_str)\n"
            "\n"
            '    cmd = win32com.client.Dispatch("ADODB.Command")\n'
            "    cmd.ActiveConnection = conn\n"
            "    cmd.CommandText = dax_query\n"
            "    cmd.CommandTimeout = timeout\n"
            "\n"
            "    try:\n"
            "        recordset = cmd.Execute()[0]\n"
            "        fields = [recordset.Fields(i).Name for i in range(recordset.Fields.Count)]\n"
            "        rows = recordset.GetRows(max_rows) if max_rows else recordset.GetRows()\n"
            "    finally:\n"
            "        with suppress(Exception):\n"
            "            cmd.ActiveConnection = None\n"
            "        for obj in (recordset, conn):\n"
            "            close = getattr(obj, 'Close', None)\n"
            "            if callable(close):\n"
            "                with suppress(Exception):\n"
            "                    close()\n"
            "\n"
            "    data = {}\n"
            "    for i, name in enumerate(fields):\n"
            "        col = name\n"
            "        if '[' in col and ']' in col:\n"
            "            col = col[col.find('[') + 1 : col.find(']')]\n"
            "        col = col.replace(' ', '_')\n"
            "        vals = list(rows[i]) if rows and i < len(rows) else []\n"
            "        data[col] = vals\n"
            "\n"
            "    return pd.DataFrame(data)"
        ),
        _md_cell(f"## Run Query\n\nLoaded from `queries/{query_filename}`"),
        _code_cell(
            f'DAX_FILE = Path("queries/{query_filename}")\n'
            "DAX_QUERY = DAX_FILE.read_text(encoding=\"utf-8\").strip()\n"
            "print(f\"Loaded {len(DAX_QUERY)} chars from {DAX_FILE}\")"
        ),
        _code_cell(
            "df = dax_to_pandas(DAX_QUERY, CONNECTION_STRING)\n"
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

    # Create folder structure
    output.mkdir(parents=True, exist_ok=True)
    queries_dir = output / "queries"
    queries_dir.mkdir(exist_ok=True)

    # Write query
    query_path = queries_dir / query_filename
    query_path.write_text(query_text, encoding="utf-8")

    # Copy .queryBuilder sidecar if it exists alongside the source file
    if query_file is not None:
        sidecar = Path(query_file).with_suffix(".dax.queryBuilder")
        if sidecar.exists():
            shutil.copy2(sidecar, queries_dir / sidecar.name)

    # Write run_query.py
    run_script = output / "run_query.py"
    conn_placeholder = connection_string or "YOUR_CONNECTION_STRING_HERE"
    # Collapse newlines/whitespace in connection strings to prevent broken string literals
    conn_placeholder = " ".join(conn_placeholder.split())
    run_script.write_text(
        _RUN_QUERY_TEMPLATE.format(
            connection_string=conn_placeholder.replace("\\", "\\\\").replace('"', '\\"'),
            query_filename=query_filename,
        ),
        encoding="utf-8",
    )

    # Validate generated script is valid Python
    script_source = run_script.read_text(encoding="utf-8")
    try:
        compile(script_source, str(run_script), "exec")
    except SyntaxError as exc:
        raise RuntimeError(
            f"Generated run_query.py has a syntax error (line {exc.lineno}): {exc.msg}. "
            f"This is a bug in scaffold — please report it."
        ) from exc

    # Write pyproject.toml
    pyproject = output / "pyproject.toml"
    pyproject.write_text(
        _PYPROJECT_TEMPLATE.format(project_name=safe_project),
        encoding="utf-8",
    )

    # Write Jupyter notebook
    notebook_path = output / "notebook.ipynb"
    nb = _build_notebook(conn_placeholder, query_filename, query_text)
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
