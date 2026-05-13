"""Tests for the scaffold module."""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from dax_query_mcp.scaffold import render_streamlit_query_pack_app, scaffold_workspace


def test_scaffold_creates_all_files(tmp_path: Path) -> None:
    output = tmp_path / "my-project"
    result = scaffold_workspace(
        output,
        query_text="EVALUATE ROW('x', 1)",
        query_name="test-query",
    )
    assert output.exists()
    assert (output / "run_query.py").exists()
    assert (output / "notebook.ipynb").exists()
    assert (output / "pyproject.toml").exists()
    assert not (output / "requirements.txt").exists()
    assert (output / "README.md").exists()
    assert (output / "queries" / "test-query.dax").exists()
    assert result["project_name"] == "my-project"
    assert result["query_filename"] == "test-query.dax"
    assert len(result["files_created"]) == 5


def test_scaffold_from_file(tmp_path: Path) -> None:
    # Create a source .dax file
    src = tmp_path / "source.dax"
    src.write_text("EVALUATE ROW('hello', 42)", encoding="utf-8")

    output = tmp_path / "exported"
    result = scaffold_workspace(output, query_file=str(src))

    query_path = output / "queries" / "source.dax"
    assert query_path.exists()
    assert query_path.read_text(encoding="utf-8") == "EVALUATE ROW('hello', 42)"
    assert result["query_filename"] == "source.dax"


def test_scaffold_copies_sidecar(tmp_path: Path) -> None:
    src = tmp_path / "my.dax"
    src.write_text("EVALUATE ROW('x', 1)", encoding="utf-8")
    sidecar = tmp_path / "my.dax.queryBuilder"
    sidecar.write_text('{"test": true}', encoding="utf-8")

    output = tmp_path / "with-sidecar"
    scaffold_workspace(output, query_file=str(src))

    assert (output / "queries" / "my.dax.queryBuilder").exists()


def test_scaffold_run_query_has_placeholder(tmp_path: Path) -> None:
    output = tmp_path / "proj"
    scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)")

    script = (output / "run_query.py").read_text(encoding="utf-8")
    assert "YOUR_CONNECTION_STRING_HERE" in script
    assert "dax_to_pandas" in script
    assert "_clean_column_name" in script


def test_scaffold_embeds_connection_string(tmp_path: Path) -> None:
    output = tmp_path / "proj"
    scaffold_workspace(
        output,
        query_text="EVALUATE ROW('x', 1)",
        connection_string="Provider=MSOLAP;Data Source=localhost",
    )
    script = (output / "run_query.py").read_text(encoding="utf-8")
    assert "Provider=MSOLAP;Data Source=localhost" in script


def test_scaffold_pyproject_has_project_name(tmp_path: Path) -> None:
    output = tmp_path / "Cool Project"
    scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)")

    toml = (output / "pyproject.toml").read_text(encoding="utf-8")
    assert 'name = "cool-project"' in toml


def test_scaffold_rejects_existing_dir_without_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "exists"
    output.mkdir()
    with pytest.raises(FileExistsError):
        scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)")


def test_scaffold_overwrites_with_flag(tmp_path: Path) -> None:
    output = tmp_path / "exists"
    output.mkdir()
    result = scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)", overwrite=True)
    assert (output / "run_query.py").exists()


def test_scaffold_raises_without_query(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="query_file or query_text"):
        scaffold_workspace(tmp_path / "empty")


def test_scaffold_appends_dax_extension(tmp_path: Path) -> None:
    output = tmp_path / "proj"
    result = scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)", query_name="my-query")
    assert result["query_filename"] == "my-query.dax"


def test_scaffold_notebook_is_valid_ipynb(tmp_path: Path) -> None:
    """Generated notebook must be valid JSON with expected ipynb structure."""
    output = tmp_path / "nb-test"
    scaffold_workspace(
        output,
        query_text="EVALUATE ROW('hello', 42)",
        query_name="test",
        connection_string="Provider=MSOLAP;Data Source=localhost",
    )
    nb_path = output / "notebook.ipynb"
    assert nb_path.exists()
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    assert nb["nbformat"] == 4
    assert len(nb["cells"]) >= 5
    # Check the query text is in a code cell
    all_source = " ".join(
        "".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"
    )
    assert "dax_to_pandas" in all_source
    assert 'queries/test.dax' in all_source
    assert "read_text" in all_source
    assert "Provider=MSOLAP" in all_source


def test_scaffold_pyproject_has_ipykernel(tmp_path: Path) -> None:
    output = tmp_path / "kern"
    scaffold_workspace(output, query_text="EVALUATE ROW('x', 1)")
    toml = (output / "pyproject.toml").read_text(encoding="utf-8")
    assert "ipykernel" in toml


def test_scaffold_multiline_connection_string_produces_valid_python(tmp_path: Path) -> None:
    """Connection strings with newlines must not break the generated script."""
    output = tmp_path / "multiline"
    scaffold_workspace(
        output,
        query_text="EVALUATE ROW('x', 1)",
        connection_string=(
            "Provider=MSOLAP.8;\n"
            "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;\n"
            'Initial Catalog="My Model"'
        ),
    )
    script = (output / "run_query.py").read_text(encoding="utf-8")
    # Must compile without SyntaxError
    compile(script, "run_query.py", "exec")
    # Connection string should be on one line
    assert "\\n" not in script.split("CONNECTION_STRING")[1].split("\n")[0]


def test_scaffold_connection_string_with_quotes(tmp_path: Path) -> None:
    """Double quotes in connection strings must be escaped."""
    output = tmp_path / "quotes"
    scaffold_workspace(
        output,
        query_text="EVALUATE ROW('x', 1)",
        connection_string='Provider=MSOLAP;Initial Catalog="My Model"',
    )
    script = (output / "run_query.py").read_text(encoding="utf-8")
    compile(script, "run_query.py", "exec")


def test_scaffold_powerbi_rest_connection_config(tmp_path: Path) -> None:
    """Generated single-query scaffold preserves REST transport metadata."""
    output = tmp_path / "rest"
    scaffold_workspace(
        output,
        query_text='EVALUATE ROW("Ping", 1)',
        transport="powerbi_rest",
        dataset_id="00000000-0000-0000-0000-000000000000",
        auth_mode="env",
        access_token_env="TEST_POWERBI_TOKEN",
    )

    script = (output / "run_query.py").read_text(encoding="utf-8")
    compile(script, "run_query.py", "exec")
    assert '"transport": "powerbi_rest"' in script
    assert '"dataset_id": "00000000-0000-0000-0000-000000000000"' in script
    assert '"auth_mode": "env"' in script
    assert '"access_token_env": "TEST_POWERBI_TOKEN"' in script
    assert "powerbi_rest_to_pandas" in script
    assert "az.cmd" in script

    nb = json.loads((output / "notebook.ipynb").read_text(encoding="utf-8"))
    all_source = " ".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
    assert "powerbi_rest_to_pandas" in all_source
    assert "TEST_POWERBI_TOKEN" in all_source


def test_scaffold_powerbi_rest_rejects_workspace_scoped_base_url(tmp_path: Path) -> None:
    """Generated scaffold refuses /groups/{workspace} REST base URLs."""
    output = tmp_path / "rest_groups"
    scaffold_workspace(
        output,
        query_text='EVALUATE ROW("Ping", 1)',
        transport="powerbi_rest",
        dataset_id="00000000-0000-0000-0000-000000000000",
        auth_mode="env",
        access_token_env="TEST_POWERBI_TOKEN",
        api_base_url="https://api.powerbi.com/v1.0/myorg/groups/00000000-0000-0000-0000-000000000000",
    )

    namespace = {"__name__": "generated_run_query", "__file__": str(output / "run_query.py")}
    script = (output / "run_query.py").read_text(encoding="utf-8")
    exec(script, namespace)

    with pytest.raises(RuntimeError, match="dataset-only executeQueries endpoint"):
        namespace["powerbi_rest_to_pandas"]('EVALUATE ROW("Ping", 1)', namespace["CONNECTION"])


def test_scaffold_mock_connection_executes_without_com(tmp_path: Path) -> None:
    """MOCK:// scaffolds can run the demo query without pywin32/ADODB."""
    output = tmp_path / "mock"
    scaffold_workspace(
        output,
        query_text=(
            "EVALUATE\n"
            "SUMMARIZE(\n"
            "    Sales,\n"
            '    "Total Sales", [Total Sales],\n'
            '    "Total Quantity", [Total Quantity]\n'
            ")"
        ),
        connection_string="MOCK://contoso",
    )

    namespace = {"__name__": "generated_run_query", "__file__": str(output / "run_query.py")}
    script = (output / "run_query.py").read_text(encoding="utf-8")
    exec(script, namespace)

    dataframe = namespace["execute_dax"](
        (output / "queries" / "query.dax").read_text(encoding="utf-8"),
        namespace["CONNECTION"],
    )
    assert dataframe.iloc[0]["Total_Sales"] == 178390.0
    assert dataframe.iloc[0]["Total_Quantity"] == 290


def test_streamlit_query_pack_app_contains_full_explorer_surfaces() -> None:
    """Generated query-pack Streamlit app includes the richer explorer workflow."""
    script = render_streamlit_query_pack_app(
        connections_config={"mock": {"transport": "msolap", "connection_string": "MOCK://contoso"}},
        queries=[
            {
                "id": "sales",
                "name": "sales",
                "display_name": "Sales",
                "file": "queries/sales.dax",
                "connection_name": "mock",
                "connection": "mock",
                "description": "Sales query",
                "tags": ["mock"],
                "parameters": {
                    "category": {
                        "type": "list[text]",
                        "default": ["Bikes"],
                        "allowed_values": ["Bikes", "Accessories"],
                    }
                },
                "outputs": {"default_format": "csv", "table_name": "Sales"},
            }
        ],
    )

    compile(script, "streamlit_app.py", "exec")
    assert '["Explore", "Profile", "Downloads", "History", "Catalog", "Upload"]' in script
    assert "render_chart_builder" in script
    assert "render_pivot_builder" in script
    assert "render_filtered_dataframe" in script
    assert "Run the query to see results, charts, and pivots here." in script
    assert "Download filtered CSV" in script
    assert "Run history" in script
    assert "Drag-and-drop data explorer" in script
    assert "file_uploader" in script
    assert "DAX editor mode bypasses parameter rendering" in script
    assert "key=widget_key(entry_id, \"chart:type\")" in script
    assert "key=widget_key(entry_id, \"pivot:rows\")" in script
    assert "pd.to_numeric(non_null, errors=\"coerce\")" in script
    assert "sort=False" in script
    assert "default_series_index = 1 if len(possible_series) > 1 else 0" in script


def test_streamlit_query_pack_app_smoke_runs_with_fake_streamlit(tmp_path: Path, monkeypatch) -> None:
    """Generated app top-level code should execute without duplicate or missing widget basics."""
    queries_dir = tmp_path / "queries"
    queries_dir.mkdir()
    (queries_dir / "sales.dax").write_text('EVALUATE ROW("Sales", 1)', encoding="utf-8")
    script_path = tmp_path / "streamlit_app.py"
    script = render_streamlit_query_pack_app(
        connections_config={"mock": {"transport": "msolap", "connection_string": "MOCK://contoso"}},
        queries=[
            {
                "id": "sales",
                "name": "sales",
                "display_name": "Sales",
                "file": "queries/sales.dax",
                "connection_name": "mock",
                "connection": "mock",
                "description": "Sales query",
                "tags": ["mock"],
                "parameters": {"amount": {"type": "number", "default": 1}},
                "outputs": {"default_format": "csv", "table_name": "Sales"},
            }
        ],
    )
    script_path.write_text(script, encoding="utf-8")
    fake_streamlit = _FakeStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)

    namespace = {"__name__": "generated_streamlit_app", "__file__": str(script_path)}
    exec(script, namespace)

    assert ("title", "DAX Query Pack Explorer") in fake_streamlit.calls
    assert fake_streamlit.widget_keys
    assert len(fake_streamlit.widget_keys) == len(set(fake_streamlit.widget_keys))


def test_streamlit_query_pack_app_run_button_renders_chart(monkeypatch) -> None:
    """Clicking Run query should render results and a chart in the Explore tab."""
    script = render_streamlit_query_pack_app(
        connections_config={"mock": {"transport": "msolap", "connection_string": "MOCK://contoso"}},
        queries=[
            {
                "id": "sales",
                "name": "sales",
                "display_name": "Sales",
                "dax_query": "EVALUATE SUMMARIZECOLUMNS('Calendar'[Month], \"Total\", SUM(Sales[Amount]))",
                "connection_name": "mock",
                "connection": "mock",
                "description": "Sales query",
                "tags": ["mock"],
                "parameters": {},
                "outputs": {"default_format": "csv", "table_name": "Sales"},
            }
        ],
    )
    fake_streamlit = _FakeStreamlit(clicked_buttons={"Run query"})
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)

    namespace = {"__name__": "generated_streamlit_app", "__file__": "streamlit_app.py"}
    exec(script, namespace)

    assert any(call == ("subheader", "Results") for call in fake_streamlit.calls)
    assert any(call == ("subheader", "Charts") for call in fake_streamlit.calls)
    assert any(call[0] == "bar_chart" for call in fake_streamlit.calls)


class _FakeBlock:
    def __init__(self, root: "_FakeStreamlit") -> None:
        self._root = root

    def __enter__(self) -> "_FakeBlock":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def __getattr__(self, name: str):
        return getattr(self._root, name)


class _FakeStreamlit(types.ModuleType):
    def __init__(self, *, clicked_buttons: set[str] | None = None) -> None:
        super().__init__("streamlit")
        self.calls: list[tuple[str, object]] = []
        self.widget_keys: list[str] = []
        self.session_state: dict[str, object] = {}
        self.sidebar = _FakeBlock(self)
        self.clicked_buttons = clicked_buttons or set()

    def _record_key(self, key: str | None) -> None:
        if key:
            self.widget_keys.append(key)

    def cache_data(self, *args, **kwargs):
        def decorator(func):
            func.clear = lambda: None
            return func

        if args and callable(args[0]):
            return decorator(args[0])
        return decorator

    def set_page_config(self, **kwargs) -> None:
        self.calls.append(("set_page_config", kwargs))

    def title(self, value: str) -> None:
        self.calls.append(("title", value))

    def header(self, value: str) -> None:
        self.calls.append(("header", value))

    def subheader(self, value: str) -> None:
        self.calls.append(("subheader", value))

    def caption(self, value: str) -> None:
        self.calls.append(("caption", value))

    def write(self, value: object) -> None:
        self.calls.append(("write", value))

    def markdown(self, value: str) -> None:
        self.calls.append(("markdown", value))

    def info(self, value: str) -> None:
        self.calls.append(("info", value))

    def warning(self, value: str) -> None:
        self.calls.append(("warning", value))

    def error(self, value: str) -> None:
        self.calls.append(("error", value))

    def success(self, value: str) -> None:
        self.calls.append(("success", value))

    def metric(self, label: str, value: object, *args, **kwargs) -> None:
        self.calls.append(("metric", (label, value)))

    def json(self, value: object) -> None:
        self.calls.append(("json", value))

    def code(self, value: str, *args, **kwargs) -> None:
        self.calls.append(("code", value))

    def dataframe(self, value: object, *args, **kwargs) -> None:
        self.calls.append(("dataframe", value))

    def bar_chart(self, *args, **kwargs) -> None:
        self.calls.append(("bar_chart", args))

    def line_chart(self, *args, **kwargs) -> None:
        self.calls.append(("line_chart", args))

    def area_chart(self, *args, **kwargs) -> None:
        self.calls.append(("area_chart", args))

    def scatter_chart(self, *args, **kwargs) -> None:
        self.calls.append(("scatter_chart", args))

    def tabs(self, labels: list[str]) -> list[_FakeBlock]:
        self.calls.append(("tabs", labels))
        return [_FakeBlock(self) for _ in labels]

    def columns(self, spec) -> list[_FakeBlock]:
        count = spec if isinstance(spec, int) else len(spec)
        return [_FakeBlock(self) for _ in range(count)]

    def expander(self, *args, **kwargs) -> _FakeBlock:
        return _FakeBlock(self)

    def spinner(self, *args, **kwargs) -> _FakeBlock:
        return _FakeBlock(self)

    def text_input(self, label: str, value: str = "", key: str | None = None, **kwargs) -> str:
        self._record_key(key)
        return value

    def text_area(self, label: str, value: str = "", key: str | None = None, **kwargs) -> str:
        self._record_key(key)
        return value

    def multiselect(
        self,
        label: str,
        options,
        default=None,
        key: str | None = None,
        **kwargs,
    ):
        self._record_key(key)
        return [] if default is None else default

    def selectbox(self, label: str, options, index: int = 0, key: str | None = None, **kwargs):
        self._record_key(key)
        options_list = list(options)
        return options_list[index] if options_list else None

    def number_input(self, label: str, value=0, key: str | None = None, **kwargs):
        self._record_key(key)
        return value

    def checkbox(self, label: str, value: bool = False, key: str | None = None, **kwargs) -> bool:
        self._record_key(key)
        return value

    def button(self, label: str, key: str | None = None, **kwargs) -> bool:
        self._record_key(key)
        return label in self.clicked_buttons or (key is not None and key in self.clicked_buttons)

    def date_input(self, label: str, value=None, key: str | None = None, **kwargs):
        self._record_key(key)
        return value

    def file_uploader(self, label: str, key: str | None = None, **kwargs):
        self._record_key(key)
        return None

    def slider(self, label: str, value=None, key: str | None = None, **kwargs):
        self._record_key(key)
        return value

    def download_button(self, label: str, data, key: str | None = None, **kwargs) -> bool:
        self._record_key(key)
        return False

    def stop(self) -> None:
        raise RuntimeError("st.stop called in fake Streamlit test")
