from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from .config import load_queries
from .executor import DAXExecutor
from .formatting import dataframe_dtypes_to_markdown, dataframe_to_markdown
from .models import DAXQueryConfig


class DAXPipeline:
    def __init__(
        self,
        config_dir: str = "queries",
        export_to: str | None = None,
        *,
        executor: DAXExecutor | None = None,
    ):
        self.config_dir = Path(config_dir)
        self.export_dir: Path | None = None
        self.export_to_dir: Path | None = None
        self.custom_export_path = export_to
        self.executor = executor or DAXExecutor()

        logger.info(f"Initializing DAX Pipeline with config directory: {self.config_dir}")
        if export_to:
            logger.info(f"Additional export location: {export_to}")

        self.queries = load_queries(self.config_dir)

    def run_query(self, query_name: str, preview: bool = False, export: bool = False) -> pd.DataFrame | None:
        start_time = time.time()
        query_config = self.queries.get(query_name)

        if query_config is None:
            logger.error(f"Query '{query_name}' not found. Available queries: {list(self.queries.keys())}")
            return None

        logger.info(f"Running query: {query_name}")
        if query_config.description:
            logger.info(f"Description: {query_config.description}")

        try:
            dataframe = self.executor.execute(query_config)
            execution_time = time.time() - start_time
            logger.success(
                "Query '{}' completed successfully. Shape: {}, Time: {:.2f}s",
                query_name,
                dataframe.shape,
                execution_time,
            )

            if preview:
                self._preview(dataframe)

            if export:
                self.export_dataframe(dataframe, query_config)

            return dataframe
        except Exception as exc:
            execution_time = time.time() - start_time
            logger.error(f"Error executing query '{query_name}': {exc} (Time: {execution_time:.2f}s)")
            return None

    def run_all_queries(self, preview: bool = False, export: bool = False) -> dict[str, pd.DataFrame]:
        start_time = time.time()
        logger.info(f"Starting execution of all queries ({len(self.queries)} total)")
        results: dict[str, pd.DataFrame] = {}

        for query_name in self.queries:
            dataframe = self.run_query(query_name, preview=preview, export=export)
            if dataframe is not None:
                results[query_name] = dataframe

        total_time = time.time() - start_time
        logger.info(
            f"Completed execution of all queries. {len(results)} successful, "
            f"{len(self.queries) - len(results)} failed. Total time: {total_time:.2f}s"
        )
        return results

    def list_queries(self) -> None:
        print("\n--- Available Queries ---")
        for name, config in self.queries.items():
            description = config.description or "No description"
            print(f"  {name}: {description}")

    def export_dataframe(self, dataframe: pd.DataFrame, query_config: DAXQueryConfig) -> None:
        export_dir = self.get_export_dir()
        filename = query_config.export_name
        filepath = export_dir / f"{filename}.csv"
        dataframe.to_csv(filepath, index=False)
        logger.success(f"Exported '{query_config.name}' to: {filepath}")

        if self.custom_export_path:
            custom_export_dir = self.get_custom_export_dir()
            custom_filepath = custom_export_dir / f"{filename}.csv"
            dataframe.to_csv(custom_filepath, index=False)
            logger.success(f"Exported '{query_config.name}' to custom location: {custom_filepath}")

    def get_export_dir(self) -> Path:
        if self.export_dir is None:
            self.export_dir = self._build_export_dir(Path("export"))
            logger.info(f"Created export directory: {self.export_dir}")
        return self.export_dir

    def get_custom_export_dir(self) -> Path:
        if self.custom_export_path is None:
            raise ValueError("Custom export path has not been configured")
        if self.export_to_dir is None:
            self.export_to_dir = self._build_export_dir(Path(self.custom_export_path))
            logger.info(f"Created custom export directory: {self.export_to_dir}")
        return self.export_to_dir

    def _build_export_dir(self, base_path: Path) -> Path:
        now = datetime.now()
        date_str = now.strftime("%B_%d_%Y")
        time_str = now.strftime("%H%M%S")
        export_dir = base_path / f"export_{date_str}_{time_str}"
        export_dir.mkdir(parents=True, exist_ok=True)
        return export_dir

    @staticmethod
    def _preview(dataframe: pd.DataFrame) -> None:
        from rich.console import Console
        from rich.table import Table

        console = Console()

        # Data preview
        preview_df = dataframe.head(5)
        data_table = Table(show_lines=True, title="Preview")
        for col in preview_df.columns:
            data_table.add_column(str(col), header_style="bold cyan", style="white")
        for _, row in preview_df.iterrows():
            data_table.add_row(*[str(v) for v in row])
        if len(dataframe) > 5:
            data_table.caption = f"Showing 5 of {len(dataframe)} rows"
        console.print(data_table)

        # Column info
        info_table = Table(title="Column Info", show_lines=True)
        info_table.add_column("Column", header_style="bold cyan", style="white")
        info_table.add_column("Type", header_style="bold cyan", style="white")
        for col in dataframe.columns:
            info_table.add_row(str(col), str(dataframe[col].dtype))
        console.print(info_table)

