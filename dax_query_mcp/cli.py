from __future__ import annotations

import argparse
import json
import sys
import time

from loguru import logger

from .mcp_server import inspect_connection_metadata
from .pipeline import DAXPipeline
from .query_builder import load_query_builder_definition_file, save_query_builder_artifacts


def main() -> int:
    start_time = time.time()
    parser = _build_parser()
    args = parser.parse_args()

    _configure_logger(debug=args.debug)

    if args.save_query_builder_from:
        definition = load_query_builder_definition_file(args.save_query_builder_from)
        payload = save_query_builder_artifacts(
            definition,
            queries_dir=args.config_dir,
            overwrite=args.overwrite_query_builder,
        )
        print(json.dumps(payload, indent=2, default=str))
        logger.info(f"Query builder save completed in {time.time() - start_time:.2f}s")
        return 0

    if args.inspect_connection:
        payload = inspect_connection_metadata(
            args.inspect_connection,
            connections_dir=args.connections_dir,
            preview_rows=args.preview_rows,
            command_timeout_seconds=args.command_timeout_seconds,
        )
        print(json.dumps(payload, indent=2, default=str))
        logger.info(f"Connection inspection completed in {time.time() - start_time:.2f}s")
        return 0

    pipeline = DAXPipeline(args.config_dir, args.export_to)

    if args.list:
        pipeline.list_queries()
        logger.info(f"Pipeline execution completed in {time.time() - start_time:.2f}s")
        return 0

    if args.query:
        pipeline.run_query(args.query, preview=args.preview, export=args.export)
    else:
        pipeline.run_all_queries(preview=args.preview, export=args.export)

    logger.info(f"Pipeline execution completed in {time.time() - start_time:.2f}s")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DAX query runner and query-builder tooling for Power BI semantic models"
    )
    parser.add_argument("--preview", action="store_true", help="Show preview of query results")
    parser.add_argument("--export", action="store_true", help="Export results to CSV files")
    parser.add_argument(
        "--export-to",
        type=str,
        help="Additional export location (exports to both default and custom location)",
    )
    parser.add_argument("--query", type=str, help="Run specific query by name")
    parser.add_argument("--list", action="store_true", help="List available queries")
    parser.add_argument(
        "--save-query-builder-from",
        type=str,
        help="Path to a JSON query builder definition file to save as .dax and .dax.queryBuilder",
    )
    parser.add_argument(
        "--overwrite-query-builder",
        action="store_true",
        help="Allow --save-query-builder-from to overwrite existing query builder artifacts",
    )
    parser.add_argument(
        "--inspect-connection",
        type=str,
        help="Inspect a named connection using built-in MDSCHEMA metadata queries",
    )
    parser.add_argument(
        "--connections-dir",
        type=str,
        default="Connections",
        help="Directory containing connection configurations for --inspect-connection",
    )
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=10,
        help="Number of preview rows to include in metadata inspection output",
    )
    parser.add_argument(
        "--command-timeout-seconds",
        type=int,
        help="Override command timeout for metadata inspection queries",
    )
    parser.add_argument(
        "--config-dir",
        type=str,
        default="queries",
        help="Directory containing query configurations or saved query-builder artifacts",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser


def _configure_logger(*, debug: bool) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        ),
        level="DEBUG" if debug else "INFO",
    )

