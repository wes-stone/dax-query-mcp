#!/usr/bin/env python3
"""Compatibility entrypoint for the dax-query CLI."""

from dax_query_mcp.cli import main


if __name__ == "__main__":
    raise SystemExit(main())

