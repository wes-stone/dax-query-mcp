from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd

DEFAULT_DATE_FORMAT = "%b-%d-%Y"

_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def _format_dates(dataframe: pd.DataFrame, date_format: str) -> pd.DataFrame:
    """Return a copy with datetime columns rendered as formatted strings."""
    df = dataframe.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime(date_format)
    return df


def preview_records(
    dataframe: pd.DataFrame,
    preview_rows: int,
    *,
    date_format: str = DEFAULT_DATE_FORMAT,
) -> list[dict[str, Any]]:
    df = _format_dates(dataframe.head(preview_rows), date_format)
    preview_json = df.to_json(orient="records", date_format="iso")
    return json.loads(preview_json)


def dataframe_to_markdown(
    dataframe: pd.DataFrame,
    *,
    max_rows: int,
    date_format: str = DEFAULT_DATE_FORMAT,
) -> str:
    preview = preview_records(dataframe, max_rows, date_format=date_format)
    columns = [_ANSI_ESCAPE_RE.sub("", str(column)) for column in dataframe.columns]
    if not columns:
        return "_No columns_"
    if not preview:
        return "| " + " | ".join(columns) + " |\n|" + "|".join([" --- " for _ in columns]) + "|\n| _no rows_ |"

    header = "| " + " | ".join(_escape_cell(column) for column in columns) + " |"
    separator = "|" + "|".join([" --- " for _ in columns]) + "|"
    rows = []
    for record in preview:
        rows.append("| " + " | ".join(_escape_cell(record.get(column, "")) for column in columns) + " |")
    return "\n".join([header, separator, *rows])


def dataframe_dtypes_to_markdown(dataframe: pd.DataFrame) -> str:
    dtype_frame = pd.DataFrame(
        {
            "column": [str(column) for column in dataframe.columns],
            "dtype": [str(dtype) for dtype in dataframe.dtypes],
        }
    )
    return dataframe_to_markdown(dtype_frame, max_rows=max(1, len(dtype_frame)))


def _escape_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    text = _ANSI_ESCAPE_RE.sub("", text)
    return text.replace("|", "\\|").replace("\n", " ")
