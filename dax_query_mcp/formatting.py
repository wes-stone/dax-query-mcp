from __future__ import annotations

import json
from typing import Any

import pandas as pd


def preview_records(dataframe: pd.DataFrame, preview_rows: int) -> list[dict[str, Any]]:
    preview_json = dataframe.head(preview_rows).to_json(orient="records", date_format="iso")
    return json.loads(preview_json)


def dataframe_to_markdown(dataframe: pd.DataFrame, *, max_rows: int) -> str:
    preview = preview_records(dataframe, max_rows)
    columns = [str(column) for column in dataframe.columns]
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
    return text.replace("|", "\\|").replace("\n", " ")
