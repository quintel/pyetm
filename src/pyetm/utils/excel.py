from __future__ import annotations

import math
from typing import Union, List, Optional
import numpy as np
import pandas as pd
from xlsxwriter.workbook import Workbook
from xlsxwriter.worksheet import Worksheet


def handle_numeric_value(
    worksheet: Worksheet,
    row: int,
    col: int,
    value: float,
    cell_format=None,
    nan_as_formula: bool = True,
    decimal_precision: int = 10,
) -> int:
    """Handle numeric values with NaN support"""
    if np.isnan(value):
        if nan_as_formula:
            return worksheet.write_formula(row, col, "=NA()", cell_format, "#N/A")
        return worksheet.write(row, col, "N/A", cell_format)

    # Set decimal precision
    factor = 10**decimal_precision
    value = math.ceil(value * factor) / factor

    return worksheet.write_number(row, col, value, cell_format)


def set_column_widths(
    worksheet: Worksheet,
    start_col: int,
    num_cols: int,
    width: Union[int, List[int], None],
) -> None:
    """Set column widths in worksheet"""
    if width is None:
        return

    if isinstance(width, list):
        if len(width) != num_cols:
            raise ValueError(f"Expected {num_cols} widths, got {len(width)}")
        for i, w in enumerate(width):
            worksheet.set_column(start_col + i, start_col + i, w)
    else:
        worksheet.set_column(start_col, start_col + num_cols - 1, width)


def write_index(
    worksheet: Worksheet, index: pd.Index, row_offset: int, bold_format=None
) -> None:
    """Write pandas index to worksheet"""
    # Write index names if they exist
    if index.names != [None] * index.nlevels:
        for col, name in enumerate(index.names):
            if name is not None:
                worksheet.write(row_offset - 1, col, name, bold_format)

    # Write index values
    if isinstance(index, pd.MultiIndex):
        for row, values in enumerate(index.values):
            for col, value in enumerate(values):
                worksheet.write(row + row_offset, col, value)
    else:
        for row, value in enumerate(index.values):
            worksheet.write(row + row_offset, 0, value)


def add_frame(
    name: str,
    frame: pd.DataFrame,
    workbook: Workbook,
    index: bool = True,
    column_width: Union[int, List[int], None] = None,
    index_width: Union[int, List[int], None] = None,
    freeze_panes: bool = True,
    bold_headers: bool = True,
    nan_as_formula: bool = True,
    decimal_precision: int = 10,
) -> Worksheet:
    """Add DataFrame to workbook as a new worksheet"""

    # Create worksheet
    worksheet = workbook.add_worksheet(str(name))

    # Add numeric handler
    worksheet.add_write_handler(
        float,
        lambda ws, r, c, v, fmt=None: handle_numeric_value(
            ws, r, c, v, fmt, nan_as_formula, decimal_precision
        ),
    )

    # Create bold format
    bold_format = workbook.add_format({"bold": True}) if bold_headers else None

    # Calculate offsets
    col_offset = frame.index.nlevels if index else 0
    row_offset = frame.columns.nlevels

    # Adjust row offset if index has names
    if index and frame.index.names != [None] * frame.index.nlevels:
        row_offset += 1

    # Write column headers
    if isinstance(frame.columns, pd.MultiIndex):
        # Write column names
        if index and frame.columns.names != [None] * frame.columns.nlevels:
            for idx, name in enumerate(frame.columns.names):
                if name is not None:
                    worksheet.write(idx, col_offset - 1, name, bold_format)

        # Write column values
        for col_num, values in enumerate(frame.columns.values):
            for row_num, value in enumerate(values):
                worksheet.write(row_num, col_num + col_offset, value, bold_format)
    else:
        # Write simple column headers
        for col_num, value in enumerate(frame.columns.values):
            worksheet.write(row_offset - 1, col_num + col_offset, value, bold_format)

    # Set column widths
    set_column_widths(worksheet, col_offset, len(frame.columns), column_width)

    # Write data
    for row_num, row_data in enumerate(frame.values):
        for col_num, value in enumerate(row_data):
            worksheet.write(row_num + row_offset, col_num + col_offset, value)

    # Write index
    if index:
        set_column_widths(
            worksheet, 0, frame.index.nlevels, index_width or column_width
        )
        write_index(worksheet, frame.index, row_offset, bold_format)

    # Freeze panes
    if freeze_panes:
        worksheet.freeze_panes(row_offset, col_offset)

    return worksheet


def add_series(
    name: str,
    series: pd.Series,
    workbook: Workbook,
    index: bool = True,
    column_width: Optional[int] = None,
    index_width: Union[int, List[int], None] = None,
    freeze_panes: bool = True,
    bold_headers: bool = True,
    nan_as_formula: bool = True,
    decimal_precision: int = 10,
) -> Worksheet:
    """Add Series to workbook as a new worksheet"""

    # Create worksheet
    worksheet = workbook.add_worksheet(str(name))

    # Add numeric handler
    worksheet.add_write_handler(
        float,
        lambda ws, r, c, v, fmt=None: handle_numeric_value(
            ws, r, c, v, fmt, nan_as_formula, decimal_precision
        ),
    )

    # Create bold format if needed
    bold_format = workbook.add_format({"bold": True}) if bold_headers else None

    # Calculate offsets
    col_offset = series.index.nlevels if index else 0

    # Write header
    header = str(series.name) if series.name is not None else "Series"
    if isinstance(series.name, (list, tuple)):
        header = "_".join(map(str, series.name))

    worksheet.write(0, col_offset, header, bold_format)

    # Set column width
    if column_width:
        worksheet.set_column(col_offset, col_offset, column_width)

    # Write data
    for row_num, value in enumerate(series.values):
        worksheet.write(row_num + 1, col_offset, value)

    # Write index
    if index:
        set_column_widths(
            worksheet, 0, series.index.nlevels, index_width or column_width
        )
        write_index(worksheet, series.index, 1, bold_format)

    # Freeze panes
    if freeze_panes:
        worksheet.freeze_panes(1, col_offset)

    return worksheet
