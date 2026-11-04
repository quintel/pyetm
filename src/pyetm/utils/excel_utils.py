"""Excel file utilities for reading and writing scenario data."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Set, Union, Tuple, cast, get_args
import numpy as np
import pandas as pd
from pandas import Series, Index
import datetime as dt
from xlsxwriter.workbook import Workbook  # type: ignore[import-untyped]
from xlsxwriter.worksheet import Worksheet  # type: ignore[import-untyped]
from pyetm.models.export_config import ExportConfig
from pyetm.types import AnnualExportType, CarrierType

logger = logging.getLogger(__name__)


# Export config resolution
class ExportConfigResolver:
    """Handles resolution of export configuration from various sources."""

    @staticmethod
    def resolve_boolean(
        explicit_value: Optional[bool], config_value: Optional[bool], default: bool
    ) -> bool:
        """Resolve boolean value from explicit parameter, config, or default."""
        if explicit_value is not None:
            return bool(explicit_value)
        if config_value is not None:
            return bool(config_value)
        return default

    @staticmethod
    def extract_from_export_config_sheet(
        export_config_df: pd.DataFrame,
    ) -> Optional[ExportConfig]:
        """Extract export configuration from a row-based EXPORT_CONFIG sheet (fields as columns, one row of values)."""
        if export_config_df is None or export_config_df.empty:
            return None

        try:
            row = export_config_df.iloc[0]
            return ExportConfig(
                include_inputs=_flag(row, "include_inputs", "inputs"),
                include_sortables=_flag(row, "include_sortables", "sortables"),
                include_custom_curves=_flag(row, "include_custom_curves", "custom_curves"),
                include_gqueries=_flag(row, "include_gqueries", "gquery_results")
                or _flag(row, "gqueries"),
                include_input_defaults=_flag(row, "include_input_defaults", "defaults"),
                include_input_min_max=_flag(row, "include_input_min_max", "min_max"),
                hourly_curves=_hourly_curves(row),
                include_annual_exports=_annual_exports(row.get("annual_exports")),
            )
        except Exception:
            logger.exception("EXPORT_CONFIG: could not be read")
            return None


def _parse_bool(value: Any) -> Optional[bool]:
    """Parse a cell as a boolean: bools, numbers, or yes/no style strings. None when blank or unclear."""
    if _is_blank(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, float, np.integer)):
        return bool(int(value)) if np.isfinite(value) else None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "y", "1"}:
            return True
        if normalized in {"false", "no", "n", "0"}:
            return False
    return None


def _is_blank(value: Any) -> bool:
    """An empty cell: missing, NaN, or whitespace only."""
    if isinstance(value, str):
        return not value.strip()
    return value is None or (isinstance(value, float) and pd.isna(value))


def _flag(row: Series[Any], *names: str) -> bool:
    """First readable boolean among the named cells, else False."""
    for name in names:
        value = _parse_bool(row.get(name))
        if value is not None:
            return value
    return False


def _parse_list(value: Any) -> Optional[List[str]]:
    """Split a comma-separated cell into trimmed entries."""
    if not isinstance(value, str) or not value.strip():
        return None
    return [entry.strip() for entry in value.split(",") if entry.strip()]


def _hourly_curves(row: Series[Any]) -> Optional[List[str]]:
    """Carriers or curve names from hourly_curves: true for all carriers, or a list. Blank means false."""
    value = row.get("hourly_curves")
    enabled = _parse_bool(value)
    if enabled is True:
        carriers = list(get_args(CarrierType))
        logger.info("EXPORT_CONFIG: hourly_curves set to 'true', exporting all carriers: %s", carriers)
        return carriers
    if enabled is False:
        logger.info("EXPORT_CONFIG: hourly_curves set to 'false', no hourly curves will be exported")
        return None

    parsed = _parse_list(row.get("output_carriers")) or _parse_list(value)
    if parsed:
        return _validated("hourly_curves", parsed, _validate_curves)
    if not _is_blank(value):
        logger.warning("EXPORT_CONFIG: hourly_curves could not be parsed, no hourly curves will be exported")
    return None


def _annual_exports(value: Any) -> Optional[List[str]]:
    """Export names from annual_exports: true for all types, or a list."""
    enabled = _parse_bool(value)
    if enabled is True:
        exports = list(get_args(AnnualExportType))
        logger.info("EXPORT_CONFIG: annual_exports set to 'true', exporting all types: %s", exports)
        return exports
    if enabled is False:
        logger.info("EXPORT_CONFIG: annual_exports set to 'false', no annual exports will be included")
        return None
    if not isinstance(value, str):
        return None

    parsed = _parse_list(value)
    if not parsed:
        logger.warning("EXPORT_CONFIG: annual_exports could not be parsed, no annual exports will be included")
        return None
    return _validated("annual_exports", parsed, _validate_exports)


def _validated(
    field: str,
    entries: List[str],
    validate: Callable[[List[str]], Tuple[List[str], List[str]]],
) -> Optional[List[str]]:
    """Run a packable validator, log its warnings, and return the valid entries or None."""
    valid, warnings = validate(entries)
    for warning in warnings:
        logger.warning("EXPORT_CONFIG: %s", warning)
    if not valid:
        logger.warning("EXPORT_CONFIG: all %s entries were invalid, none will be exported", field)
        return None
    logger.info("EXPORT_CONFIG: %s set to: %s", field, valid)
    return valid


# Packables import this module, so these imports stay deferred.
def _validate_curves(entries: List[str]) -> Tuple[List[str], List[str]]:
    from pyetm.models.packables.hourly_output_curves_pack import HourlyOutputCurvesPack

    return HourlyOutputCurvesPack.validate_curve_config(entries)


def _validate_exports(entries: List[str]) -> Tuple[List[str], List[str]]:
    from pyetm.models.packables.annual_exports_pack import AnnualExportsPack

    return AnnualExportsPack.validate_export_types(entries)


# Excel writing
def handle_numeric_value(
    worksheet: Worksheet,
    row: int,
    col: int,
    value: float,
    cell_format: Any = None,
    nan_as_formula: bool = True,
    decimal_precision: int = 10,
) -> int:
    """Handle numeric values with NaN and Inf support"""
    if np.isnan(value) or np.isinf(value):
        if nan_as_formula:
            return cast(int, worksheet.write_formula(row, col, "=NA()", cell_format, "#N/A"))
        return cast(int, worksheet.write(row, col, "N/A", cell_format))

    value = round(value, decimal_precision)

    return cast(int, worksheet.write_number(row, col, value, cell_format))


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
    worksheet: Worksheet, index: Index[Any], row_offset: int, bold_format: Any = None
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
                if isinstance(value, np.datetime64):
                    value = pd.Timestamp(value)
                worksheet.write(row + row_offset, col, value)
    else:
        for row, value in enumerate(index.values):
            if isinstance(value, np.datetime64):
                value = pd.Timestamp(value)
            worksheet.write(row + row_offset, 0, value)


def create_scenario_formats(workbook: Workbook) -> Dict[str, Any]:
    """Create alternating background formats for scenario blocks"""
    return {
        "white_header": workbook.add_format(
            {"bold": True, "bg_color": "#FFFFFF", "border": 1, "align": "center"}
        ),
        "grey_header": workbook.add_format(
            {"bold": True, "bg_color": "#D9D9D9", "border": 1, "align": "center"}
        ),
        "white_data": workbook.add_format({"bg_color": "#FFFFFF", "border": 1, "align": "left"}),
        "grey_data": workbook.add_format({"bg_color": "#D9D9D9", "border": 1, "align": "left"}),
        "bold": workbook.add_format({"bold": True}),
        "default": None,
    }


def get_scenario_blocks(columns: pd.MultiIndex) -> List[Tuple[Any, ...]]:
    """
    Identify scenario blocks in multi-index columns
    Returns list of (scenario_name, start_col, end_col) tuples
    """
    if not isinstance(columns, pd.MultiIndex):
        return []

    blocks = []
    current_scenario = None
    start_col = None

    for i, (scenario, *_) in enumerate(columns):
        if scenario != current_scenario:
            if current_scenario is not None:
                blocks.append((current_scenario, start_col, i - 1))
            current_scenario = scenario
            start_col = i

    # Add the last block
    if current_scenario is not None:
        blocks.append((current_scenario, start_col, len(columns) - 1))

    return blocks


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
    scenario_styling: bool = True,
    row_based_scenarios: bool = False,
) -> Worksheet:
    """Add DataFrame to Excel workbook as a new worksheet with formatting."""
    worksheet = workbook.add_worksheet(str(name))
    worksheet.add_write_handler(
        float,
        lambda ws, r, c, v, fmt=None: handle_numeric_value(
            ws, r, c, v, fmt, nan_as_formula, decimal_precision
        ),
    )

    if scenario_styling:
        formats = create_scenario_formats(workbook)
    else:
        formats = {"bold": workbook.add_format({"bold": True}) if bold_headers else None}
    bold_format = formats["bold"] if bold_headers else None

    col_offset = frame.index.nlevels if index else 0
    row_offset = frame.columns.nlevels
    header_format, data_format = _cell_formats(
        frame.columns, formats, bold_format, scenario_styling, row_based_scenarios
    )

    name_format = formats["bold"] if scenario_styling else bold_format
    _write_column_names(worksheet, frame.columns, col_offset, name_format)
    _write_headers(worksheet, frame.columns, col_offset, header_format)
    _write_data(worksheet, frame, row_offset, col_offset, data_format)

    set_column_widths(worksheet, col_offset, len(frame.columns), column_width)
    if index:
        set_column_widths(worksheet, 0, frame.index.nlevels, index_width or column_width)
        write_index(worksheet, frame.index, row_offset, bold_format)
    if freeze_panes:
        worksheet.freeze_panes(row_offset, col_offset)

    return worksheet


def _cell_formats(
    columns: pd.Index[Any],
    formats: Dict[str, Any],
    bold_format: Any,
    scenario_styling: bool,
    row_based_scenarios: bool,
) -> Tuple[Callable[[int], Any], Callable[[int, int], Any]]:
    """Return (header format by column, data format by row and column)."""
    if not scenario_styling:
        return (lambda col: bold_format), (lambda row, col: None)

    if row_based_scenarios and not isinstance(columns, pd.MultiIndex):
        return (lambda col: formats["white_header"]), (lambda row, col: _band(formats, "data", row))

    bands = _column_bands(columns)
    return (
        lambda col: _band(formats, "header", bands[col]),
        lambda row, col: _band(formats, "data", bands[col]),
    )


def _column_bands(columns: pd.Index[Any]) -> List[int]:
    """Band number per column: one band per scenario block, or per column when single level."""
    if not isinstance(columns, pd.MultiIndex):
        return list(range(len(columns)))

    return [
        band
        for band, (_, start, end) in enumerate(get_scenario_blocks(columns))
        for _ in range(start, end + 1)
    ]


def _band(formats: Dict[str, Any], kind: str, band: int) -> Any:
    """Alternate white and grey backgrounds."""
    return formats[f"grey_{kind}" if band % 2 else f"white_{kind}"]


def _write_column_names(
    worksheet: Worksheet, columns: pd.Index[Any], col_offset: int, cell_format: Any
) -> None:
    """Write MultiIndex level names in the column left of the headers."""
    if not isinstance(columns, pd.MultiIndex):
        return

    for level, level_name in enumerate(columns.names):
        if level_name is not None:
            worksheet.write(level, col_offset - 1, str(level_name), cell_format)


def _write_headers(
    worksheet: Worksheet,
    columns: pd.Index[Any],
    col_offset: int,
    header_format: Callable[[int], Any],
) -> None:
    """Write column headers, one row per column level."""
    for col_num, labels in enumerate(columns.values):
        if not isinstance(columns, pd.MultiIndex):
            labels = (labels,)
        for row_num, label in enumerate(labels):
            worksheet.write(row_num, col_num + col_offset, label, header_format(col_num))


def _write_data(
    worksheet: Worksheet,
    frame: pd.DataFrame,
    row_offset: int,
    col_offset: int,
    data_format: Callable[[int, int], Any],
) -> None:
    """Write cell values, turning lists (e.g. curve results) into strings."""
    for row_num, row_data in enumerate(frame.values):
        for col_num, value in enumerate(row_data):
            cell = str(value) if isinstance(value, list) else value
            worksheet.write(
                row_num + row_offset, col_num + col_offset, cell, data_format(row_num, col_num)
            )


# Dataframe prep and sanitization
def sanitize_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Convert DataFrame to Excel-compatible format."""
    if df is None or df.empty:
        return pd.DataFrame()

    sanitized_df = df.copy()

    # Sanitize index and columns
    sanitized_df.index = sanitized_df.index.map(sanitize_excel_value)
    sanitized_df.columns = pd.Index([sanitize_excel_value(col) for col in sanitized_df.columns])

    # Sanitize cell values
    sanitized_df = sanitized_df.map(sanitize_excel_value)

    return sanitized_df


def sanitize_excel_value(value: Any) -> Any:
    """Convert a single value to Excel-safe format."""
    if value is None:
        return ""

    if isinstance(value, (str, int, float, bool)):
        return value

    # Handle datetime objects
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        try:
            return str(value)
        except Exception:
            return ""

    # Handle lists by converting to comma-separated string
    if isinstance(value, (list, tuple, set)):
        try:
            return ", ".join(str(item) for item in value)
        except Exception:
            return str(value)

    # Generic fallback
    try:
        return str(value)
    except Exception:
        return ""


def build_excel_main_dataframe(main_df: pd.DataFrame) -> pd.DataFrame:
    """Build a MAIN sheet DataFrame for Excel export"""
    if main_df is None or main_df.empty:
        return pd.DataFrame()

    # Apply preferred field ordering
    ordered_df = apply_field_ordering(main_df)
    return ordered_df


def apply_field_ordering(df: pd.DataFrame) -> pd.DataFrame:
    """Apply preferred field ordering to DataFrame columns (for pivoted main sheet)."""
    # Fields to exclude from export (internal tracking only)
    excluded_fields = ["id", "identifier", "scenario_id", "preset"]

    # Preferred field order
    preferred_fields = [
        "title",
        "session_id",
        "saved_scenario_id",
        "description",
        "template_id",
        "area_code",
        "start_year",
        "end_year",
        "keep_compatible",
        "private",
        "source",
        "url",
        "version",
        "created_at",
        "updated_at",
    ]

    # Filter out excluded fields
    available_columns = [col for col in df.columns if col not in excluded_fields]

    # Build ordered field list
    present_fields = [field for field in preferred_fields if field in available_columns]
    remaining_fields = [field for field in available_columns if field not in present_fields]
    ordered_fields = present_fields + remaining_fields

    return df.loc[:, ordered_fields]


# Excel reading and parsing


def parse_excel_sheet(
    excel_file: Any, sheet_name: str, header: Any = None
) -> Optional[pd.DataFrame]:
    """Safely parse an Excel sheet, returning None if it fails or doesn't exist."""
    if sheet_name not in excel_file.sheet_names:
        return None
    try:
        return cast(pd.DataFrame, excel_file.parse(sheet_name, header=header))
    except Exception as e:
        return None


def find_first_non_empty_row(df: pd.DataFrame) -> Optional[int]:
    """Find the first row that contains non-empty data."""
    if df is None:
        return None

    for index, (_, row) in enumerate(df.iterrows()):
        try:
            if not row.isna().all():
                return index
        except Exception:
            # Fallback check for non-standard empty values
            if any(value not in (None, "", float("nan")) for value in row):
                return index

    return None


def normalize_sheet(
    df: pd.DataFrame,
    *,
    helper_names: Set[str],
    reset_index: bool = True,
    rename_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Normalize a sheet by finding headers and cleaning data."""
    if df is None:
        return pd.DataFrame()

    df = df.dropna(how="all")
    if df.empty:
        return df

    header_position = find_first_non_empty_row(df)
    if header_position is None:
        return pd.DataFrame()

    # Extract header and data
    header = df.iloc[header_position].astype(str).map(str.strip)
    data = df.iloc[header_position + 1 :].copy()
    data.columns = pd.Index(header.values)

    # Keep only non-helper columns
    columns_to_keep = [col for col in data.columns if not is_helper_column(col, helper_names)]
    data = data[columns_to_keep]

    # Apply column renaming if provided
    if rename_map:
        data = data.rename(columns=rename_map)

    if reset_index:
        data.reset_index(drop=True, inplace=True)

    return data


def is_helper_column(column_name: Any, helper_names: Set[str]) -> bool:
    """Check if a column is a helper column that should be ignored."""
    if not isinstance(column_name, str):
        return True

    normalized_name = column_name.strip().lower()
    return normalized_name in (helper_names or set()) or normalized_name in {"", "nan"}


# Scenario metadata extraction


def extract_scenario_sheet_info(main_df: pd.DataFrame) -> Dict[str, Dict[str, Optional[str]]]:
    """Extract sheet information for each scenario from main DataFrame."""
    if isinstance(main_df, pd.Series):
        return extract_single_scenario_sheet_info(main_df)
    else:
        return extract_multiple_scenario_sheet_info(main_df)


def extract_single_scenario_sheet_info(series: Series[Any]) -> Dict[str, Dict[str, Optional[str]]]:
    """Extract sheet info for single scenario (Series case)."""
    identifier = str(series.name)

    return {
        identifier: {
            "short_name": get_safe_value(series, "short_name", identifier),
            "sortables": get_value_before_output(series, "sortables"),
            "custom_curves": get_value_before_output(series, "custom_curves"),
        }
    }


def extract_multiple_scenario_sheet_info(df: pd.DataFrame) -> Dict[str, Dict[str, Optional[str]]]:
    """Extract sheet info for multiple scenarios"""
    scenario_sheets: Dict[str, Dict[str, Optional[str]]] = {}

    for idx, row in df.iterrows():
        key = str(idx)
        sortables = get_value_before_output(row, "sortables")
        custom_curves = get_value_before_output(row, "custom_curves")
        scenario_sheets[key] = {
            "short_name": get_safe_value(row, "short_name", key),
            "sortables": sortables,
            "custom_curves": custom_curves,
        }

    return scenario_sheets


def get_safe_value(series: Series[Any], key: str, default: str) -> str:
    """Safely get value from series with default fallback."""
    value = series.get(key)
    if pd.notna(value):
        return str(value)
    return default


def get_value_before_output(series: Series[Any], key: str) -> Optional[str]:
    """Get value from series, but only if it appears before 'output' section."""
    seen_output = False

    for label, value in zip(series.index, series.values):
        normalized_label = str(label).strip().lower()

        if normalized_label == "output":
            seen_output = True

        if normalized_label == key and not seen_output:
            return value if pd.notna(value) else None

    return None
