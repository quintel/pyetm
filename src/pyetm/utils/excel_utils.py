from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Set, Union
import numpy as np
import pandas as pd
import datetime as dt
from xlsxwriter.workbook import Workbook
from xlsxwriter.worksheet import Worksheet
from pyetm.models.export_config import ExportConfig


# Export config resolution
class ExportConfigResolver:
    """Handles resolution of export configuration from various sources."""

    @staticmethod
    def extract_from_main_sheet(
        main: pd.DataFrame, scenarios: list
    ) -> Optional[ExportConfig]:
        """Extract export config from the main sheet, skipping helper columns."""
        if main is None or main.empty or not scenarios:
            return None
        # Find the first non-helper column
        for col in main.columns:
            if str(col).lower() == "helper":
                continue
            series = main[col]
            return ExportConfigResolver._parse_config_from_series(series)
        return None

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

            def parse_carriers(value: Any) -> Optional[List[str]]:
                if not isinstance(value, str) or not value.strip():
                    return None
                return [
                    carrier.strip() for carrier in value.split(",") if carrier.strip()
                ]

            rb = ExportConfigResolver.resolve_boolean
            get = row.get

            # Exports/output_carriers logic
            exports_val = get("exports")
            output_carriers = None
            if rb(exports_val, None, False) is True:
                output_carriers = ["electricity", "hydrogen", "heat", "methane"]
            elif rb(exports_val, None, False) is False:
                output_carriers = None
            else:
                output_carriers = parse_carriers(
                    get("output_carriers")
                ) or parse_carriers(exports_val)

            config = ExportConfig(
                include_inputs=rb(get("include_inputs"), get("inputs"), False),
                include_sortables=rb(get("include_sortables"), get("sortables"), False),
                include_custom_curves=rb(
                    get("include_custom_curves"), get("custom_curves"), False
                ),
                include_gqueries=rb(
                    get("include_gqueries"), get("gquery_results"), False
                )
                or rb(get("gqueries"), None, False),
                inputs_defaults=rb(get("defaults"), None, False),
                inputs_min_max=rb(get("min_max"), None, False),
                output_carriers=output_carriers,
            )
            return config
        except Exception as e:
            return None

    @staticmethod
    def _parse_config_from_series(series: pd.Series) -> "ExportConfig":
        """Parse ExportConfig from a pandas Series (column from main sheet)."""

        def _iter_rows():
            for label, value in zip(series.index, series.values):
                yield str(label).strip().lower(), value

        def _value_after_output(name: str) -> Any:
            target = name.strip().lower()
            seen_output = False
            chosen: Any = None
            for lbl, val in _iter_rows():
                if lbl == "output":
                    seen_output = True
                    continue
                if seen_output and lbl == target:
                    chosen = val
            return chosen

        def _value_any(name: str) -> Any:
            target = name.strip().lower()
            chosen: Any = None
            for lbl, val in _iter_rows():
                if lbl == target:
                    chosen = val
            return chosen

        def get_cell_value(name: str) -> Any:
            val = _value_after_output(name)
            return val if val is not None else _value_any(name)

        def parse_bool(value: Any) -> Optional[bool]:
            """Parse boolean from various formats."""
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                try:
                    return bool(int(value))
                except Exception:
                    return None
            if isinstance(value, str):
                normalized = value.strip().lower()
                if normalized in {"true", "yes", "y", "1"}:
                    return True
                if normalized in {"false", "no", "n", "0"}:
                    return False
            return None

        def parse_bool_field(*names: str) -> Optional[bool]:
            """Return the first non-None boolean parsed from the provided field names."""
            for n in names:
                val = parse_bool(get_cell_value(n))
                if val is not None:
                    return val
            return None

        def parse_carriers(value: Any) -> Optional[List[str]]:
            """Parse comma-separated carrier list."""
            if not isinstance(value, str) or not value.strip():
                return None
            return [carrier.strip() for carrier in value.split(",") if carrier.strip()]

        exports_val = get_cell_value("exports")
        carriers_val = get_cell_value("output_carriers")

        exports_bool = parse_bool(exports_val)
        if exports_bool is True:
            output_carriers = ["electricity", "hydrogen", "heat", "methane"]
        elif exports_bool is False:
            output_carriers = None
        else:
            output_carriers = parse_carriers(carriers_val) or parse_carriers(
                exports_val
            )

        config = ExportConfig(
            include_inputs=parse_bool_field("include_inputs", "inputs"),
            include_sortables=parse_bool_field("include_sortables", "sortables"),
            include_custom_curves=parse_bool_field(
                "include_custom_curves", "custom_curves"
            ),
            include_gqueries=parse_bool_field(
                "include_gqueries", "gquery_results", "gqueries"
            ),
            inputs_defaults=parse_bool(get_cell_value("defaults")),
            inputs_min_max=parse_bool(get_cell_value("min_max")),
            output_carriers=output_carriers,
        )
        return config


# Excel writing
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


def create_scenario_formats(workbook: Workbook) -> dict:
    """Create alternating background formats for scenario blocks"""
    return {
        "white_header": workbook.add_format(
            {"bold": True, "bg_color": "#FFFFFF", "border": 1, "align": "center"}
        ),
        "grey_header": workbook.add_format(
            {"bold": True, "bg_color": "#D9D9D9", "border": 1, "align": "center"}
        ),
        "white_data": workbook.add_format(
            {"bg_color": "#FFFFFF", "border": 1, "align": "left"}
        ),
        "grey_data": workbook.add_format(
            {"bg_color": "#D9D9D9", "border": 1, "align": "left"}
        ),
        "bold": workbook.add_format({"bold": True}),
        "default": None,
    }


def get_scenario_blocks(columns: pd.MultiIndex) -> List[tuple]:
    """
    Identify scenario blocks in multi-index columns
    Returns list of (scenario_name, start_col, end_col) tuples
    """
    if not isinstance(columns, pd.MultiIndex):
        return []

    blocks = []
    current_scenario = None
    start_col = None

    for i, (scenario, _) in enumerate(columns):
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
) -> Worksheet:
    """Add DataFrame to Excel workbook as a new worksheet with formatting."""

    # Create worksheet
    worksheet = workbook.add_worksheet(str(name))

    # Add numeric handler
    worksheet.add_write_handler(
        float,
        lambda ws, r, c, v, fmt=None: handle_numeric_value(
            ws, r, c, v, fmt, nan_as_formula, decimal_precision
        ),
    )

    # Create formats
    formats = (
        create_scenario_formats(workbook)
        if scenario_styling
        else {
            "bold": workbook.add_format({"bold": True}) if bold_headers else None,
            "default": None,
        }
    )

    # Calculate offsets
    col_offset = frame.index.nlevels if index else 0
    row_offset = frame.columns.nlevels

    # Handle multi-index columns with scenario styling
    if isinstance(frame.columns, pd.MultiIndex) and scenario_styling:
        # Get scenario blocks for alternating colors
        scenario_blocks = get_scenario_blocks(frame.columns)

        # Write column names
        if frame.columns.names != [None] * frame.columns.nlevels:
            for idx, name in enumerate(frame.columns.names):
                if name is not None:
                    worksheet.write(idx, col_offset - 1, name, formats["bold"])

        # Write column headers with alternating scenario backgrounds
        for col_num, values in enumerate(frame.columns.values):
            # Determine which scenario block this column belongs to
            scenario_idx = next(
                (
                    i
                    for i, (_, start, end) in enumerate(scenario_blocks)
                    if start <= col_num <= end
                ),
                0,
            )
            is_grey = scenario_idx % 2 == 1
            header_format = (
                formats["grey_header"] if is_grey else formats["white_header"]
            )

            for row_num, value in enumerate(values):
                worksheet.write(row_num, col_num + col_offset, value, header_format)

        # Write data with scenario block coloring
        for row_num, row_data in enumerate(frame.values):
            for col_num, value in enumerate(row_data):
                # Determine scenario block
                scenario_idx = next(
                    (
                        i
                        for i, (_, start, end) in enumerate(scenario_blocks)
                        if start <= col_num <= end
                    ),
                    0,
                )
                is_grey = scenario_idx % 2 == 1
                data_format = formats["grey_data"] if is_grey else formats["white_data"]

                worksheet.write(
                    row_num + row_offset, col_num + col_offset, value, data_format
                )

    else:
        # Standard column handling or single-index scenario styling
        bold_format = formats.get("bold") if bold_headers else None

        if isinstance(frame.columns, pd.MultiIndex):
            # Write column names without styling
            if frame.columns.names != [None] * frame.columns.nlevels:
                for idx, name in enumerate(frame.columns.names):
                    if name is not None:
                        worksheet.write(idx, col_offset - 1, name, bold_format)

            # Write column values
            for col_num, values in enumerate(frame.columns.values):
                for row_num, value in enumerate(values):
                    worksheet.write(row_num, col_num + col_offset, value, bold_format)

            # Write data without styling
            for row_num, row_data in enumerate(frame.values):
                for col_num, value in enumerate(row_data):
                    worksheet.write(row_num + row_offset, col_num + col_offset, value)
        else:
            # Single-level columns
            if scenario_styling:
                # Alternate header backgrounds by scenario column
                for col_num, value in enumerate(frame.columns.values):
                    is_grey = (col_num % 2) == 1
                    header_format = (
                        formats["grey_header"] if is_grey else formats["white_header"]
                    )
                    worksheet.write(
                        row_offset - 1, col_num + col_offset, value, header_format
                    )

                # Alternate data backgrounds by scenario column
                for row_num, row_data in enumerate(frame.values):
                    for col_num, value in enumerate(row_data):
                        is_grey = (col_num % 2) == 1
                        data_format = (
                            formats["grey_data"] if is_grey else formats["white_data"]
                        )
                        worksheet.write(
                            row_num + row_offset,
                            col_num + col_offset,
                            value,
                            data_format,
                        )
            else:
                # No scenario styling: write simple headers and data
                for col_num, value in enumerate(frame.columns.values):
                    worksheet.write(
                        row_offset - 1, col_num + col_offset, value, bold_format
                    )

                for row_num, row_data in enumerate(frame.values):
                    for col_num, value in enumerate(row_data):
                        worksheet.write(
                            row_num + row_offset, col_num + col_offset, value
                        )

    # Set column widths
    set_column_widths(worksheet, col_offset, len(frame.columns), column_width)

    if index:
        set_column_widths(
            worksheet, 0, frame.index.nlevels, index_width or column_width
        )

        # Create index format matching the styling
        index_format = formats.get("bold") if bold_headers else None
        write_index(worksheet, frame.index, row_offset, index_format)

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
    """Add Series to Excel workbook as a new worksheet."""

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


# Dataframe prep and sanitization
def sanitize_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Convert DataFrame to Excel-compatible format."""
    if df is None or df.empty:
        return pd.DataFrame()

    sanitized_df = df.copy()

    # Sanitize index and columns
    sanitized_df.index = sanitized_df.index.map(sanitize_excel_value)
    sanitized_df.columns = [sanitize_excel_value(col) for col in sanitized_df.columns]

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


def build_excel_main_dataframe(main_df: pd.DataFrame, scenarios: List) -> pd.DataFrame:
    """Build a MAIN sheet DataFrame for Excel export"""
    if main_df is None or main_df.empty:
        return pd.DataFrame()

    # Apply preferred field ordering
    ordered_df = apply_field_ordering(main_df)
    return ordered_df


def apply_field_ordering(df: pd.DataFrame) -> pd.DataFrame:
    """Apply preferred field ordering to DataFrame columns (for pivoted main sheet)."""
    preferred_fields = [
        "title",
        "description",
        "scenario_id",
        "template",
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
    present_fields = [field for field in preferred_fields if field in df.columns]
    remaining_fields = [field for field in df.columns if field not in present_fields]
    ordered_fields = present_fields + remaining_fields
    return df.loc[:, ordered_fields]


def apply_scenario_column_labels(df: pd.DataFrame, scenarios: List) -> pd.DataFrame:
    """Apply human-readable labels to scenario columns."""
    try:
        column_rename_map = build_column_rename_map(scenarios, df.columns)

        if column_rename_map:
            return df.rename(columns=column_rename_map)
        return df
    except Exception:
        # If renaming fails, return original DataFrame
        return df


def build_column_rename_map(scenarios: List, columns) -> Dict[Any, str]:
    """Build mapping of column IDs to human-readable labels."""
    rename_map = {}
    scenarios_by_id = {str(getattr(s, "id", "")): s for s in scenarios}

    for column in columns:
        matched_scenario = find_matching_scenario(column, scenarios, scenarios_by_id)
        if matched_scenario is not None:
            label = get_scenario_display_label(matched_scenario, column)
            rename_map[column] = label

    return rename_map


def find_matching_scenario(
    column, scenarios: List, scenarios_by_id: Dict[str, Any]
) -> Optional[Any]:
    """Find scenario matching the given column identifier."""
    # Try exact ID match first
    for scenario in scenarios:
        if column == getattr(scenario, "id", None):
            return scenario

    # Try string ID match as fallback
    return scenarios_by_id.get(str(column))


def get_scenario_display_label(scenario, fallback_column) -> str:
    """Get display label for scenario, with fallbacks."""
    try:
        if hasattr(scenario, "identifier"):
            return scenario.identifier()
    except Exception:
        pass

    # Try title attribute
    title = getattr(scenario, "title", None)
    if title:
        return title

    # Try ID attribute
    scenario_id = getattr(scenario, "id", None)
    if scenario_id:
        return str(scenario_id)

    # Final fallback
    return str(fallback_column)


# Excel reading and parsing


def parse_excel_sheet(
    excel_file, sheet_name: str, header=None
) -> Optional[pd.DataFrame]:
    """Safely parse an Excel sheet, returning None if it fails or doesn't exist."""
    if sheet_name not in excel_file.sheet_names:
        return None
    try:
        return excel_file.parse(sheet_name, header=header)
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
    data.columns = header.values

    # Keep only non-helper columns
    columns_to_keep = [
        col for col in data.columns if not is_helper_column(col, helper_names)
    ]
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


def extract_scenario_sheet_info(main_df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    """Extract sheet information for each scenario from main DataFrame."""
    if isinstance(main_df, pd.Series):
        return extract_single_scenario_sheet_info(main_df)
    else:
        return extract_multiple_scenario_sheet_info(main_df)


def extract_single_scenario_sheet_info(series: pd.Series) -> Dict[str, Dict[str, str]]:
    """Extract sheet info for single scenario (Series case)."""
    identifier = str(series.name)

    return {
        identifier: {
            "short_name": get_safe_value(series, "short_name", identifier),
            "sortables": get_value_before_output(series, "sortables"),
            "custom_curves": get_value_before_output(series, "custom_curves"),
        }
    }


def extract_multiple_scenario_sheet_info(df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    """Extract sheet info for multiple scenarios"""
    scenario_sheets: Dict[str, Dict[str, str]] = {}

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


def get_safe_value(series: pd.Series, key: str, default: str) -> str:
    """Safely get value from series with default fallback."""
    value = series.get(key)
    if pd.notna(value):
        return str(value)
    return default


def get_value_before_output(series: pd.Series, key: str) -> Optional[str]:
    """Get value from series, but only if it appears before 'output' section."""
    seen_output = False

    for label, value in zip(series.index, series.values):
        normalized_label = str(label).strip().lower()

        if normalized_label == "output":
            seen_output = True

        if normalized_label == key and not seen_output:
            return value if pd.notna(value) else None

    return None
