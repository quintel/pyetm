import logging
from typing import ClassVar, Dict, Any, List, Optional
from openpyxl import Workbook
import pandas as pd
import numpy as np
from pyetm.models.packables.packable import Packable
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class InputsPack(Packable):
    """
    InputsPack handles the import, export, and management of scenario input values,
    including support for scenario short names, flexible scenario resolution,
    and comprehensive data validation.

    Features:
        - Optional inclusion of default values and min/max ranges
        - Multi-level column headers for organized data export
        - Proper handling of list/tuple values in Excel export
        - Comprehensive error handling with detailed logging
    """

    key: ClassVar[str] = "inputs"
    sheet_name: ClassVar[str] = "SLIDER_SETTINGS"

    def __init__(self, **data):
        super().__init__(**data)
        self._scenario_short_names: Dict[str, str] = {}

    def set_scenario_short_names(self, scenario_short_names: Dict[str, str]):
        """Set mapping of scenario IDs to short names for display purposes."""
        self._scenario_short_names = scenario_short_names or {}

    def _get_scenario_display_key(self, scenario: "Any") -> Any:
        """Get the display key for a scenario (short name, identifier, or ID)."""
        short_name = self._scenario_short_names.get(str(scenario.id))
        if short_name:
            return short_name

        try:
            identifier = scenario.identifier()
            if isinstance(identifier, (str, int)):
                return identifier
        except Exception:
            pass

        return scenario.id

    def resolve_scenario(self, label: Any):
        """Resolve a scenario from various label formats (short name, identifier, or numeric ID)."""
        if label is None:
            return None

        label_str = str(label).strip()

        # Try short name first
        for scenario in self.scenarios:
            if self._scenario_short_names.get(str(scenario.id)) == label_str:
                return scenario

        # Identifier/title
        found_scenario = super().resolve_scenario(label_str)
        if found_scenario is not None:
            return found_scenario

        # Try numeric ID as fallback
        try:
            numeric_id = int(float(label_str))
            for scenario in self.scenarios:
                if scenario.id == numeric_id:
                    return scenario
        except (ValueError, TypeError):
            pass

        return None

    def to_dataframe(
        self,
        columns: str | List[str] = "user",
        *,
        include_defaults: bool = False,
        include_min_max: bool = False,
    ) -> pd.DataFrame:
        if not self.scenarios:
            return pd.DataFrame()

        if isinstance(columns, str):
            cols = [columns] if columns else []
        else:
            cols = [c for c in columns if c]
        if "user" in cols:
            cols.remove("user")
        cols.insert(0, "user")

        if include_defaults:
            for col in ("default", "permitted_values"):
                if col not in cols:
                    cols.append(col)
        if include_min_max:
            for col in ("min", "max"):
                if col not in cols:
                    cols.append(col)

        frames, labels = [], []
        for scenario in self.scenarios:
            df = scenario.inputs.to_dataframe(columns=cols)
            if df is not None and not df.empty:
                frames.append(df)
                labels.append(self._get_scenario_display_key(scenario))

        return (
            pd.concat(frames, axis=1, keys=labels, names=["scenario", "field"])
            if frames
            else pd.DataFrame()
        )

    def _to_dataframe(self, columns="user", **kwargs):
        return self.to_dataframe(columns=columns)

    def add_to_workbook(
        self,
        workbook: Workbook,
        include_defaults: bool = False,
        include_min_max: bool = False,
        sheet_name: str = None,
    ):
        """Add inputs sheet with proper field handling. Optionally override sheet name."""
        name = sheet_name if sheet_name else self.sheet_name
        df = self.to_dataframe(
            include_defaults=include_defaults, include_min_max=include_min_max
        )
        if df is not None and not df.empty:
            df = df.map(
                lambda v: (
                    ", ".join(map(str, v)) if isinstance(v, (list, tuple, set)) else v
                )
            )
            self._add_dataframe_to_workbook(workbook, name, df)

    def import_from_excel(
        self,
        excel_file: pd.ExcelFile,
        main_df: Optional[pd.DataFrame] = None,
        scenarios_by_column: Optional[Dict[str, Any]] = None,
        update_set: set[str] = None,
    ):
        """Import inputs sheet from Excel file."""
        df = excel_utils.parse_excel_sheet(excel_file, self.sheet_name, header=None)
        if df is not None and not df.empty:
            self.from_dataframe(df, update_set)

    def from_dataframe(self, df, update_set: set[str] = None):
        """Import input values from DataFrame."""
        if df is None or getattr(df, "empty", False):
            return

        skip_upload = not self._should_include_upload(update_set)

        try:
            df = df.dropna(how="all")
            if df.empty:
                return

            header_positions = self.first_non_empty_row_positions(df, 1)
            if not header_positions:
                return

            header_row_index = header_positions[0]
            header_row = df.iloc[header_row_index].astype(str)

            # Extract data rows
            data_df = df.iloc[header_row_index + 1 :].copy()
            data_df.columns = header_row.values

            if data_df.empty or len(data_df.columns) < 2:
                return

            # Process input data
            input_column = data_df.columns[0]
            input_keys = data_df[input_column].astype(str).str.strip()

            # Filter out empty input keys
            valid_mask = input_keys != ""
            data_df = data_df.loc[valid_mask]
            input_keys = input_keys.loc[valid_mask]
            data_df.index = input_keys

            # Process each scenario column
            scenario_columns = [col for col in data_df.columns if col != input_column]
            data_df[scenario_columns] = data_df[scenario_columns].replace(
                {"": np.nan, "nan": np.nan}
            )
            for column_name in scenario_columns:
                scenario = self.resolve_scenario(column_name)
                if scenario is None:
                    logger.warning(
                        "Could not find scenario for SLIDER_SETTINGS column label '%s'",
                        column_name,
                    )
                    continue

                raw_updates = data_df[column_name].dropna().to_dict()
                if not raw_updates:
                    continue

                try:
                    scenario.update_user_values(raw_updates, skip_upload=skip_upload)
                except Exception as e:
                    logger.warning(
                        "Failed updating inputs for scenario '%s' from column '%s': %s",
                        scenario.identifier(),
                        column_name,
                        e,
                    )
                finally:
                    self.log_scenario_warnings(scenario, "_inputs", "Inputs")

        except Exception as e:
            logger.warning("Failed to parse simplified SLIDER_SETTINGS sheet: %s", e)
