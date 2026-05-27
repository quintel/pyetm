"""Input packing utilities for batch scenario operations."""

import logging
from typing import ClassVar, Dict, Any, List, Optional
from openpyxl import Workbook  # type: ignore[import-untyped]
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

    def to_dataframe(
        self,
        columns: str | List[str] = "value",
        *,
        include_defaults: bool = False,
        include_min_max: bool = False,
    ) -> pd.DataFrame:
        if not self.scenarios:
            return pd.DataFrame()

        if isinstance(columns, str):
            columns = [columns] if columns else []
        else:
            columns = [c for c in columns if c]

        if include_defaults:
            for col in ("default", "permitted_values"):
                if col not in columns:
                    columns.append(col)
        if include_min_max:
            for col in ("min", "max"):
                if col not in columns:
                    columns.append(col)

        frames, labels = [], []
        for scenario in self.scenarios:
            df = scenario.inputs.to_dataframe(columns=columns)
            if df is not None and not df.empty:
                frames.append(df)
                labels.append(self._get_scenario_display_key(scenario))

        return (
            pd.concat(frames, axis=1, keys=labels, names=["scenario", "field"])
            if frames
            else pd.DataFrame()
        )

    def add_to_workbook(
        self,
        workbook: Any,
        sheet_name: Optional[str] = "SLIDER_SETTINGS",
        **kwargs: Any,
    ) -> None:
        """Add inputs sheet with proper field handling. Optionally override sheet name."""
        # Extract our specific parameters from kwargs
        include_defaults = kwargs.get("include_defaults", False)
        include_min_max = kwargs.get("include_min_max", False)

        name = sheet_name or "SLIDER_SETTINGS"
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

    def from_dataframe(self, df: Any, update_set: Optional[set[str]] = None) -> None:
        """Import input values from DataFrame."""
        if df is None or getattr(df, "empty", False):
            return

        skip_upload = not self._should_include_upload(update_set or set())

        try:
            # Extract grid data using base class helper
            data_df = self._extract_grid_data(df)
            if data_df is None:
                return

            # Prepare grid data using base class helper
            data_df, input_keys, scenario_columns = self._prepare_grid_data(data_df)

            for column_name in scenario_columns:
                # Resolve scenario with automatic warning
                scenario = self._resolve_scenario_with_warning(
                    column_name, "SLIDER_SETTINGS sheet"
                )
                if scenario is None:
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
            logger.warning("Failed to parse SLIDER_SETTINGS sheet: %s", e)
