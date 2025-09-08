import logging
from typing import ClassVar, Dict, Any, List, Set
import pandas as pd
from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class InputsPack(Packable):
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

    def _extract_input_values(self, scenario, field_name: str) -> Dict[str, Any]:
        """Extract input values for a specific field from a scenario."""
        values = self._extract_from_input_objects(scenario, field_name)
        if values:
            return values

        return self._extract_from_dataframe(scenario, field_name)

    def _extract_from_input_objects(self, scenario, field_name: str) -> Dict[str, Any]:
        """Extract values by iterating through scenario input objects."""
        try:
            values = {}
            for input_obj in scenario.inputs:
                key = getattr(input_obj, "key", None)
                if key is None:
                    continue

                value = getattr(input_obj, field_name, None)
                values[str(key)] = value

            return values if values else {}
        except Exception:
            return {}

    def _extract_from_dataframe(self, scenario, field_name: str) -> Dict[str, Any]:
        """Extract values from scenario inputs DataFrame."""
        try:
            df = scenario.inputs.to_dataframe(columns=field_name)
        except Exception:
            return {}

        if df is None or getattr(df, "empty", False):
            return {}

        # Handle MultiIndex (drop 'unit' level if present)
        df = self._normalize_dataframe_index(df)
        series = self._dataframe_to_series(df, field_name)
        if series is None:
            return {}

        series.index = series.index.map(str)
        return series.to_dict()

    def _normalize_dataframe_index(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove 'unit' level from MultiIndex if present."""
        if isinstance(df.index, pd.MultiIndex) and "unit" in (df.index.names or []):
            df = df.copy()
            df.index = df.index.droplevel("unit")
        return df

    def _dataframe_to_series(self, df: pd.DataFrame, field_name: str) -> pd.Series:
        """Convert DataFrame to Series, selecting appropriate column."""
        if isinstance(df, pd.Series):
            return df
        columns_lower = {str(col).lower(): col for col in df.columns}
        for candidate in (field_name, "user", "value", "default"):
            if candidate in columns_lower:
                return df[columns_lower[candidate]]
        return df.iloc[:, 0]

    def to_dataframe(
        self,
        columns: str | List[str] = "user",
        *,
        include_defaults: bool = False,
        include_min_max: bool = False,
    ) -> pd.DataFrame:
        if not self.scenarios:
            return pd.DataFrame()

        # Normalize requested columns
        base_cols: List[str]
        if isinstance(columns, list):
            base_cols = [c for c in columns if c]
        else:
            base_cols = [str(columns)] if columns else ["user"]
        if not base_cols:
            base_cols = ["user"]

        if "user" not in base_cols:
            base_cols.insert(0, "user")

        if include_defaults and "default" not in base_cols:
            base_cols.append("default")
        if include_min_max:
            for extra in ("min", "max"):
                if extra not in base_cols:
                    base_cols.append(extra)

        frames: List[pd.DataFrame] = []
        labels: List[Any] = []
        for scenario in self.scenarios:
            try:
                df = scenario.inputs.to_dataframe(columns=base_cols)
            except Exception:
                continue
            if df is None or getattr(df, "empty", False):
                continue

            frames.append(df)
            labels.append(self._get_scenario_display_key(scenario))

        if not frames:
            return pd.DataFrame()

        merged = pd.concat(frames, axis=1, keys=labels, names=["scenario", "field"])
        return merged

    def _to_dataframe(self, columns="user", **kwargs):
        return self.to_dataframe(columns=columns)

    def from_dataframe(self, df):
        """Import input values from DataFrame."""
        if df is None or getattr(df, "empty", False):
            return

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

            for column_name in scenario_columns:
                scenario = self.resolve_scenario(column_name)
                if scenario is None:
                    logger.warning(
                        "Could not find scenario for SLIDER_SETTINGS column label '%s'",
                        column_name,
                    )
                    continue

                column_data = data_df[column_name]

                # Filter out blank values
                updates = {
                    key: value
                    for key, value in column_data.items()
                    if not self._is_blank_value(value)
                }

                if not updates:
                    continue
                try:
                    scenario.update_user_values(updates)
                except Exception as e:
                    logger.warning(
                        "Failed updating inputs for scenario '%s' from column '%s': %s",
                        scenario.identifier(),
                        column_name,
                        e,
                    )
                finally:
                    self._log_scenario_input_warnings(scenario)

        except Exception as e:
            logger.warning("Failed to parse simplified SLIDER_SETTINGS sheet: %s", e)

    def _is_blank_value(self, value: Any) -> bool:
        """Check if a value should be considered blank/empty."""
        if value is None:
            return True
        if isinstance(value, float) and pd.isna(value):
            return True
        if isinstance(value, str) and value.strip().lower() in {"", "nan"}:
            return True
        return False

    def _log_scenario_input_warnings(self, scenario):
        """Log any warnings from scenario inputs if available."""
        try:
            if hasattr(scenario, "_inputs") and scenario._inputs is not None:
                scenario._inputs.log_warnings(
                    logger,
                    prefix=f"Inputs warning for '{scenario.identifier()}'",
                )
        except Exception:
            pass
