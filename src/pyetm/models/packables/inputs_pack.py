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

    def _build_consolidated_dataframe(
        self, field_mappings: Dict[Any, List[str]]
    ) -> pd.DataFrame:
        """Build DataFrame with different fields per scenario."""
        if not self.scenarios:
            return pd.DataFrame()
        relevant_scenarios = {s for s in self.scenarios if s in field_mappings}
        if not relevant_scenarios:
            return pd.DataFrame()
        all_input_keys = self._collect_all_input_keys(
            relevant_scenarios, field_mappings
        )
        if not all_input_keys:
            return pd.DataFrame()
        sorted_keys = sorted(all_input_keys)

        scenario_frames = []
        scenario_labels = []

        for scenario in relevant_scenarios:
            scenario_label = self._get_scenario_display_key(scenario)
            fields = field_mappings.get(scenario, ["user"]) or ["user"]

            scenario_data = self._build_scenario_data(scenario, fields, sorted_keys)
            if not scenario_data:
                continue

            scenario_df = pd.DataFrame(scenario_data, index=sorted_keys)
            scenario_df.index.name = "input"
            scenario_frames.append(scenario_df)
            scenario_labels.append(scenario_label)

        if not scenario_frames:
            return pd.DataFrame()

        return pd.concat(
            scenario_frames, axis=1, keys=scenario_labels, names=["scenario", "field"]
        )

    def _collect_all_input_keys(
        self, scenarios: Set[Any], field_mappings: Dict[Any, List[str]]
    ) -> Set[str]:
        """Collect all unique input keys across scenarios and fields."""
        all_keys = set()
        for scenario in scenarios:
            fields = field_mappings.get(scenario, ["user"]) or ["user"]
            for field in fields:
                input_values = self._extract_input_values(scenario, field)
                all_keys.update(input_values.keys())
        return all_keys

    def _build_scenario_data(
        self, scenario, fields: List[str], sorted_keys: List[str]
    ) -> Dict[str, List[Any]]:
        """Build data dictionary for a single scenario across multiple fields."""
        data = {}
        for field in fields:
            value_mapping = self._extract_input_values(scenario, field) or {}
            data[field] = [value_mapping.get(key) for key in sorted_keys]
        return data

    def _build_simple_dataframe(self, field_name: str = "user") -> pd.DataFrame:
        """Build simple DataFrame with one field per scenario."""
        if not self.scenarios:
            return pd.DataFrame()

        all_input_keys = set()
        scenario_data = {}

        # Collect data from all scenarios
        for scenario in self.scenarios:
            scenario_label = self._get_scenario_display_key(scenario)
            input_values = self._extract_input_values(scenario, field_name)

            if not input_values:
                continue

            scenario_data[scenario_label] = input_values
            all_input_keys.update(input_values.keys())

        if not all_input_keys:
            return pd.DataFrame()

        # Build DataFrame
        sorted_keys = sorted(all_input_keys)
        data = {}
        for scenario_label, values in scenario_data.items():
            data[scenario_label] = [values.get(key) for key in sorted_keys]

        df = pd.DataFrame(data, index=sorted_keys)
        df.index.name = "input"
        return df

    def _build_bounds_dataframe(self) -> pd.DataFrame:
        """Build DataFrame with min/max bounds (assumes identical across scenarios)."""
        if not self.scenarios:
            return pd.DataFrame()

        # Collect all input keys
        all_input_keys = set()
        for scenario in self.scenarios:
            try:
                keys = [
                    str(getattr(inp, "key", ""))
                    for inp in scenario.inputs
                    if getattr(inp, "key", None)
                ]
            except Exception:
                try:
                    df = scenario.inputs.to_dataframe(columns=["min", "max"])
                    df = self._normalize_dataframe_index(df)
                    keys = [str(idx) for idx in df.index.unique()]
                except Exception:
                    keys = []

            all_input_keys.update(key for key in keys if key)

        if not all_input_keys:
            return pd.DataFrame()

        sorted_keys = sorted(all_input_keys)

        min_values = {}
        max_values = {}

        for scenario in self.scenarios:
            min_mapping = self._extract_input_values(scenario, "min") or {}
            max_mapping = self._extract_input_values(scenario, "max") or {}

            for key in sorted_keys:
                if key not in min_values and key in min_mapping:
                    min_values[key] = min_mapping[key]
                if key not in max_values and key in max_mapping:
                    max_values[key] = max_mapping[key]

            if len(min_values) == len(sorted_keys) and len(max_values) == len(
                sorted_keys
            ):
                break

        data = {
            ("", "min"): [min_values.get(key) for key in sorted_keys],
            ("", "max"): [max_values.get(key) for key in sorted_keys],
        }
        df = pd.DataFrame(data, index=sorted_keys)
        df.index.name = "input"
        df.columns = pd.MultiIndex.from_tuples(df.columns, names=["scenario", "field"])
        return df

    def _to_dataframe(self, columns: str = "user", **kwargs) -> pd.DataFrame:
        """Build DataFrame with specified field for all scenarios."""
        if not isinstance(columns, str) or columns.strip() == "":
            columns = "user"
        return self._build_simple_dataframe(columns)

    def to_dataframe_per_scenario_fields(
        self, fields_map: Dict["Any", List[str]]
    ) -> pd.DataFrame:
        """Build DataFrame where each scenario may have different fields."""
        return self._build_consolidated_dataframe(fields_map)

    def to_dataframe_defaults(self) -> pd.DataFrame:
        """Build DataFrame of default values for each input per scenario."""
        return self._build_simple_dataframe("default")

    def to_dataframe_min_max(self) -> pd.DataFrame:
        """Build DataFrame with min/max bounds (shared across scenarios)."""
        return self._build_bounds_dataframe()

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

    def build_combined_dataframe(
        self, include_defaults: bool = False, include_min_max: bool = False
    ) -> pd.DataFrame:
        """Build DataFrame with various field combinations based on flags."""
        if not self.scenarios:
            return pd.DataFrame()

        # Determine what fields we need
        fields = ["user"]
        if include_defaults:
            fields.append("default")

        if fields == ["user"]:
            return self._build_simple_dataframe("user")
        elif fields == ["default"]:
            return self._build_simple_dataframe("default")
        elif include_min_max and not include_defaults:
            return self._build_user_with_bounds_dataframe()
        elif include_min_max and include_defaults:
            return self._build_full_combined_dataframe()
        else:
            field_map = {scenario: fields for scenario in self.scenarios}
            return self._build_consolidated_dataframe(field_map)

    def _build_full_combined_dataframe(self) -> pd.DataFrame:
        """Build DataFrame with user values, defaults, and min/max bounds."""
        try:
            field_map = {scenario: ["user", "default"] for scenario in self.scenarios}
            df_core = self._build_consolidated_dataframe(field_map)
            df_bounds = self._build_bounds_dataframe()

            if not df_bounds.empty and not df_core.empty:
                return pd.concat([df_bounds, df_core], axis=1)
            elif not df_core.empty:
                return df_core
            else:
                return df_bounds
        except Exception:
            pass

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
