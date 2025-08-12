import logging
from typing import ClassVar, Dict, Any
import pandas as pd
from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class InputsPack(Packable):
    key: ClassVar[str] = "inputs"
    sheet_name: ClassVar[str] = "PARAMETERS"

    def __init__(self, **data):
        super().__init__(**data)
        self._scenario_short_names: Dict[str, str] = {}

    def set_scenario_short_names(self, scenario_short_names: Dict[str, str]):
        self._scenario_short_names = scenario_short_names or {}

    def _key_for(self, scenario: "Any") -> Any:
        short = self._scenario_short_names.get(str(scenario.id))
        if short:
            return short

        label = None
        try:
            label = scenario.identifier()
        except Exception:
            label = None

        if not isinstance(label, (str, int)):
            return scenario.id
        return label

    def resolve_scenario(self, label: Any):
        if label is None:
            return None
        label_str = str(label).strip()
        # Match short name first
        for scenario in self.scenarios:
            if self._scenario_short_names.get(str(scenario.id)) == label_str:
                return scenario
        # Fallback to identifier/title
        found = super().resolve_scenario(label_str)
        if found is not None:
            return found
        # Fallback to numeric id
        try:
            num = int(float(label_str))
            for scenario in self.scenarios:
                if scenario.id == num:
                    return scenario
        except Exception:
            pass
        return None

    def _to_dataframe(self, columns: str = "user", **kwargs):
        if not self.scenarios:
            return pd.DataFrame()

        def extract_user_map_from_inputs_obj(scen) -> dict[str, Any] | None:
            try:
                it = iter(scen.inputs)
                values = {}
                for inp in it:
                    key = getattr(inp, "key", None)
                    if key is None:
                        continue
                    values[str(key)] = getattr(inp, "user", None)
                if values:
                    return values
            except Exception:
                pass

            try:
                df = scen.inputs.to_dataframe(columns="user")
            except Exception:
                df = None
            if df is None or getattr(df, "empty", False):
                return None

            if isinstance(df.index, pd.MultiIndex) and "unit" in (df.index.names or []):
                df = df.copy()
                df.index = df.index.droplevel("unit")
            if isinstance(df, pd.Series):
                series = df
            else:
                cols_lc = [str(c).lower() for c in df.columns]
                chosen = None
                for candidate in ("user", "value"):
                    if candidate in cols_lc:
                        chosen = df.columns[cols_lc.index(candidate)]
                        break
                if chosen is None:
                    chosen = df.columns[0]
                series = df[chosen]
            series.index = series.index.map(lambda k: str(k))
            return series.to_dict()

        all_keys: set[str] = set()
        per_label_values: dict[Any, dict[str, Any]] = {}
        for scen in self.scenarios:
            label = self._key_for(scen)
            user_map = extract_user_map_from_inputs_obj(scen)
            if not user_map:
                continue
            per_label_values[label] = user_map
            all_keys.update(user_map.keys())

        if not all_keys:
            return pd.DataFrame()

        sorted_keys = sorted(all_keys)
        data: dict[Any, list[Any]] = {}
        for label, value_map in per_label_values.items():
            data[label] = [value_map.get(k) for k in sorted_keys]

        df = pd.DataFrame(data, index=sorted_keys)
        df.index.name = "input"
        return df

    def from_dataframe(self, df):
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = df.dropna(how="all")
            if df.empty:
                return
            header_pos_list = self.first_non_empty_row_positions(df, 1)
            if not header_pos_list:
                return
            header_pos = header_pos_list[0]
            header_row = df.iloc[header_pos].astype(str)
            data = df.iloc[header_pos + 1 :].copy()
            data.columns = header_row.values
            if data.empty or len(data.columns) < 2:
                return
            input_col = data.columns[0]
            inputs_series = data[input_col].astype(str).str.strip()
            mask = inputs_series != ""
            data = data.loc[mask]
            inputs_series = inputs_series.loc[mask]
            data.index = inputs_series
            scenario_cols = [c for c in data.columns if c != input_col]
            for col in scenario_cols:
                scenario = self.resolve_scenario(col)
                if scenario is None:
                    logger.warning(
                        "Could not find scenario for PARAMETERS column label '%s'", col
                    )
                    continue
                series = data[col]

                def _is_blank_val(v: Any) -> bool:
                    if v is None:
                        return True
                    if isinstance(v, float) and pd.isna(v):
                        return True
                    if isinstance(v, str) and v.strip().lower() in {"", "nan"}:
                        return True
                    return False

                updates = {k: v for k, v in series.items() if not _is_blank_val(v)}
                if not updates:
                    continue
                try:
                    scenario.update_user_values(updates)
                except Exception as e:
                    logger.warning(
                        "Failed updating inputs for scenario '%s' from column '%s': %s",
                        scenario.identifier(),
                        col,
                        e,
                    )
        except Exception as e:
            logger.warning("Failed to parse simplified PARAMETERS sheet: %s", e)
