import logging
from typing import ClassVar, Dict, Any
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

    def _extract_map(self, scen, attr: str) -> dict[str, Any] | None:
        """Extract a mapping of input key -> attribute value for a scenario's inputs.

        Special attr 'min_max' resolves to 'min' (fallback to 'max').
        """
        iter_attr_primary = attr
        iter_attr_fallback = None
        if attr == "min_max":
            iter_attr_primary = "min"
            iter_attr_fallback = "max"

        # 1) Try iterating input objects first
        try:
            values: dict[str, Any] = {}
            for inp in scen.inputs:
                key = getattr(inp, "key", None)
                if key is None:
                    continue
                val = getattr(inp, iter_attr_primary, None)
                if val is None and iter_attr_fallback is not None:
                    val = getattr(inp, iter_attr_fallback, None)
                values[str(key)] = val if attr != "min_max" else val
            if values:
                return values
        except Exception:
            pass

        # 2) Fallback to DataFrame/Series
        try:
            df = (
                scen.inputs.to_dataframe(columns=["min", "max"])
                if attr == "min_max"
                else scen.inputs.to_dataframe(columns=attr)
            )
        except Exception:
            df = None

        if df is None or getattr(df, "empty", False):
            return None

        # Normalize index (drop 'unit' if present)
        if isinstance(df.index, pd.MultiIndex) and "unit" in (df.index.names or []):
            df = df.copy()
            df.index = df.index.droplevel("unit")

        if isinstance(df, pd.Series):
            series = df
        else:
            cols_lc = {str(c).lower(): c for c in df.columns}
            if attr == "min_max":
                if "min" in cols_lc:
                    series = df[cols_lc["min"]]
                elif "max" in cols_lc:
                    series = df[cols_lc["max"]]
                else:
                    series = df.iloc[:, 0]
            else:
                for candidate in (attr, "user", "value", "default"):
                    if candidate in cols_lc:
                        series = df[cols_lc[candidate]]
                        break
                else:
                    series = df.iloc[:, 0]

        series.index = series.index.map(str)
        return series.to_dict()

    def _to_dataframe(self, columns: str = "user", **kwargs):
        if not self.scenarios:
            return pd.DataFrame()
        if not isinstance(columns, str) or columns.strip() == "":
            columns = "user"

        all_keys: set[str] = set()
        per_label_values: dict[Any, dict[str, Any]] = {}
        for scen in self.scenarios:
            label = self._key_for(scen)
            user_map = self._extract_map(scen, columns)
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

    def to_dataframe_per_scenario_fields(
        self, fields_map: Dict["Any", list[str]]
    ) -> pd.DataFrame:
        """Build a DataFrame where each scenario may have different fields (e.g. ['user'], ['user','default'], ['user','min','max'])."""
        if not self.scenarios:
            return pd.DataFrame()

        scen_set = {s for s in self.scenarios if s in fields_map}
        if not scen_set:
            return pd.DataFrame()

        all_keys: set[str] = set()
        for s in scen_set:
            fields = fields_map.get(s, ["user"]) or ["user"]
            for f in fields:
                m = self._extract_map(s, f)
                if m:
                    all_keys.update(m.keys())

        if not all_keys:
            return pd.DataFrame()
        sorted_keys = sorted(all_keys)

        frames: list[pd.DataFrame] = []
        keys: list[Any] = []
        for s in scen_set:
            label = self._key_for(s)
            fields = fields_map.get(s, ["user"]) or ["user"]
            data: dict[str, list[Any]] = {}
            for f in fields:
                m = self._extract_map(s, f) or {}
                data[f] = [m.get(k) for k in sorted_keys]
            if not data:
                continue
            df_s = pd.DataFrame(data, index=sorted_keys)
            df_s.index.name = "input"
            frames.append(df_s)
            keys.append(label)

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, axis=1, keys=keys, names=["scenario", "field"])

    def to_dataframe_defaults(self) -> pd.DataFrame:
        """Build a DataFrame of default values for each input per scenario."""
        if not self.scenarios:
            return pd.DataFrame()

        return self._to_dataframe(columns="default")

    def to_dataframe_min_max(self) -> pd.DataFrame:
        """Build a DataFrame with min/max once (shared across scenarios).

        Assumes bounds (min/max) are identical across scenarios. If conflicts are
        found, the first-seen value is used and no exception is raised.
        """
        if not self.scenarios:
            return pd.DataFrame()

        # Collect union of keys across scenarios
        all_keys: set[str] = set()
        for scen in self.scenarios:
            try:
                it = iter(scen.inputs)
                keys = [str(getattr(inp, "key", "")) for inp in it]
            except Exception:
                try:
                    df = scen.inputs.to_dataframe(columns=["min", "max"])
                    # Drop 'unit' level if present
                    if isinstance(df.index, pd.MultiIndex) and "unit" in (
                        df.index.names or []
                    ):
                        df = df.copy()
                        df.index = df.index.droplevel("unit")
                    keys = [str(i) for i in df.index.unique()]
                except Exception:
                    keys = []
            all_keys.update([k for k in keys if k])

        if not all_keys:
            return pd.DataFrame()

        sorted_keys = sorted(all_keys)

        # Choose bounds from the first scenario that provides them; ignore conflicts
        min_vals: dict[str, Any] = {}
        max_vals: dict[str, Any] = {}
        for scen in self.scenarios:
            m_min = self._extract_map(scen, "min") or {}
            m_max = self._extract_map(scen, "max") or {}
            for k in sorted_keys:
                if k not in min_vals and k in m_min:
                    min_vals[k] = m_min.get(k)
                if k not in max_vals and k in m_max:
                    max_vals[k] = m_max.get(k)
            if len(min_vals) == len(sorted_keys) and len(max_vals) == len(sorted_keys):
                break

        data = {
            ("", "min"): [min_vals.get(k) for k in sorted_keys],
            ("", "max"): [max_vals.get(k) for k in sorted_keys],
        }
        df = pd.DataFrame(data, index=sorted_keys)
        df.index.name = "input"
        df.columns = pd.MultiIndex.from_tuples(df.columns, names=["scenario", "field"])
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
                        "Could not find scenario for SLIDER_SETTINGS column label '%s'",
                        col,
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
                finally:
                    try:
                        if (
                            hasattr(scenario, "_inputs")
                            and scenario._inputs is not None
                        ):
                            scenario._inputs.log_warnings(
                                logger,
                                prefix=f"Inputs warning for '{scenario.identifier()}'",
                            )
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("Failed to parse simplified SLIDER_SETTINGS sheet: %s", e)
