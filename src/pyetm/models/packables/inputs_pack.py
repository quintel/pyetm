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

    def _extract_map(self, scen, attr: str) -> dict[str, Any] | None:
        """Extract a mapping of input key -> attribute value for a scenario's inputs."""
        try:
            it = iter(scen.inputs)
            values = {}
            for inp in it:
                key = getattr(inp, "key", None)
                if key is None:
                    continue
                values[str(key)] = getattr(inp, attr, None)
            if values:
                return values
        except Exception:
            pass

        try:
            df = scen.inputs.to_dataframe(columns=attr)
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
            for candidate in (attr, "user", "value", "default"):
                if candidate in cols_lc:
                    chosen = df.columns[cols_lc.index(candidate)]
                    break
            if chosen is None:
                chosen = df.columns[0]
            series = df[chosen]
        series.index = series.index.map(lambda k: str(k))
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
        """Build a DataFrame with MultiIndex columns (scenario, ['min','max'])."""
        if not self.scenarios:
            return pd.DataFrame()

        all_keys: set[str] = set()
        per_scen: list[tuple[Any, pd.DataFrame]] = []

        for scen in self.scenarios:
            try:
                it = iter(scen.inputs)
                keys = [str(getattr(inp, "key", "")) for inp in it]
            except Exception:
                try:
                    df = scen.inputs.to_dataframe(columns=["min", "max"])
                    keys = [str(i) for i in df.index.get_level_values(0).unique()]
                except Exception:
                    keys = []
            all_keys.update([k for k in keys if k])

        if not all_keys:
            return pd.DataFrame()

        sorted_keys = sorted(all_keys)

        for scen in self.scenarios:
            label = self._key_for(scen)
            min_map: dict[str, Any] = {}
            max_map: dict[str, Any] = {}
            try:
                it = iter(scen.inputs)
                for inp in it:
                    key = str(getattr(inp, "key", ""))
                    if not key:
                        continue
                    min_map[key] = getattr(inp, "min", None)
                    max_map[key] = getattr(inp, "max", None)
            except Exception:
                try:
                    df = scen.inputs.to_dataframe(columns=["min", "max"])
                    if isinstance(df.index, pd.MultiIndex):
                        df = df.copy()
                        df.index = df.index.droplevel("unit")
                    cols_lc = [str(c).lower() for c in df.columns]

                    def _col(name: str):
                        return (
                            df.columns[cols_lc.index(name)]
                            if name in cols_lc
                            else df.columns[0]
                        )

                    try:
                        min_s = df[_col("min")]
                    except Exception:
                        min_s = pd.Series(index=df.index, dtype=float)
                    try:
                        max_s = df[_col("max")]
                    except Exception:
                        max_s = pd.Series(index=df.index, dtype=float)
                    min_map.update({str(k): v for k, v in min_s.items()})
                    max_map.update({str(k): v for k, v in max_s.items()})
                except Exception:
                    pass

            min_vals = [min_map.get(k) for k in sorted_keys]
            max_vals = [max_map.get(k) for k in sorted_keys]
            df_s = pd.DataFrame({"min": min_vals, "max": max_vals}, index=sorted_keys)
            df_s.index.name = "input"
            per_scen.append((label, df_s))

        frames = [df for _, df in per_scen]
        keys = [lbl for lbl, _ in per_scen]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=1, keys=keys, names=["scenario", "stat"])

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
            logger.warning("Failed to parse simplified PARAMETERS sheet: %s", e)
