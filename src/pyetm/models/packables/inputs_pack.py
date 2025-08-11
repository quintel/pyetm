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
        self._scenario_short_names: Dict[str, str] = (
            {}
        )  # scenario_id -> short_name mapping

    def set_scenario_short_names(self, scenario_short_names: Dict[str, str]):
        self._scenario_short_names = scenario_short_names or {}

    def _key_for(self, scenario: "Any") -> Any:
        # Prefer short name if present (mapping stored by scenario.id)
        short = self._scenario_short_names.get(str(scenario.id))
        return short if short else scenario.identifier()

    def resolve_scenario(self, label: Any):
        if label is None:
            return None
        label_str = str(label).strip()
        # 1. short name
        for scenario in self.scenarios:
            if self._scenario_short_names.get(str(scenario.id)) == label_str:
                return scenario
        # 2. identifier (title or id as string)
        s = super().resolve_scenario(label_str)
        if s is not None:
            return s
        # 3. numeric id
        try:
            num = int(float(label_str))
            for scenario in self.scenarios:
                if scenario.id == num:
                    return scenario
        except Exception:
            pass
        return None

    def _build_dataframe_for_scenario(self, scenario, columns: str = "user", **kwargs):
        try:
            df = scenario.inputs.to_dataframe(columns=columns)
        except Exception as e:
            logger.warning(
                "Failed building inputs frame for scenario %s: %s",
                scenario.identifier(),
                e,
            )
            return None
        return df if df is not None and not df.empty else None

    def _to_dataframe(self, columns="user", **kwargs):
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def _normalize_inputs_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize PARAMETERS sheet (single-header variant only).
        Assumptions (new layout):
        - First non-empty row contains scenario labels (short names / identifiers).
        - First column below header lists input keys; optional second column lists units
          (detected heuristically: treat second column as unit column if most
          of its non-empty values are short (<=8 chars) and non-numeric while
          third column has numeric values).
        - All remaining columns are scenario value columns.
        - Fabricate second header level with constant 'user'.
        Returns DataFrame with MultiIndex columns (scenario_label, 'user').
        """
        df = df.dropna(how="all")
        if df.empty:
            return df

        # Determine header row (first non-empty)
        header_pos_list = self.first_non_empty_row_positions(df, 1)
        if not header_pos_list:
            return pd.DataFrame()
        header_pos = header_pos_list[0]
        header = df.iloc[header_pos].astype(str)
        data = df.iloc[header_pos + 1 :].copy()
        data.columns = header.values

        if data.empty:
            return pd.DataFrame()

        cols = list(data.columns)
        if not cols:
            return pd.DataFrame()

        # Heuristic to detect unit column
        input_col = cols[0]
        unit_col = None
        if len(cols) > 2:
            candidate_unit = cols[1]
            third_col = cols[2]
            sample_candidate = data[candidate_unit].dropna().astype(str).head(25)
            sample_third = data[third_col].dropna().head(25)
            if not sample_candidate.empty:
                short_tokens = sum(len(s.strip()) <= 8 for s in sample_candidate)
                numeric_third = (
                    not sample_third.empty
                    and pd.to_numeric(sample_third, errors="coerce").notna().mean()
                    > 0.5
                )
                if short_tokens / len(sample_candidate) > 0.6 and numeric_third:
                    unit_col = candidate_unit
        elif len(cols) == 2:
            # If only two columns treat second as scenario column (no unit)
            unit_col = None

        scenario_cols = [c for c in cols if c not in {input_col} and c != unit_col]
        input_series = data[input_col].astype(str)
        if unit_col is not None:
            unit_series = data[unit_col].astype(str)
            index = pd.MultiIndex.from_arrays(
                [input_series.values, unit_series.values], names=["input", "unit"]
            )
        else:
            index = pd.Index(input_series.values, name="input")
        canonical = data[scenario_cols].copy()
        canonical.index = index
        canonical.columns = pd.MultiIndex.from_arrays(
            [canonical.columns, ["user"] * len(canonical.columns)]
        )
        return canonical

    # --- Import (mutation) ------------------------------------------------------
    def from_dataframe(self, df):
        """
        Sets the inputs on the scenarios from the packed df (comes from excel)
        Tolerates optional unit column and leading blank rows.
        Uses short_name for scenario identification; falls back to identifier/title or id.
        """
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = self._normalize_inputs_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize inputs sheet: %s", e)
            return
        if df is None or df.empty:
            return

        labels = df.columns.get_level_values(0).unique()
        for label in labels:
            scenario = self.resolve_scenario(label)
            if scenario is None:
                logger.warning(
                    "Could not find scenario for parameters column label '%s' (not a short_name/title/id)",
                    label,
                )
                continue
            scenario_df = df[label]
            if isinstance(scenario_df, pd.Series):
                scenario_df = scenario_df.to_frame(name="user")
            else:
                if list(scenario_df.columns) != ["user"]:
                    scenario_df = scenario_df.copy()
                    first_col = scenario_df.columns[0]
                    scenario_df = scenario_df.rename(columns={first_col: "user"})
            try:
                scenario.set_user_values_from_dataframe(scenario_df)
            except Exception as e:
                logger.warning(
                    "Failed setting inputs for scenario '%s' from column label '%s': %s",
                    scenario.identifier(),
                    label,
                    e,
                )
