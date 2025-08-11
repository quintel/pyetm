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

    # --- Public configuration ---------------------------------------------------
    def set_scenario_short_names(self, scenario_short_names: Dict[str, str]):
        """Set the mapping of scenario identifiers to their short names."""
        self._scenario_short_names = scenario_short_names or {}

    # --- Scenario key / resolution overrides ------------------------------------
    def _key_for(self, scenario: "Any") -> Any:  # type: ignore[override]
        # Prefer short name if present (mapping stored by scenario.id)
        short = self._scenario_short_names.get(str(scenario.id))
        return short if short else scenario.identifier()

    def resolve_scenario(self, label: Any):  # type: ignore[override]
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

    # --- Per-scenario frame builder (used by generic template) ------------------
    def _build_dataframe_for_scenario(self, scenario, columns: str = "user", **kwargs):  # type: ignore[override]
        try:
            df = scenario.inputs.to_dataframe(columns=columns)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(
                "Failed building inputs frame for scenario %s: %s",
                scenario.identifier(),
                e,
            )
            return None
        return df if df is not None and not df.empty else None

    def _to_dataframe(self, columns="user", **kwargs):  # type: ignore[override]
        return self.build_pack_dataframe(columns=columns, **kwargs)

    # --- Normalisation logic ----------------------------------------------------
    def _normalize_inputs_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize various inputs sheet shapes into canonical shape:
        - Drop leading completely blank rows.
        - Accept either:
          (a) Two header rows (short_name row above a row containing 'user'), or
          (b) Single header row of scenario labels (no explicit 'user' row) -> we fabricate 'user'.
        - Support 1- or 2-level row index (input[, unit]).
        Returns DataFrame with columns MultiIndex(label, 'user').
        """
        df = df.dropna(how="all")
        if df.empty:
            return df

        # Locate the row containing 'user' (case-insensitive)
        user_row_pos = None
        for pos, (_, row) in enumerate(df.iterrows()):
            if any(isinstance(v, str) and v.strip().lower() == "user" for v in row):
                user_row_pos = pos
                break

        single_header = user_row_pos is None
        if single_header:
            header_start = 0
            header_end = 0
        else:
            header_start = max(user_row_pos - 1, 0)
            header_end = user_row_pos

        headers = df.iloc[header_start : header_end + 1].astype(str)
        data = df.iloc[header_end + 1 :].copy()

        if single_header:
            # Build columns from single header row; fabricate second level 'user'
            col_level0 = headers.iloc[0].values
            data.columns = col_level0
            index_candidates = list(data.columns[:2])  # heuristic
            second_is_numeric = False
            if len(index_candidates) > 1:
                sample = data[index_candidates[1]].dropna().head(5)
                if not sample.empty and all(
                    pd.to_numeric(sample, errors="coerce").notna()
                ):
                    second_is_numeric = True
            if second_is_numeric:
                idx_cols = [index_candidates[0]]
            else:
                idx_cols = index_candidates
            input_col = idx_cols[0]
            unit_col = idx_cols[1] if len(idx_cols) > 1 else None
            scenario_cols = [
                c for c in data.columns if c not in idx_cols and str(c).strip() != ""
            ]
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

        # Two-row header path (original logic)
        data.columns = pd.MultiIndex.from_arrays(
            [headers.iloc[0].values, headers.iloc[1].values]
        )

        idx_cols = [
            col
            for col in data.columns
            if not (isinstance(col[1], str) and col[1].strip().lower() == "user")
        ]
        if len(idx_cols) == 0:
            input_col = data.columns[0]
            unit_col = None
        else:
            input_col = idx_cols[0]
            unit_col = idx_cols[1] if len(idx_cols) > 1 else None

        input_series = data[input_col].astype(str)
        if unit_col is not None:
            unit_series = data[unit_col].astype(str)
            index = pd.MultiIndex.from_arrays(
                [input_series.values, unit_series.values], names=["input", "unit"]
            )
        else:
            index = pd.Index(input_series.values, name="input")

        keep_cols = [
            c
            for c in data.columns
            if c not in {input_col} and (unit_col is None or c != unit_col)
        ]
        canonical = data[keep_cols]
        canonical.index = index

        if isinstance(canonical.columns, pd.MultiIndex):
            lvl1 = canonical.columns.get_level_values(1)
            if not all(
                isinstance(v, str) and v.strip().lower() == "user" for v in lvl1
            ):
                canonical.columns = pd.MultiIndex.from_arrays(
                    [
                        canonical.columns.get_level_values(0),
                        ["user"] * len(canonical.columns),
                    ]
                )
        else:
            canonical.columns = pd.MultiIndex.from_arrays(
                [canonical.columns, ["user"] * len(canonical.columns)]
            )
        return canonical

    # --- Import (mutation) ------------------------------------------------------
    def from_dataframe(self, df):  # type: ignore[override]
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
