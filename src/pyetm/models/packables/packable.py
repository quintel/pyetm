from typing import ClassVar, Set, Callable, Optional, Dict, Any
import logging
import pandas as pd
from pydantic import BaseModel, Field

from pyetm.models.scenario import Scenario

logger = logging.getLogger(__name__)


class Packable(BaseModel):
    # Use a proper default set and keep the type consistent
    scenarios: Set["Scenario"] = Field(default_factory=set)
    key: ClassVar[str] = "base_pack"
    sheet_name: ClassVar[str] = "SHEET"

    # Internal cache for fast identifier lookup
    _scenario_id_cache: Dict[str, "Scenario"] | None = None

    # --- Public collection API -------------------------------------------------
    def add(self, *scenarios):
        "Adds one or more scenarios to the packable"
        if not scenarios:
            return
        self.scenarios.update(scenarios)
        # Invalidate cache
        self._scenario_id_cache = None

    def discard(self, scenario):
        "Removes a scenario from the pack"
        self.scenarios.discard(scenario)
        self._scenario_id_cache = None

    def clear(self):
        # Reset to an empty set
        self.scenarios.clear()
        self._scenario_id_cache = None

    # --- Summary ----------------------------------------------------------------
    def summary(self) -> dict:
        return {self.key: {"scenario_count": len(self.scenarios)}}

    # --- Template packing API (Opt-in for subclasses) ---------------------------
    def _key_for(self, scenario: "Scenario") -> Any:
        """Return the identifier used as the top-level column key when packing.
        Subclasses can override (e.g. to use short names)."""
        return scenario.identifier()

    def _build_dataframe_for_scenario(
        self, scenario: "Scenario", columns: str = "", **kwargs
    ) -> Optional[pd.DataFrame]:
        """Return a DataFrame for a single scenario or None/empty if not applicable.
        Subclasses may override to opt-in to generic build_pack_dataframe helper."""
        return None

    def _concat_frames(
        self, frames: list[pd.DataFrame], keys: list[Any]
    ) -> pd.DataFrame:
        """Concatenate per-scenario frames along axis=1 with keys.
        Separated for easier overriding/testing."""
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=1, keys=keys)

    def build_pack_dataframe(self, columns: str = "", **kwargs) -> pd.DataFrame:
        """Generic implementation collecting per-scenario frames using
        _build_dataframe_for_scenario. Subclasses call this inside their
        _to_dataframe implementation after overriding _build_dataframe_for_scenario.
        (Not automatically invoked so existing subclasses remain unchanged)."""
        frames: list[pd.DataFrame] = []
        keys: list[Any] = []
        for scenario in self.scenarios:
            try:
                df = self._build_dataframe_for_scenario(
                    scenario, columns=columns, **kwargs
                )
            except Exception as e:
                logger.warning(
                    "Failed building frame for scenario %s in %s: %s",
                    scenario.identifier(),
                    self.__class__.__name__,
                    e,
                )
                continue
            if df is None or df.empty:
                continue
            frames.append(df)
            keys.append(self._key_for(scenario))
        return self._concat_frames(frames, keys)

    # --- External API -----------------------------------------------------------
    def to_dataframe(self, columns="") -> pd.DataFrame:
        """Convert the pack into a dataframe"""
        if len(self.scenarios) == 0:
            return pd.DataFrame()
        return self._to_dataframe(columns=columns)

    def from_dataframe(self, df):
        """Should parse the df and call correct setters on identified scenarios"""

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """Base implementation - kids should implement this or use build_pack_dataframe"""
        return pd.DataFrame()

    # --- Scenario resolution helpers -------------------------------------------
    def _refresh_cache(self):
        self._scenario_id_cache = {str(s.identifier()): s for s in self.scenarios}

    def _find_by_identifier(self, identifier: str):
        ident_str = str(identifier)
        if self._scenario_id_cache is None or len(self._scenario_id_cache) != len(
            self.scenarios
        ):
            self._refresh_cache()
        return self._scenario_id_cache.get(ident_str)

    def resolve_scenario(self, label: Any) -> Optional["Scenario"]:
        """Generic resolution; subclasses can extend (e.g. InputsPack).
        Default: direct identifier match."""
        if label is None:
            return None
        return self._find_by_identifier(label)

    # --- Utility static methods (shared normalisation helpers) -----------------
    @staticmethod
    def is_blank(value: Any) -> bool:
        return (
            value is None
            or (isinstance(value, float) and pd.isna(value))
            or (isinstance(value, str) and value.strip() == "")
        )

    @staticmethod
    def drop_all_blank(df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()
        return df.dropna(how="all")

    @staticmethod
    def first_non_empty_row_positions(df: pd.DataFrame, count: int = 2) -> list[int]:
        positions: list[int] = []
        if df is None:
            return positions
        for idx, (_, row) in enumerate(df.iterrows()):
            if not row.isna().all():
                positions.append(idx)
                if len(positions) >= count:
                    break
        return positions

    def _log_fail(self, context: str, exc: Exception):
        logger.warning("%s failed in %s: %s", context, self.__class__.__name__, exc)

    def apply_identifier_blocks(
        self,
        df: pd.DataFrame,
        apply_block: Callable[["Scenario", pd.DataFrame], None],
        resolve: Optional[Callable[[Any], Optional["Scenario"]]] = None,
    ):
        """Iterate over first-level column identifiers of a MultiIndex DataFrame and apply a block function.
        resolve optionally overrides scenario resolution (defaults to direct identifier lookup).
        """
        if df is None or not isinstance(df.columns, pd.MultiIndex):
            return
        identifiers = df.columns.get_level_values(0).unique()
        for identifier in identifiers:
            scenario = (
                resolve(identifier) if resolve else None
            ) or self._find_by_identifier(identifier)
            if scenario is None:
                logger.warning(
                    "Could not find scenario for identifier '%s' in %s",
                    identifier,
                    self.__class__.__name__,
                )
                continue
            block = df[identifier]
            try:
                apply_block(scenario, block)
            except Exception as e:
                logger.warning(
                    "Failed applying block for scenario '%s' in %s: %s",
                    identifier,
                    self.__class__.__name__,
                    e,
                )

    def _normalize_two_header_sheet(
        self,
        df: pd.DataFrame,
        *,
        helper_level0: Optional[set[str]] = None,
        helper_level1: Optional[set[str]] = None,
        drop_empty_level0: bool = True,
        drop_empty_level1: bool = False,
        collapse_level0: bool = False,
        reset_index: bool = False,
    ) -> pd.DataFrame:
        """Generic normalizer for a sheet with (potential) two header rows.
        - Detect first two non-empty rows as headers (or fabricate second if missing).
        - Build MultiIndex columns (level0, level1).
        - Optionally drop columns whose level0/level1 are blank or in helper sets.
        - Optionally collapse to single level (level0) after filtering.
        - Optionally reset row index to a simple RangeIndex.
        Returns canonical DataFrame or empty DataFrame on failure.
        """
        helper_level0 = {h.lower() for h in (helper_level0 or set())}
        helper_level1 = {h.lower() for h in (helper_level1 or set())}

        if df is None:
            return pd.DataFrame()
        df = df.dropna(how="all")
        if df.empty:
            return df

        positions = self.first_non_empty_row_positions(df, 2)
        if not positions:
            return pd.DataFrame()
        header0_pos = positions[0]
        header1_pos = positions[1] if len(positions) > 1 else None

        if header1_pos is None:
            # Single header row -> fabricate second empty row
            headers0 = df.iloc[header0_pos].astype(str).values
            headers1 = ["" for _ in headers0]
            data = df.iloc[header0_pos + 1 :].copy()
        else:
            headers = df.iloc[[header0_pos, header1_pos]].astype(str)
            headers0 = headers.iloc[0].values
            headers1 = headers.iloc[1].values
            data = df.iloc[header1_pos + 1 :].copy()

        columns = pd.MultiIndex.from_arrays([headers0, headers1])
        data.columns = columns

        def _is_blank(v):
            return (
                v is None
                or (isinstance(v, float) and pd.isna(v))
                or (isinstance(v, str) and v.strip() == "")
            )

        keep = []
        for c in data.columns:
            lv0, lv1 = c[0], c[1]
            if drop_empty_level0 and _is_blank(lv0):
                continue
            if drop_empty_level1 and _is_blank(lv1):
                continue
            if isinstance(lv0, str) and lv0.strip().lower() in helper_level0:
                continue
            if isinstance(lv1, str) and lv1.strip().lower() in helper_level1:
                continue
            keep.append(c)
        data = data[keep]

        if collapse_level0:
            data.columns = [c[0] for c in data.columns]
        if reset_index:
            data.reset_index(drop=True, inplace=True)
        return data
