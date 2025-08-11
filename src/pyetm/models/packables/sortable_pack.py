import logging
from typing import ClassVar, Any

import pandas as pd

from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class SortablePack(Packable):
    key: ClassVar[str] = "sortables"
    sheet_name: ClassVar[str] = "SORTABLES"

    def _build_dataframe_for_scenario(self, scenario: Any, columns: str = "", **kwargs):  # type: ignore[override]
        try:
            df = scenario.sortables.to_dataframe()
        except Exception as e:  # pragma: no cover
            logger.warning(
                "Failed extracting sortables for %s: %s", scenario.identifier(), e
            )
            return None
        return df if not df.empty else None

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:  # type: ignore[override]
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def _normalize_sortables_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize various sortables sheet shapes using generic helper.
        Produces DataFrame with columns MultiIndex(identifier, sortable_name) then
        collapses to MultiIndex for downstream block iteration.
        """
        data = self._normalize_two_header_sheet(
            df,
            helper_level0=set(),  # no explicit helper labels on level0 expected
            helper_level1={"sortables"},
            drop_empty_level0=True,
            collapse_level0=False,  # keep two levels for clarity; we will collapse in from_dataframe if needed
            reset_index=False,
        )
        if data.empty:
            return data
        # For parity with previous logic we want identifiers as level0 only when applying
        # but we still need a MultiIndex for apply_identifier_blocks; keep as-is.
        return data

    def from_dataframe(self, df: pd.DataFrame):
        """Unpack and update sortables for each scenario from the sheet."""
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = self._normalize_sortables_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize sortables sheet: %s", e)
            return
        if df is None or df.empty or not isinstance(df.columns, pd.MultiIndex):
            return

        def _apply(scenario, block: pd.DataFrame):
            # Collapse block columns (identifier, sortable_name) -> keep only sortable_name part
            if isinstance(block.columns, pd.MultiIndex):
                block.columns = [c[1] for c in block.columns]
            scenario.set_sortables_from_dataframe(block)

        self.apply_identifier_blocks(df, _apply)
