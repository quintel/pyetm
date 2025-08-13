import logging
from typing import ClassVar, Any
import pandas as pd
from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class SortablePack(Packable):
    key: ClassVar[str] = "sortables"
    sheet_name: ClassVar[str] = "SORTABLES"

    def _build_dataframe_for_scenario(self, scenario: Any, columns: str = "", **kwargs):
        try:
            df = scenario.sortables.to_dataframe()
        except Exception as e:
            logger.warning(
                "Failed extracting sortables for %s: %s", scenario.identifier(), e
            )
            return None
        return df if not df.empty else None

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def _normalize_sortables_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize a sortables sheet expecting a single header row."""
        return self._normalize_single_header_sheet(
            df,
            helper_columns={"sortables"},
            drop_empty=True,
            reset_index=False,
        )

    def from_dataframe(self, df: pd.DataFrame):
        """Unpack and update sortables for each scenario from the sheet."""
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = self._normalize_sortables_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize sortables sheet: %s", e)
            return
        if df is None or df.empty:
            return

        def _apply(scenario, block: pd.DataFrame):
            scenario.set_sortables_from_dataframe(block)

        if isinstance(df.columns, pd.MultiIndex):
            self.apply_identifier_blocks(df, _apply)
        else:
            for scenario in self.scenarios:
                _apply(scenario, df)
