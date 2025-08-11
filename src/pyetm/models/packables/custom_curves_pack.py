import logging
from typing import ClassVar, Any

import pandas as pd

from pyetm.models.custom_curves import CustomCurves
from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class CustomCurvesPack(Packable):
    key: ClassVar[str] = "custom_curves"
    sheet_name: ClassVar[str] = "CUSTOM_CURVES"

    def _build_dataframe_for_scenario(self, scenario: Any, columns: str = "", **kwargs):  # type: ignore[override]
        try:
            series_list = list(scenario.custom_curves_series())
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(
                "Failed extracting custom curves for %s: %s", scenario.identifier(), e
            )
            return None
        if not series_list:
            return None
        return pd.concat(series_list, axis=1)

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:  # type: ignore[override]
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def _normalize_curves_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        data = self._normalize_two_header_sheet(
            df,
            helper_level1={"sortables"},
            drop_empty_level0=True,
            collapse_level0=False,
            reset_index=True,  # values rows should be reindexed 0..n
        )
        return data

    def from_dataframe(self, df: pd.DataFrame):  # type: ignore[override]
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = self._normalize_curves_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize custom curves sheet: %s", e)
            return
        if df is None or df.empty or not isinstance(df.columns, pd.MultiIndex):
            return

        def _apply(scenario, block: pd.DataFrame):
            # block has columns MultiIndex(identifier, curve_key); collapse to curve_key
            if isinstance(block.columns, pd.MultiIndex):
                block.columns = [c[1] for c in block.columns]
            try:
                curves = CustomCurves._from_dataframe(block, scenario_id=scenario.id)
            except Exception as e:  # pragma: no cover
                logger.warning(
                    "Failed to build custom curves for '%s': %s",
                    scenario.identifier(),
                    e,
                )
                return
            scenario.update_custom_curves(curves)

        self.apply_identifier_blocks(df, _apply)
