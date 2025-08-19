import logging
import asyncio
from typing import ClassVar, Any
import pandas as pd
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class CustomCurvesPack(Packable):
    key: ClassVar[str] = "custom_curves"
    sheet_name: ClassVar[str] = "CUSTOM_CURVES"

    def _build_dataframe_for_scenario(self, scenario: Any, columns: str = "", **kwargs):
        try:
            series_list = list(scenario.custom_curves_series())
        except Exception as e:
            logger.warning(
                "Failed extracting custom curves for %s: %s", scenario.identifier(), e
            )
            return None
        if not series_list:
            return None
        return pd.concat(series_list, axis=1)

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def _normalize_curves_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_single_header_sheet(
            df,
            helper_columns={"sortables"},
            drop_empty=True,
            reset_index=True,
        )

    # Add async version of from_dataframe
    async def from_dataframe(self, df: pd.DataFrame):
        """Async version of from_dataframe for handling async scenario updates."""
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = self._normalize_curves_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize custom curves sheet: %s", e)
            return
        if df is None or df.empty:
            return

        async def _apply_async(scenario, block: pd.DataFrame):
            try:
                curves = CustomCurves._from_dataframe(block, scenario_id=scenario.id)
                await scenario.update_custom_curves(curves)
            except Exception as e:
                logger.warning(
                    "Failed to build custom curves for '%s': %s",
                    scenario.identifier(),
                    e,
                )

        # Process all scenarios concurrently
        tasks = [_apply_async(scenario, df) for scenario in self.scenarios]
        await asyncio.gather(*tasks, return_exceptions=True)
