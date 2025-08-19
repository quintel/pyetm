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

    async def _build_dataframe_for_scenario(
        self, scenario: Any, columns: str = "", **kwargs
    ):
        try:
            series_list = [series async for series in scenario.custom_curves_series()]
        except Exception as e:
            logger.warning(
                "Failed extracting custom curves for %s: %s", scenario.identifier(), e
            )
            return None
        if not series_list:
            return None
        return pd.concat(series_list, axis=1)

    async def to_dataframe_async(self, columns="", **kwargs) -> pd.DataFrame:
        """Async version that ensures curves are loaded first."""
        frames: list[pd.DataFrame] = []
        keys: list[Any] = []

        for scenario in self.scenarios:
            try:
                if hasattr(scenario, "fetch_custom_curves"):
                    await scenario.fetch_custom_curves()

                df = await self._build_dataframe_for_scenario(
                    scenario, columns=columns, **kwargs
                )
                if df is not None and not df.empty:
                    frames.append(df)
                    keys.append(self._key_for(scenario))

            except Exception as e:
                logger.warning(
                    f"Failed building frame for scenario {scenario.identifier()}: {e}"
                )

        return self._concat_frames(frames, keys)

    def _normalize_curves_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_single_header_sheet(
            df,
            helper_columns={"sortables"},
            drop_empty=True,
            reset_index=True,
        )

    async def from_dataframe(self, df: pd.DataFrame):
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
