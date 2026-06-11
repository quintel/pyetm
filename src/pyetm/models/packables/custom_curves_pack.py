"""Custom curves packing utilities."""

import logging
from typing import ClassVar, Any, Optional, Sequence, Dict, Set, cast
import pandas as pd
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.packables.packable import Packable
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class CustomCurvesPack(Packable):
    """
    A packable for managing custom curves data.
    """

    key: ClassVar[str] = "custom_curves"
    sheet_name: ClassVar[str] = "CUSTOM_CURVES"
    _sheet_index: ClassVar[Set[str]] = {"curves", "custom_curves", "hour", "index"}

    @staticmethod
    def excel_read_kwargs() -> Dict[str, Any]:
        """
        Returns a dict representing the excel read kwargs like the header
        Availabale to overload for users own implementation
        """
        return {"header": None}

    # TODO: quickly refactor the to_dataframe and build_ ones to use generators, and just keep one!
    def _build_dataframe_for_scenario(self, scenario: Any, columns: str = "", **kwargs: Any) -> Optional[pd.DataFrame]:
        if len(scenario.custom_curves) == 0:
            return pd.DataFrame()
        return cast(pd.DataFrame, pd.concat(scenario.custom_curves_series(), axis=1))

    def _to_dataframe(self, columns: str = "", **kwargs: Any) -> pd.DataFrame:
        return self.build_pack_dataframe(columns=columns, **kwargs).rename_axis("hour")

    def to_dict_per_curve(
        self, curves: Optional[Sequence[str]] = None
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Build dict organized by curve name, then by scenario.
        """
        result: Dict[str, Dict[str, pd.DataFrame]] = {}

        for scenario in self.scenarios:
            try:
                scenario_key = self._key_for(scenario)

                # Determine which curves to fetch
                if curves is not None:
                    # Filter to requested curves that exist
                    curves_to_fetch = [
                        c for c in curves if c in list(scenario.custom_curves.attached_keys())
                    ]
                else:
                    # Fetch all attached curves
                    curves_to_fetch = list(scenario.custom_curves.attached_keys())

                for curve_name in curves_to_fetch:
                    curve_data = scenario.custom_curve_series(curve_name)
                    if curve_data is None or curve_data.empty:
                        continue

                    if curve_name not in result:
                        result[curve_name] = {}
                    # Ensure curve_data is a DataFrame
                    if isinstance(curve_data, pd.Series):
                        df_data = curve_data.to_frame()
                    else:
                        df_data = curve_data
                    result[curve_name][scenario_key] = df_data

                self.log_scenario_warnings(scenario, self.key, self.sheet_name)

            except Exception as e:
                logger.warning(
                    "Failed building curves dict for scenario %s: %s",
                    scenario.identifier(),
                    e,
                )
                continue

        return result

    def load_from_dataframe(self, df: pd.DataFrame, scenario: Any, update_set: Optional[Set[str]] = None) -> None:
        """
        Loads from a dataframe for a single scenario
        """
        normalized_data = excel_utils.normalize_sheet(df, helper_names=self._sheet_index)

        if normalized_data.empty:
            return

        self.apply_custom_curves_to_scenario(scenario, normalized_data, update_set)

    def apply_custom_curves_to_scenario(
        self, scenario: Any, data: pd.DataFrame, update_set: Optional[Set[str]] = None
    ) -> None:
        """Apply custom curves to scenario with validation and error handling."""
        skip_upload = not self._should_include_upload(update_set)

        try:
            curves = CustomCurves._from_dataframe(data, scenario_id=scenario.id)

            # Log processing warnings
            curves.log_warnings(
                logger,
                prefix=f"Custom curves warning for '{scenario.identifier()}'",
            )

            # Validate curves and log validation issues (skip if read-only)
            if not skip_upload:
                self.validate_and_log_curves(curves, scenario)

            # Apply curves to scenario
            scenario.update_custom_curves(curves, skip_upload=skip_upload)

        except Exception as e:
            logger.warning("Failed processing custom curves for '%s': %s", scenario.identifier(), e)

    # TODO: curves should validate themselves on their from_dataframe
    def validate_and_log_curves(self, curves: CustomCurves, scenario: Any) -> None:
        """Validate curves and log any validation issues."""
        try:
            validation_results = curves.validate_for_upload()
            for key, issues in (validation_results or {}).items():
                for issue in issues:
                    logger.warning(
                        "Custom curve validation for '%s' in '%s' [%s]: %s",
                        key,
                        scenario.identifier(),
                        getattr(issue, "field", key),
                        getattr(issue, "message", str(issue)),
                    )
        except Exception:
            # Validation errors are not critical, continue processing
            pass

    def from_dataframe(self, df: pd.DataFrame, update_set: Optional[Set[str]] = None) -> None:
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = self._normalize_single_header_sheet(
                df,
                helper_columns={"curves", "custom_curves"},
                drop_empty=True,
                reset_index=True,
            )
        except Exception as e:
            logger.warning("Failed to normalize custom curves sheet: %s", e)
            return
        if df is None or df.empty:
            return

        def _apply(scenario: Any, block: pd.DataFrame) -> None:
            try:
                curves = CustomCurves._from_dataframe(block, scenario_id=scenario.id)
                curves.log_warnings(
                    logger,
                    prefix=f"Custom curves warning for '{scenario.identifier()}'",
                )
                self.validate_and_log_curves(curves, scenario)
                scenario.update_custom_curves(curves)
            except Exception as e:
                logger.warning(
                    "Failed to build custom curves for '%s': %s",
                    scenario.identifier(),
                    e,
                )

        for scenario in self.scenarios:
            _apply(scenario, df)
