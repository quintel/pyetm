import logging
from typing import ClassVar, Any
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
    _sheet_index: ClassVar[set] = {"curves", "custom_curves", "hour", "index"}

    @staticmethod
    def excel_read_kwargs():
        """
        Returns a dict representing the excel read kwargs like the header
        Availabale to overload for users own implementation
        """
        return {"header": None}

    # TODO: quickly refactor the to_dataframe and build_ ones to use generators, and just keep one!
    def _build_dataframe_for_scenario(self, scenario: Any, **kwargs) -> pd.DataFrame:
        if len(scenario.custom_curves) == 0:
            return pd.DataFrame()
        return pd.concat(scenario.custom_curves_series(), axis=1)

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        return self.build_pack_dataframe(columns=columns, **kwargs).rename_axis("hour")

    def load_from_dataframe(self, df: pd.DataFrame, scenario: "Any", update_set: set[str] = None):
        """
        Loads from a dataframe for a single scenario
        """
        normalized_data = excel_utils.normalize_sheet(
            df, helper_names=self._sheet_index
        )

        if normalized_data.empty:
            return

        self.apply_custom_curves_to_scenario(scenario, normalized_data, update_set)

    def apply_custom_curves_to_scenario(self, scenario: "Any", data: pd.DataFrame, update_set: set[str] = None):
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
            logger.warning(
                "Failed processing custom curves for '%s': %s", scenario.identifier(), e
            )

    # TODO: curves should validate themselves on their from_dataframe
    def validate_and_log_curves(self, curves: CustomCurves, scenario: "Any"):
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

    def from_dataframe(self, df: pd.DataFrame):
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

        def _apply(scenario, block: pd.DataFrame):
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
