import logging
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
            self.log_scenario_warnings(scenario, "_custom_curves", "Custom curves")
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

    def import_scenario_specific_sheet(
        self, excel_file: pd.ExcelFile, sheet_name: str, scenario: "Any"
    ):
        """Import custom curves from a scenario-specific sheet."""
        df = self.parse_excel_sheet(excel_file, sheet_name, header=None)
        if df is not None and not df.empty:
            self.process_single_scenario_curves(scenario, df)

    def process_single_scenario_curves(self, scenario: "Any", df: pd.DataFrame):
        """Process custom curves data for a single scenario."""
        normalized_data = self.normalize_sheet(
            df,
            helper_names={"curves", "custom_curves", "hour", "index"},
            reset_index=True,
        )

        if normalized_data is None or normalized_data.empty:
            return

        self.apply_custom_curves_to_scenario(scenario, normalized_data)

    def apply_custom_curves_to_scenario(self, scenario: "Any", data: pd.DataFrame):
        """Apply custom curves to scenario with validation and error handling."""
        try:
            curves = CustomCurves._from_dataframe(data, scenario_id=scenario.id)

            # Log processing warnings
            curves.log_warnings(
                logger,
                prefix=f"Custom curves warning for '{scenario.identifier()}'",
            )

            # Validate curves and log validation issues
            self.validate_and_log_curves(curves, scenario)

            # Apply curves to scenario
            scenario.update_custom_curves(curves)

        except Exception as e:
            logger.warning(
                "Failed processing custom curves for '%s': %s", scenario.identifier(), e
            )

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
