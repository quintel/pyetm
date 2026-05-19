"""Annual exports packing utilities."""

import logging
from typing import ClassVar, Any, Optional, Sequence
import pandas as pd
from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class AnnualExportsPack(Packable):
    """
    A packable for managing annual exports across scenarios.

    Each export type becomes a separate worksheet in Excel exports.
    """

    key: ClassVar[str] = "annual_exports"
    sheet_name: ClassVar[str] = "ANNUAL_EXPORTS"

    def _build_dataframe_for_scenario(
        self,
        scenario: Any,
        columns: str = "",
        exports: Optional[Sequence[str]] = None,
        **kwargs,
    ):
        """
        Build a DataFrame for one scenario by delegating to the model's to_dataframe() method.

        Returns a DataFrame with export_name as part of the multi-index.
        If no exports specified, returns None (opt-in only).
        """
        try:
            # If not specified, don't fetch any (opt-in only)
            if exports is None:
                return None

            # Fetch the requested exports from the API
            if hasattr(scenario, 'get_annual_export') and exports:
                try:
                    for export_name in exports:
                        try:
                            scenario.get_annual_export(export_name)
                        except Exception as e:
                            logger.warning(
                                "Failed to fetch export %s for scenario %s: %s",
                                export_name,
                                scenario.identifier(),
                                e,
                            )
                except (TypeError, AttributeError):
                    # Handle case where exports isn't iterable
                    pass

            # Delegate to the model's to_dataframe() method
            df = scenario.annual_exports.to_dataframe(exports=exports, **kwargs)

            self.log_scenario_warnings(scenario, "annual_exports", "Annual exports")

            return df if not df.empty else None

        except Exception as e:
            logger.warning(
                "Failed extracting annual exports for %s: %s",
                scenario.identifier(),
                e,
            )
            return None

    def to_dict_per_export(
        self, exports: Optional[Sequence[str]] = None
    ) -> dict[str, dict[str, pd.DataFrame]]:
        """
        Build a dict organized by export type, then by scenario.

        Returns dict[export_name][scenario_id] = DataFrame for that export and scenario.
        """
        result = {}

        for scenario in self.scenarios:
            try:
                scenario_key = self._key_for(scenario)

                # Fetch the requested exports from the API
                if hasattr(scenario, 'get_annual_export') and exports:
                    try:
                        for export_name in exports:
                            try:
                                scenario.get_annual_export(export_name)
                            except Exception as e:
                                logger.warning(
                                    "Failed to fetch export %s for scenario %s: %s",
                                    export_name,
                                    scenario.identifier(),
                                    e,
                                )
                    except (TypeError, AttributeError):
                        # Handle case where exports isn't iterable
                        pass

                # Get multi-index dataframe from model
                df = scenario.annual_exports.to_dataframe(exports=exports)

                if df is None or df.empty:
                    continue

                for export_name in df.index.get_level_values("export_name").unique():
                    export_df = df.xs(export_name, level="export_name")

                    if export_name not in result:
                        result[export_name] = {}
                    result[export_name][scenario_key] = export_df

            except Exception as e:
                logger.warning(
                    "Failed building exports for scenario %s: %s",
                    scenario.identifier(),
                    e,
                )
                continue

        return result

    def to_excel(self, path: str, exports: Optional[Sequence[str]] = None) -> None:
        """
        Export annual exports to Excel file.

        Each export type becomes a separate worksheet.
        Within each worksheet, scenarios are concatenated with a scenario identifier column.
        """
        from xlsxwriter import Workbook
        from pyetm.utils import excel_utils

        if not self.scenarios:
            logger.info("No scenarios to export")
            return

        # Get data organized by export type
        export_dict = self.to_dict_per_export(exports=exports)

        if not export_dict:
            logger.info("No export data available")
            return

        workbook = None
        try:
            workbook = Workbook(str(path))

            # Create a worksheet for each export type
            for export_name, scenarios_data in sorted(export_dict.items()):
                if not scenarios_data:
                    continue

                # Combine all scenarios for this export into one DataFrame
                frames = []
                for scenario_key, df in scenarios_data.items():
                    # Add scenario identifier column
                    df_copy = df.copy()
                    df_copy.insert(0, "scenario", scenario_key)
                    frames.append(df_copy)

                if frames:
                    combined = pd.concat(frames, ignore_index=True)

                    # Create worksheet with export name (truncate if too long)
                    sheet_name = export_name.upper()[:31]  # Excel limit

                    excel_utils.add_frame(
                        name=sheet_name,
                        frame=combined,
                        workbook=workbook,
                        column_width=18,
                        scenario_styling=False,
                    )

        finally:
            if workbook is not None:
                workbook.close()
