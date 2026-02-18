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
        Build a DataFrame for one scenario with annual export data.

        For annual exports, we return a dict of {export_name: DataFrame}
        """
        try:
            export_data = {}

            # Determine which exports to fetch
            if exports is not None:
                exports_to_fetch = list(exports)
            else:
                # If not specified, don't fetch any (opt-in only)
                return None

            for export_name in exports_to_fetch:
                df = scenario.export(export_name)
                if df is not None and not df.empty:
                    export_data[export_name] = df

            self.log_scenario_warnings(scenario, "_annual_exports", "Annual exports")

            return export_data if export_data else None

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
        """
        result = {}

        for scenario in self.scenarios:
            try:
                scenario_data = self._build_dataframe_for_scenario(
                    scenario, exports=exports
                )
                if not scenario_data:
                    continue

                scenario_key = self._key_for(scenario)

                for export_name, df in scenario_data.items():
                    if export_name not in result:
                        result[export_name] = {}
                    result[export_name][scenario_key] = df

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
