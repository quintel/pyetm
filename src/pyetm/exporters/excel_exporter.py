"""Excel export functionality for scenario data."""

import logging
from pathlib import Path
from typing import Optional, Sequence, Dict, TYPE_CHECKING
import pandas as pd
from xlsxwriter import Workbook

from pyetm.models.export_data_collection import ExportDataCollection
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyetm.models.scenario_packer import ScenarioPacker


class ExcelExporter:
    """Handles Excel export for scenario data."""

    @staticmethod
    def write(
        export_data: ExportDataCollection,
        path: str,
        packer: "ScenarioPacker",
        include_input_details: Optional[bool] = None,
    ) -> Path:
        """
        Write export data collection to Excel format.

        Args:
            export_data: The collected export data in generic format
            path: Output file path for the main Excel file
            packer: ScenarioPacker instance (needed for pack Excel formatting methods)
            include_input_details: Add detailed input sheet (Excel-specific feature)

        Returns:
            Path to the created main Excel file
        """
        # Ensure destination directory exists
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        # Create and populate main workbook
        workbook = Workbook(path)
        try:
            # Write main info sheet
            ExcelExporter._write_main_info(
                workbook, export_data.main_info, list(packer._scenarios())
            )

            # Write data sheets based on what's in the collection
            if export_data.inputs is not None:
                packer._inputs.add_to_workbook(workbook)

            if export_data.sortables is not None:
                packer._sortables.add_to_workbook(workbook)

            if export_data.custom_curves is not None:
                packer._custom_curves.add_to_workbook(workbook)

            if export_data.gquery_results is not None:
                packer._query_pack.add_to_workbook(workbook)

            if export_data.users is not None:
                packer._users.add_to_workbook(workbook)

            # Excel-specific: Add detailed input sheet if requested
            if include_input_details:
                packer._inputs.add_to_workbook(
                    workbook,
                    include_defaults=True,
                    include_min_max=True,
                    sheet_name="INPUT_DETAILS",
                )
        finally:
            workbook.close()

        # Handle hourly output curves (separate Excel files)
        if export_data.hourly_output_curves is not None:
            ExcelExporter._export_hourly_curves(
                export_data, Path(path), export_data.config.output_carriers
            )

        # Handle annual exports (separate Excel files)
        if export_data.annual_exports is not None:
            ExcelExporter._export_annual_exports(export_data, Path(path))

        return Path(path)

    @staticmethod
    def _write_main_info(
        workbook: Workbook, main_info: pd.DataFrame, scenarios: list
    ):
        """Write main scenario information to Excel workbook."""
        if not main_info.empty:
            excel_main_df = excel_utils.build_excel_main_dataframe(main_info, scenarios)
            sanitized_df = excel_utils.sanitize_dataframe_for_excel(excel_main_df)
            excel_utils.add_frame(
                name="MAIN",
                frame=sanitized_df,
                workbook=workbook,
                column_width=18,
                scenario_styling=True,
            )

    @staticmethod
    def _export_hourly_curves(
        export_data: ExportDataCollection,
        main_path: Path,
        carriers: Optional[Sequence[str]],
    ):
        """
        Export hourly curves to separate Excel file.

        Args:
            export_data: The export data collection containing hourly curves
            main_path: Path to the main Excel file
            carriers: Carrier types to organize the export by
        """
        if not export_data.hourly_output_curves:
            return

        output_path = main_path.with_name(
            f"{main_path.stem}_hourly_output_curves{main_path.suffix}"
        )

        try:
            ExcelExporter._write_hourly_curves(
                export_data.hourly_output_curves, output_path, carriers
            )
        except Exception as e:
            logger.warning("Failed exporting output curves workbook: %s", e)

    @staticmethod
    def _export_annual_exports(
        export_data: ExportDataCollection,
        main_path: Path,
    ):
        """
        Export annual exports to separate Excel file.

        Args:
            export_data: The export data collection containing annual exports
            main_path: Path to the main Excel file
        """
        if not export_data.annual_exports:
            return

        output_path = main_path.with_name(
            f"{main_path.stem}_annual_exports{main_path.suffix}"
        )

        try:
            ExcelExporter._write_annual_exports(export_data.annual_exports, output_path)
        except Exception as e:
            logger.warning("Failed exporting annual exports workbook: %s", e)

    @staticmethod
    def _write_annual_exports(
        exports_data: Dict[str, Dict[str, pd.DataFrame]],
        output_path: Path,
    ) -> None:
        """
        Write annual exports data to Excel file.

        Args:
            exports_data: Dict mapping export names to scenario data
            output_path: Path to the output Excel file
        """
        if not exports_data:
            logger.info("No export data available")
            return

        workbook = None
        try:
            workbook = Workbook(str(output_path))

            for export_name, scenarios_data in sorted(exports_data.items()):
                if not scenarios_data:
                    continue

                frames = []
                for scenario_key, df in scenarios_data.items():
                    df_copy = df.copy()
                    df_copy.insert(0, "scenario", scenario_key)
                    frames.append(df_copy)

                if frames:
                    combined = pd.concat(frames, ignore_index=True)
                    sheet_name = export_name.upper()[:31]

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

    @staticmethod
    def _write_hourly_curves(
        curves_data: Dict[str, Dict[str, pd.DataFrame]],
        output_path: Path,
        carriers: Optional[Sequence[str]],
    ) -> None:
        """
        Write hourly curves data to Excel file, organized by carrier.

        Args:
            curves_data: Dict mapping curve names to scenario data
            output_path: Path to the output Excel file
            carriers: Carrier types to organize sheets by
        """
        from pyetm.models.hourly_output_curves import HourlyOutputCurves

        if not curves_data:
            logger.info("No hourly curves data available")
            return

        carrier_map = HourlyOutputCurves._load_carrier_mappings()
        valid_carriers = list(carrier_map.keys())
        selected = list(valid_carriers if carriers is None else carriers)
        selected = [c for c in selected if c in valid_carriers]
        if not selected:
            selected = valid_carriers

        curve_to_carrier = {}
        for carrier, curves_list in carrier_map.items():
            for curve_name in curves_list:
                curve_to_carrier[curve_name] = carrier

        workbook = None
        wrote_any = False
        try:
            for carrier in selected:
                carrier_curves = {
                    curve_name: scenarios_data
                    for curve_name, scenarios_data in curves_data.items()
                    if curve_to_carrier.get(curve_name) == carrier
                }

                if not carrier_curves:
                    continue

                series_entries = []
                for curve_name, scenarios_data in sorted(carrier_curves.items()):
                    for scenario_id, df in sorted(scenarios_data.items()):
                        if df is None or df.empty:
                            continue

                        if isinstance(df, pd.Series):
                            series_entries.append(((scenario_id, curve_name), df))
                        elif isinstance(df, pd.DataFrame):
                            if df.shape[1] == 1:
                                series_entries.append(
                                    ((scenario_id, curve_name), df.iloc[:, 0])
                                )
                            else:
                                for col in df.columns:
                                    sub_curve = f"{curve_name}:{col}"
                                    series_entries.append(
                                        ((scenario_id, sub_curve), df[col])
                                    )

                if not series_entries:
                    continue

                cols = [key for key, _ in series_entries]
                frames = [s for _, s in series_entries]
                combined = pd.concat(frames, axis=1)
                combined.columns = pd.MultiIndex.from_tuples(
                    cols, names=["Scenario", "Curve"]
                )

                if workbook is None:
                    workbook = Workbook(str(output_path))

                excel_utils.add_frame(
                    name=carrier.upper(),
                    frame=combined,
                    workbook=workbook,
                    column_width=18,
                    scenario_styling=True,
                )
                wrote_any = True

        finally:
            if workbook is not None and wrote_any:
                workbook.close()
