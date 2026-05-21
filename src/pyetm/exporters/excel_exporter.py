"""Excel export functionality for scenario data."""

import logging
from pathlib import Path
from typing import Optional, Sequence, Dict, List, Any, TYPE_CHECKING, cast
import pandas as pd
from pandas import Series
from xlsxwriter import Workbook  # type: ignore[import-untyped]

from pyetm.models.export_data_collection import ExportDataCollection
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyetm.models.scenario import Scenario


class PathManager:
    """Handles file path operations for Excel export."""

    @staticmethod
    def ensure_directory_exists(path: str) -> None:
        """Create parent directory if it doesn't exist."""
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        except (PermissionError, OSError) as e:
            logger.warning("Could not create directory for %s: %s", path, e)

    @staticmethod
    def get_hourly_curves_path(main_path: Path) -> Path:
        """Get path for hourly curves workbook."""
        return main_path.with_name(f"{main_path.stem}_hourly_output_curves{main_path.suffix}")

    @staticmethod
    def get_annual_exports_path(main_path: Path) -> Path:
        """Get path for annual exports workbook."""
        return main_path.with_name(f"{main_path.stem}_annual_exports{main_path.suffix}")


class MainSheetWriter:
    """Writes main scenario info sheet to Excel."""

    @staticmethod
    def write(workbook: Workbook, main_info: pd.DataFrame, scenarios: list[Any]) -> None:
        """Write main scenario information sheet."""
        if main_info.empty:
            return

        excel_main_df = excel_utils.build_excel_main_dataframe(main_info, scenarios)
        sanitized_df = excel_utils.sanitize_dataframe_for_excel(excel_main_df)
        excel_utils.add_frame(
            name="MAIN",
            frame=sanitized_df,
            workbook=workbook,
            column_width=18,
            scenario_styling=True,
        )


class DataSheetWriter:
    """Writes data sheets (inputs, sortables, etc.) to Excel."""

    @staticmethod
    def write_inputs(workbook: Workbook, inputs: Optional[pd.DataFrame]) -> None:
        """Write inputs sheet."""
        if inputs is None or inputs.empty:
            return
        excel_utils.add_frame("SLIDER_SETTINGS", inputs, workbook, column_width=18)

    @staticmethod
    def write_inputs_detailed(workbook: Workbook, inputs_detailed: Optional[pd.DataFrame]) -> None:
        """Write detailed inputs sheet with defaults and min/max."""
        if inputs_detailed is None or inputs_detailed.empty:
            return

        sanitized = inputs_detailed.map(
            lambda v: ", ".join(map(str, v)) if isinstance(v, (list, tuple, set)) else v
        )
        excel_utils.add_frame("INPUT_DETAILS", sanitized, workbook, column_width=18)

    @staticmethod
    def write_sortables(workbook: Workbook, sortables: Optional[pd.DataFrame]) -> None:
        """Write sortables sheet."""
        if sortables is None or sortables.empty:
            return
        excel_utils.add_frame("SORTABLES", sortables, workbook, column_width=18)

    @staticmethod
    def write_custom_curves(
        workbook: Workbook, custom_curves: Optional[Dict[str, Dict[str, Series[Any]]]]
    ) -> None:
        """Write custom curves sheet."""
        if not custom_curves:
            return
        combined_df = DataSheetWriter._combine_custom_curves(custom_curves)
        if combined_df is not None and not combined_df.empty:
            excel_utils.add_frame("CUSTOM_CURVES", combined_df, workbook, column_width=18)

    @staticmethod
    def _combine_custom_curves(curves_dict: Dict[str, Dict[str, Series[Any]]]) -> Optional[pd.DataFrame]:
        """Combine custom curves into single DataFrame."""
        all_series = []
        for curve_name, scenarios_data in curves_dict.items():
            for scenario_id, series in scenarios_data.items():
                all_series.append(((scenario_id, curve_name), series))

        if not all_series:
            return None

        cols = [key for key, _ in all_series]
        frames = [s for _, s in all_series]
        combined = pd.concat(frames, axis=1)
        combined.columns = pd.MultiIndex.from_tuples(cols, names=["Scenario", "Curve"])
        return combined

    @staticmethod
    def write_gquery_results(workbook: Workbook, gquery_results: Optional[pd.DataFrame]) -> None:
        """Write gquery results sheet."""
        if gquery_results is None or gquery_results.empty:
            return
        excel_utils.add_frame("GQUERIES", gquery_results, workbook, column_width=18)

    @staticmethod
    def write_users(workbook: Workbook, users: Optional[pd.DataFrame]) -> None:
        """Write users sheet."""
        if users is None or users.empty:
            return
        excel_utils.add_frame("USERS", users, workbook, column_width=18)


class HourlyCurvesWriter:
    """Writes hourly curves to separate Excel file."""

    @staticmethod
    def write(
        curves_data: Dict[str, Dict[str, pd.DataFrame]],
        output_path: Path,
        carriers: Optional[Sequence[str]],
    ) -> None:
        """Write hourly curves to Excel, organized by carrier."""
        if not curves_data:
            logger.info("No hourly curves data available")
            return

        carrier_curves = HourlyCurvesWriter._organize_by_carrier(curves_data, carriers)
        HourlyCurvesWriter._write_carrier_sheets(carrier_curves, output_path)

    @staticmethod
    def _organize_by_carrier(
        curves_data: Dict[str, Dict[str, pd.DataFrame]], carriers: Optional[Sequence[str]]
    ) -> Dict[str, Dict[str, Dict[str, pd.DataFrame]]]:
        """Organize curves by carrier type."""
        from pyetm.models.hourly_output_curves import HourlyOutputCurves

        carrier_map = HourlyOutputCurves._load_carrier_mappings()
        selected_carriers = HourlyCurvesWriter._select_carriers(carrier_map, carriers)

        curve_to_carrier = {}
        for carrier, curves_list in carrier_map.items():
            for curve_name in curves_list:
                curve_to_carrier[curve_name] = carrier

        carrier_curves = {}
        for carrier in selected_carriers:
            carrier_curves[carrier] = {
                curve_name: scenarios_data
                for curve_name, scenarios_data in curves_data.items()
                if curve_to_carrier.get(curve_name) == carrier
            }

        return carrier_curves

    @staticmethod
    def _select_carriers(carrier_map: Dict[str, Any], carriers: Optional[Sequence[str]]) -> List[str]:
        """Select which carriers to export."""
        valid_carriers = list(carrier_map.keys())
        selected = list(valid_carriers if carriers is None else carriers)
        selected = [c for c in selected if c in valid_carriers]
        return selected if selected else valid_carriers

    @staticmethod
    def _write_carrier_sheets(
        carrier_curves: Dict[str, Dict[str, Dict[str, pd.DataFrame]]], output_path: Path
    ) -> None:
        """Write carrier sheets to workbook."""
        workbook = None
        wrote_any = False

        try:
            for carrier, curves in carrier_curves.items():
                if not curves:
                    continue

                combined = HourlyCurvesWriter._combine_carrier_curves(curves)
                if combined is None:
                    continue

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

    @staticmethod
    def _combine_carrier_curves(
        curves: Dict[str, Dict[str, pd.DataFrame]]
    ) -> Optional[pd.DataFrame]:
        """Combine curves for a carrier into single DataFrame."""
        series_entries = []

        for curve_name, scenarios_data in sorted(curves.items()):
            for scenario_id, df in sorted(scenarios_data.items()):
                if df is None or df.empty:
                    continue

                entries = HourlyCurvesWriter._normalize_to_series(df, scenario_id, curve_name)
                series_entries.extend(entries)

        if not series_entries:
            return None

        cols = [key for key, _ in series_entries]
        frames = [s for _, s in series_entries]
        combined = pd.concat(frames, axis=1)
        combined.columns = pd.MultiIndex.from_tuples(cols, names=["Scenario", "Curve"])
        return combined

    @staticmethod
    def _normalize_to_series(
        df: pd.DataFrame | Series[Any], scenario_id: str, curve_name: str
    ) -> List[tuple[tuple[str, str], Series[Any]]]:
        """Normalize DataFrame or Series to list of series entries."""
        if isinstance(df, pd.Series):
            return [((scenario_id, curve_name), df)]

        if isinstance(df, pd.DataFrame):
            if df.shape[1] == 1:
                return [((scenario_id, curve_name), df.iloc[:, 0])]
            return [
                ((scenario_id, f"{curve_name}:{col}"), df[col])
                for col in df.columns
            ]

        return []


class AnnualExportsWriter:
    """Writes annual exports to separate Excel file."""

    @staticmethod
    def write(exports_data: Dict[str, Dict[str, pd.DataFrame]], output_path: Path) -> None:
        """Write annual exports to Excel file."""
        if not exports_data:
            logger.info("No export data available")
            return

        workbook = None
        try:
            workbook = Workbook(str(output_path))

            for export_name in sorted(exports_data.keys()):
                AnnualExportsWriter._write_export_sheet(
                    workbook, export_name, exports_data[export_name]
                )
        finally:
            if workbook is not None:
                workbook.close()

    @staticmethod
    def _write_export_sheet(
        workbook: Workbook, export_name: str, scenarios_data: Dict[str, pd.DataFrame]
    ) -> None:
        """Write a single export sheet."""
        if not scenarios_data:
            return

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


class ExcelExporter:
    """Handles Excel export for scenario data."""

    @staticmethod
    def write(
        export_data: ExportDataCollection, path: str, scenarios: list[Any]
    ) -> Path:
        """
        Write export data collection to Excel format.

        Args:
            export_data: The collected export data in generic format
            path: Output file path for the main Excel file
            scenarios: List of scenario objects for proper formatting

        Returns:
            Path to the created main Excel file
        """
        PathManager.ensure_directory_exists(path)

        ExcelExporter._write_main_workbook(export_data, path, scenarios)
        ExcelExporter._write_separate_workbooks(export_data, Path(path))

        return Path(path)

    @staticmethod
    def _write_main_workbook(
        export_data: ExportDataCollection, path: str, scenarios: list[Any]
    ) -> None:
        """Write main workbook with all core data sheets."""
        workbook = Workbook(path)
        try:
            MainSheetWriter.write(workbook, export_data.main_info, scenarios)
            DataSheetWriter.write_inputs(workbook, export_data.inputs)
            DataSheetWriter.write_inputs_detailed(workbook, export_data.inputs_detailed)
            DataSheetWriter.write_sortables(workbook, export_data.sortables)
            DataSheetWriter.write_custom_curves(workbook, export_data.custom_curves)
            DataSheetWriter.write_gquery_results(workbook, export_data.gquery_results)
            DataSheetWriter.write_users(workbook, export_data.users)
        finally:
            workbook.close()

    @staticmethod
    def _write_separate_workbooks(export_data: ExportDataCollection, main_path: Path) -> None:
        """Write separate workbooks for hourly curves and annual exports."""
        if export_data.hourly_output_curves:
            ExcelExporter._safe_write_hourly_curves(
                export_data.hourly_output_curves,
                PathManager.get_hourly_curves_path(main_path),
                export_data.config.output_carriers,
            )

        if export_data.annual_exports:
            ExcelExporter._safe_write_annual_exports(
                export_data.annual_exports,
                PathManager.get_annual_exports_path(main_path),
            )

    @staticmethod
    def _safe_write_hourly_curves(
        curves_data: Dict[str, Dict[str, pd.DataFrame]], output_path: Path, carriers: Optional[Sequence[str]]
    ) -> None:
        """Write hourly curves with error handling."""
        try:
            HourlyCurvesWriter.write(curves_data, output_path, carriers)
        except (IOError, OSError) as e:
            logger.warning("Failed exporting output curves workbook: %s", e)

    @staticmethod
    def _safe_write_annual_exports(exports_data: Dict[str, Dict[str, pd.DataFrame]], output_path: Path) -> None:
        """Write annual exports with error handling."""
        try:
            AnnualExportsWriter.write(exports_data, output_path)
        except (IOError, OSError) as e:
            logger.warning("Failed exporting annual exports workbook: %s", e)
