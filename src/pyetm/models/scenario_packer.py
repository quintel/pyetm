"""Scenario packing utilities for creating multiple scenario variations."""

from __future__ import annotations

import pandas as pd
from pandas import Series
import logging
from pathlib import Path
from os import PathLike
from pydantic import BaseModel
from typing import Optional, Dict, Any, Sequence, List, Union, TYPE_CHECKING, cast
from xlsxwriter import Workbook  # type: ignore[import-untyped]

from pyetm.models.packables.inputs_pack import InputsPack
from pyetm.models.packables.hourly_output_curves_pack import HourlyOutputCurvesPack
from pyetm.models.packables.annual_exports_pack import AnnualExportsPack
from pyetm.models.packables.query_pack import QueryPack
from pyetm.models.packables.sortable_pack import SortablePack
from pyetm.models.packables.custom_curves_pack import CustomCurvesPack
from pyetm.models.packables.users_pack import UsersPack
from pyetm.models import Session
from pyetm.models.export_config import ExportConfig
from pyetm.models.export_data_collection import ExportDataCollection
from pyetm.types import AnnualExportType, HourlyCurveType, CarrierType
from pyetm.validators import (
    validate_hourly_curve_names,
    validate_export_names,
    validate_carrier_type,
)

if TYPE_CHECKING:
    from pyetm.models.scenario import Scenario
from pyetm.models.scenario_loader import (
    ScenarioLoader,
    SessionLoader,
    SavedScenarioLoader,
)
from pyetm.utils import excel_utils
from pyetm.utils.safe_cast import cast_bool, cast_int

logger = logging.getLogger(__name__)


class ScenarioPacker(BaseModel):
    """
    Packs one or multiple scenarios for export to dataframes or excel
    """

    # Pack collections
    _custom_curves: CustomCurvesPack = CustomCurvesPack()
    _inputs: InputsPack = InputsPack()
    _sortables: SortablePack = SortablePack()
    _hourly_output_curves: HourlyOutputCurvesPack = HourlyOutputCurvesPack()
    _annual_exports: AnnualExportsPack = AnnualExportsPack()
    _query_pack: QueryPack = QueryPack()
    _users: UsersPack = UsersPack()

    # Scenario management methods
    def add(self, *scenarios: Any) -> None:
        """Add scenarios to all packs. Supports Sessions and Scenarios"""
        self.add_custom_curves(*scenarios)
        self.add_inputs(*scenarios)
        self.add_sortables(*scenarios)
        self.add_hourly_output_curves(*scenarios)
        self.add_annual_exports(*scenarios)
        self._query_pack.add(*scenarios)
        self._users.add(*scenarios)

    def add_custom_curves(self, *scenarios: Any) -> None:
        self._custom_curves.add(*scenarios)

    def add_inputs(self, *scenarios: Any) -> None:
        self._inputs.add(*scenarios)

    def add_sortables(self, *scenarios: Any) -> None:
        self._sortables.add(*scenarios)

    def add_hourly_output_curves(self, *scenarios: Any) -> None:
        self._hourly_output_curves.add(*scenarios)

    def add_annual_exports(self, *scenarios: Any) -> None:
        self._annual_exports.add(*scenarios)

    def main_info(self) -> pd.DataFrame:
        """
        Create main info DataFrame with scenarios as rows.

        Returns DataFrame with scenarios as rows. The scenario_id column contains
        scenario identifiers, and the DataFrame index is set to match for proper
        Excel export. The session_id and saved_scenario_id columns from
        _to_dataframe() provide the export-friendly IDs.
        """
        scenarios = self._scenarios()
        if not scenarios:
            return pd.DataFrame()
        df = pd.concat([scenario._to_dataframe() for scenario in scenarios], axis=1)
        # Transpose and create scenario_id column from the index
        result = df.T.reset_index(names=["scenario_id"])
        # Set the index to scenario_id for proper Excel row labels, while keeping the column
        result.index = result["scenario_id"]
        result.index.name = None
        return result

    def inputs(self, columns: str = "value") -> pd.DataFrame:
        return self._inputs.to_dataframe(columns=columns)

    def gquery_results(self, columns: str = "future") -> pd.DataFrame:
        return self._query_pack.to_dataframe(columns=columns)

    def sortables(self) -> pd.DataFrame:
        return self._sortables.to_dataframe()

    def custom_curves(
        self, as_dict: bool = False, curves: Optional[Sequence[str]] = None
    ) -> pd.DataFrame | dict[str, dict[str, pd.DataFrame]]:
        """
        Get custom curves for all scenarios.

        Args:
            as_dict: If True, returns dict[curve_name, dict[scenario_id, DataFrame]].
                     If False (default), returns concatenated DataFrame for backward compatibility.
            curves: Optional filter for specific curve names (only used when as_dict=True)

        Returns:
            DataFrame (default) or dict depending on as_dict parameter
        """
        if as_dict:
            return self._custom_curves.to_dict_per_curve(curves=curves)
        return self._custom_curves.to_dataframe()

    @staticmethod
    def _validate_curve_params(
        curves: Optional[Sequence[str]], carrier_type: Optional[CarrierType]
    ) -> None:
        """Validate curve parameters are mutually exclusive and at least one is provided."""
        if curves is not None and carrier_type is not None:
            raise ValueError("Specify either 'curves' or 'carrier_type', not both")
        if curves is None and carrier_type is None:
            raise ValueError("Must specify either 'curves' or 'carrier_type'")

    @staticmethod
    def _resolve_curve_names(
        curves: Optional[Sequence[str]], carrier_type: Optional[CarrierType]
    ) -> List[str]:
        """Resolve curve names from curves parameter or carrier type mapping."""
        if carrier_type is not None:
            validate_carrier_type(carrier_type)
            from pyetm.models.hourly_output_curves import HourlyOutputCurves

            carrier_mappings = HourlyOutputCurves._load_carrier_mappings()
            return carrier_mappings.get(carrier_type, [])
        else:
            # Ensure curves is not None before calling validate
            if curves is None:
                return []
            return validate_hourly_curve_names(list(curves))

    @staticmethod
    def _get_curves_for_carriers(carriers: Sequence[str]) -> List[str]:
        """
        Map carrier names to curve names using HourlyOutputCurves mappings.

        Args:
            carriers: List of carrier types (e.g., ["electricity", "heat"])

        Returns:
            List of curve names for all specified carriers

        Example:
            ["electricity"] → ["electricity_production", "electricity_import", ...]
        """
        from pyetm.models.hourly_output_curves import HourlyOutputCurves

        carrier_mappings = HourlyOutputCurves._load_carrier_mappings()
        all_curves = []
        for carrier in carriers:
            validate_carrier_type(carrier)
            all_curves.extend(carrier_mappings.get(carrier, []))
        return all_curves

    def hourly_output_curves(
        self,
        curves: Optional[Sequence[str]] = None,
        carrier_type: Optional[CarrierType] = None,
    ) -> dict[str, dict[str, pd.DataFrame]]:
        """
        Get hourly output curves for all scenarios, organized by curve name.

        Args:
            curves: Specific curve names to retrieve. If provided, carrier_type is ignored.
            carrier_type: Carrier type to get all associated curves for.
                         One of: "electricity", "heat", "hydrogen", "methane"

        Returns:
            Dict mapping curve names to dicts of {scenario_title: DataFrame}

        Note:
            For concatenated DataFrame format, use: packer._hourly_output_curves.to_dataframe(curves)
        """
        self._validate_curve_params(curves, carrier_type)
        curve_names = self._resolve_curve_names(curves, carrier_type)
        return self._hourly_output_curves.to_dict_per_curve(curves=curve_names)

    def annual_exports(
        self,
        exports: Optional[AnnualExportType | Sequence[AnnualExportType]] = None,
    ) -> dict[str, dict[str, pd.DataFrame]]:
        """
        Get annual exports for all scenarios, organized by export type.
        """
        validated_exports: Optional[List[str]] = None
        if exports is not None:
            # If exports is a string, validate_export_names expects it directly
            # If exports is a Sequence (but not str), pass it directly too
            validated_exports = validate_export_names(cast("str | list[str]", exports))
        return self._annual_exports.to_dict_per_export(exports=validated_exports)

    def collect_export_data(
        self,
        *,
        hourly_curves: Optional[Sequence[str]] = None,
        include_inputs: Optional[bool] = None,
        include_sortables: Optional[bool] = None,
        include_custom_curves: Optional[bool] = None,
        include_gqueries: Optional[bool] = None,
        include_hourly_curves: Optional[bool] = None,
        include_input_defaults: Optional[bool] = None,
        include_input_min_max: Optional[bool] = None,
        include_users: Optional[bool] = None,
        include_annual_exports: Optional[Sequence[str]] = None,
    ) -> ExportDataCollection:
        """
        Collect export data in format-agnostic structure.

        Args:
            hourly_curves: Carrier types for hourly output curves (e.g., ["electricity", "heat"]).
                          Only used if include_hourly_curves is True.
            include_inputs: Include input parameters. Defaults based on scenario export config.
            include_sortables: Include sortable technologies. Defaults to False.
            include_custom_curves: Include custom curves. Defaults to False.
            include_gqueries: Include query results. Defaults to False.
            include_hourly_curves: Include hourly output curves. Defaults to False.
            include_input_defaults: Include default values for inputs. Defaults to False.
            include_input_min_max: Include min/max bounds for inputs. Defaults to False.
            include_users: Include user permissions. Defaults to False.
            include_annual_exports: List of annual export names to include.
                                   Examples: ["energy_flow", "sankey", "storage_parameters"]
                                   Defaults to None (no annual exports).

        Returns:
            ExportDataCollection containing all requested data in format-agnostic structures
        """
        if not self._scenarios():
            raise ValueError("Packer was empty, nothing to export")

        global_config = self._get_global_export_config()
        resolved_flags = self._resolve_export_flags(
            global_config,
            include_inputs,
            include_sortables,
            include_custom_curves,
            include_gqueries,
            include_hourly_curves,
            include_input_defaults,
            include_input_min_max,
            include_users,
            include_annual_exports,
            hourly_curves,
        )

        hourly_curve_carriers = self._determine_hourly_curve_carriers(
            hourly_curves, resolved_flags, global_config
        )
        config = self._build_export_config(resolved_flags, hourly_curve_carriers)
        collected_data = self._collect_data_by_flags(
            resolved_flags, hourly_curve_carriers
        )

        return ExportDataCollection(
            main_info=self.main_info(),
            inputs=collected_data.get("inputs"),
            inputs_detailed=collected_data.get("inputs_detailed"),
            sortables=collected_data.get("sortables"),
            custom_curves=collected_data.get("custom_curves"),
            hourly_output_curves=collected_data.get("hourly_output_curves"),
            annual_exports=collected_data.get("annual_exports"),
            gquery_results=collected_data.get("gquery_results"),
            users=collected_data.get("users"),
            config=config,
        )

    def _determine_hourly_curve_carriers(
        self,
        hourly_curves: Optional[Sequence[str]],
        resolved_flags: Dict[str, Any],
        global_config: Optional[ExportConfig],
    ) -> Optional[List[str]]:
        """Determine hourly curve carriers from parameters or config."""
        if not resolved_flags["include_hourly_curves"]:
            return None
        if hourly_curves is not None:
            return list(hourly_curves)
        if global_config and global_config.hourly_curves:
            return list(global_config.hourly_curves)
        return None

    def _build_export_config(
        self, resolved_flags: Dict[str, Any], hourly_curve_carriers: Optional[List[str]]
    ) -> ExportConfig:
        """Build ExportConfig from resolved flags."""
        return ExportConfig(
            include_inputs=resolved_flags["include_inputs"],
            include_sortables=resolved_flags["include_sortables"],
            include_custom_curves=resolved_flags["include_custom_curves"],
            include_gqueries=resolved_flags["include_gqueries"],
            include_users=resolved_flags.get("include_users", False),
            hourly_curves=hourly_curve_carriers,
            include_annual_exports=resolved_flags.get("include_annual_exports"),
            include_input_defaults=resolved_flags.get("include_input_defaults", False),
            include_input_min_max=resolved_flags.get("include_input_min_max", False),
        )

    def _collect_data_by_flags(
        self, resolved_flags: Dict[str, Any], hourly_curve_carriers: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Collect all data based on resolved flags."""
        collected: Dict[str, Any] = {}

        if resolved_flags["include_inputs"]:
            collected["inputs"] = self.inputs()
            # Collect inputs_detailed if defaults or min_max are requested
            if resolved_flags.get("include_input_defaults") or resolved_flags.get(
                "include_input_min_max"
            ):
                collected["inputs_detailed"] = self._inputs.to_dataframe(
                    include_defaults=resolved_flags.get(
                        "include_input_defaults", False
                    ),
                    include_min_max=resolved_flags.get("include_input_min_max", False),
                )
        if resolved_flags["include_sortables"]:
            collected["sortables"] = self.sortables()
        if resolved_flags["include_custom_curves"]:
            collected["custom_curves"] = self.custom_curves(as_dict=True)
        if resolved_flags["include_hourly_curves"]:
            collected["hourly_output_curves"] = self._collect_hourly_curves(
                hourly_curve_carriers
            )
        if resolved_flags.get("include_annual_exports"):
            collected["annual_exports"] = self._annual_exports.to_dict_per_export(
                exports=resolved_flags["include_annual_exports"]
            )
        if resolved_flags["include_gqueries"]:
            collected["gquery_results"] = self.gquery_results()
        if resolved_flags["include_users"]:
            collected["users"] = self._users.to_dataframe()

        return collected

    def _collect_hourly_curves(
        self, output_carriers: Optional[List[str]]
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Collect hourly output curves based on carriers."""
        if output_carriers:
            curve_names = self._get_curves_for_carriers(output_carriers)
            return self._hourly_output_curves.to_dict_per_curve(curves=curve_names)
        return self._hourly_output_curves.to_dict_per_curve(curves=None)

    def couplings(self) -> pd.DataFrame:
        if len(self._scenarios()) == 0:
            return pd.DataFrame()

        return pd.concat(
            [
                scenario.couplings.to_series(str(scenario.identifier()))
                for scenario in self._scenarios()
            ],
            axis=1,
        )

    def add_queries(self, gquery_keys: List[str]) -> None:
        self._query_pack.add_queries(gquery_keys)

    def to_excel(
        self,
        path: str,
        *,
        hourly_curves: Optional[Sequence[str]] = None,
        include_inputs: Optional[bool] = None,
        include_sortables: Optional[bool] = None,
        include_custom_curves: Optional[bool] = None,
        include_gqueries: Optional[bool] = None,
        include_hourly_curves: Optional[bool] = None,
        include_input_defaults: Optional[bool] = None,
        include_input_min_max: Optional[bool] = None,
        include_users: Optional[bool] = None,
        include_annual_exports: Optional[Sequence[str]] = None,
    ) -> None:
        """Export scenarios to Excel file."""
        from pyetm.exporters.excel_exporter import ExcelExporter

        export_data = self.collect_export_data(
            hourly_curves=hourly_curves,
            include_inputs=include_inputs,
            include_sortables=include_sortables,
            include_custom_curves=include_custom_curves,
            include_gqueries=include_gqueries,
            include_hourly_curves=include_hourly_curves,
            include_input_defaults=include_input_defaults,
            include_input_min_max=include_input_min_max,
            include_users=include_users,
            include_annual_exports=include_annual_exports,
        )

        ExcelExporter.write(
            export_data=export_data, path=path, scenarios=list(self._scenarios())
        )

    def _get_global_export_config(self) -> Optional[ExportConfig]:
        """Get global export configuration from first scenario that has one."""
        for scenario in self._scenarios():
            # Try method first (for real scenarios), then attribute (for mocks/tests)
            config = None
            if hasattr(scenario, "get_export_config") and callable(
                getattr(scenario, "get_export_config", None)
            ):
                try:
                    config = scenario.get_export_config()
                    # Validate it's a real ExportConfig, not a Mock
                    if config is not None and not isinstance(config, ExportConfig):
                        config = None
                except (AttributeError, TypeError):
                    # Fallback if method doesn't work properly
                    config = None

            # Fallback to direct attribute access
            if config is None:
                config = getattr(scenario, "_export_config", None)

            if config is not None and isinstance(config, ExportConfig):
                return config
        return None

    def _resolve_export_flags(
        self,
        global_config: Optional[ExportConfig],
        include_inputs: Optional[bool],
        include_sortables: Optional[bool],
        include_custom_curves: Optional[bool],
        include_gqueries: Optional[bool],
        include_hourly_curves: Optional[bool],
        include_input_defaults: Optional[bool],
        include_input_min_max: Optional[bool],
        include_users: Optional[bool],
        include_annual_exports: Optional[Sequence[str]] = None,
        hourly_curves: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """Resolve all export flags from parameters and configuration."""
        resolver = excel_utils.ExportConfigResolver()

        return {
            "include_inputs": resolver.resolve_boolean(
                include_inputs,
                (
                    getattr(global_config, "include_inputs", None)
                    if global_config
                    else None
                ),
                True,
            ),
            "include_sortables": resolver.resolve_boolean(
                include_sortables,
                (
                    getattr(global_config, "include_sortables", None)
                    if global_config
                    else None
                ),
                False,
            ),
            "include_custom_curves": resolver.resolve_boolean(
                include_custom_curves,
                (
                    getattr(global_config, "include_custom_curves", None)
                    if global_config
                    else None
                ),
                False,
            ),
            "include_gqueries": resolver.resolve_boolean(
                include_gqueries,
                (
                    getattr(global_config, "include_gqueries", None)
                    if global_config
                    else None
                ),
                False,
            ),
            "include_hourly_curves": resolver.resolve_boolean(
                include_hourly_curves,
                (
                    (getattr(global_config, "hourly_curves", None) is not None)
                    if global_config
                    else None
                ),
                # Auto-enable if hourly_curves carriers are specified
                hourly_curves is not None and len(hourly_curves) > 0,
            ),
            "include_input_defaults": resolver.resolve_boolean(
                include_input_defaults,
                (
                    getattr(global_config, "include_input_defaults", None)
                    if global_config
                    else None
                ),
                False,
            ),
            "include_input_min_max": resolver.resolve_boolean(
                include_input_min_max,
                (
                    getattr(global_config, "include_input_min_max", None)
                    if global_config
                    else None
                ),
                False,
            ),
            "include_users": resolver.resolve_boolean(
                include_users,
                (
                    getattr(global_config, "include_users", None)
                    if global_config
                    else None
                ),
                False,
            ),
            "include_annual_exports": (
                list(include_annual_exports)
                if include_annual_exports
                else (
                    list(getattr(global_config, "include_annual_exports", []))
                    if global_config
                    and getattr(global_config, "include_annual_exports", None)
                    else None
                )
            ),
        }

    def _add_main_sheet(self, workbook: Workbook) -> None:
        """Add main scenario information sheet to workbook."""
        main_df = self.main_info()
        if not main_df.empty:
            excel_main_df = excel_utils.build_excel_main_dataframe(
                main_df, list(self._scenarios())
            )
            sanitized_df = excel_utils.sanitize_dataframe_for_excel(excel_main_df)
            excel_utils.add_frame(
                name="MAIN",
                frame=sanitized_df,
                workbook=workbook,
                column_width=18,
                scenario_styling=True,
            )

    def _add_data_sheets(self, workbook: Workbook, flags: Dict[str, Any]) -> None:
        """Add data sheets to workbook based on flags."""
        if flags["include_inputs"]:
            self._inputs.add_to_workbook(
                workbook,
                include_defaults=flags["include_input_defaults"],
                include_min_max=flags["include_input_min_max"],
            )

        if flags["include_sortables"]:
            self._sortables.add_to_workbook(workbook)

        if flags["include_custom_curves"]:
            self._custom_curves.add_to_workbook(workbook)

        if flags.get("include_users"):
            self._users.add_to_workbook(workbook)

    def _export_hourly_output_curves_if_needed(
        self,
        main_path: str,
        hourly_curves: Optional[Sequence[str]],
        include_hourly_curves: bool,
        global_config: Optional[ExportConfig],
    ) -> None:
        """Export output curves to separate file if needed."""
        if not include_hourly_curves:
            return

        # Determine output file path
        base_path = Path(main_path)
        output_path = str(
            base_path.with_name(
                f"{base_path.stem}_hourly_output_curves{base_path.suffix}"
            )
        )

        # Determine carriers to export
        chosen_carriers = list(hourly_curves) if hourly_curves else None
        if chosen_carriers is None and global_config is not None:
            config_carriers = getattr(global_config, "hourly_curves", None)
            chosen_carriers = list(config_carriers) if config_carriers else None

        try:
            self._hourly_output_curves.to_excel_per_carrier(
                output_path, chosen_carriers
            )
        except Exception as e:
            logger.warning("Failed exporting output curves workbook: %s", e)

    def _export_annual_exports_if_needed(
        self,
        main_path: str,
        include_annual_exports: Optional[Sequence[str]],
        global_config: Optional[ExportConfig],
    ) -> None:
        """Export annual exports to separate file if needed."""
        # Determine which exports to include
        exports_to_include = None
        if include_annual_exports:
            exports_to_include = list(include_annual_exports)
        elif global_config is not None:
            config_exports = getattr(global_config, "include_annual_exports", None)
            if config_exports:
                exports_to_include = list(config_exports)

        if not exports_to_include:
            return

        # Determine output file path
        base_path = Path(main_path)
        output_path = str(
            base_path.with_name(f"{base_path.stem}_annual_exports{base_path.suffix}")
        )

        try:
            self._annual_exports.to_excel(output_path, exports=exports_to_include)
        except Exception as e:
            logger.warning("Failed exporting annual exports workbook: %s", e)

    @staticmethod
    def _normalize_update(update: bool | List[str]) -> set[str]:
        """Normalize update parameter to a set of type names."""
        valid_types = {"user_values", "custom_curves", "sortables", "users"}
        if isinstance(update, bool):
            return valid_types if update else set()

        update_set = set(update) if update else set()
        invalid_types = update_set - valid_types
        if invalid_types:
            logger.warning(
                f"Invalid update types will be ignored: {invalid_types}. Valid types: {valid_types}"
            )
        return update_set & valid_types

    @classmethod
    def from_excel(
        cls, xlsx_path: PathLike[str] | str, update: bool | List[str] = False
    ) -> "ScenarioPacker":
        """
        Import scenarios from Excel file.

        Uses per-row 'session' column to determine loader type for each scenario.

        Args:
            xlsx_path: Path to Excel file
            update: Whether to upload data to ETM. Can be:
                - False (default): Load data locally without uploading
                - True: Upload all data types (user_values, custom_curves, sortables, users)
                - List of types: Upload only specified types (e.g., ['user_values', 'users'])
        """
        packer = cls()
        update_set = cls._normalize_update(update)
        path = cls._resolve_excel_path(xlsx_path)

        excel_file = cls._open_excel_file(path, xlsx_path)
        if excel_file is None:
            return packer

        main_df = packer._import_main_sheet(excel_file)
        if main_df is None:
            return packer

        scenarios_by_column = packer._create_scenarios_from_main(main_df, update_set)
        if not scenarios_by_column:
            return packer

        export_config_df = packer._load_export_config_sheet(excel_file)
        if export_config_df is None:
            return packer

        packer._apply_export_configuration(
            main_df, scenarios_by_column, export_config_df
        )
        packer._import_all_sheets(excel_file, main_df, scenarios_by_column, update_set)

        return packer

    @staticmethod
    def _resolve_excel_path(xlsx_path: PathLike[str] | str) -> Path:
        """Resolve Excel file path, checking inputs directory if needed."""
        path = Path(xlsx_path)
        if path.is_absolute() or path.exists():
            return path

        root = ScenarioPacker._find_project_root("inputs")
        relative = path if str(path.parent) != "." else Path(path.name)
        candidate = root / "inputs" / relative
        return candidate if candidate.exists() else path

    @staticmethod
    def _find_project_root(dir_name: str) -> Path:
        """Find project root by searching for a directory."""
        for base in [
            Path.cwd(),
            *Path.cwd().parents,
            Path(__file__).resolve().parent,
            *Path(__file__).resolve().parents,
        ]:
            candidate = base / dir_name
            if candidate.exists() and candidate.is_dir():
                return base
        return Path.cwd()

    @staticmethod
    def _open_excel_file(
        path: Path, original_path: PathLike[str] | str
    ) -> Optional[pd.ExcelFile]:
        """Open Excel file with error handling."""
        try:
            return pd.ExcelFile(str(path))
        except Exception as e:
            logger.warning("Could not open Excel file '%s': %s", original_path, e)
            return None

    def _load_export_config_sheet(
        self, excel_file: pd.ExcelFile
    ) -> Optional[pd.DataFrame]:
        """Load and validate EXPORT_CONFIG sheet."""
        if "EXPORT_CONFIG" not in excel_file.sheet_names:
            logger.error("EXPORT_CONFIG sheet is required but not found in Excel file.")
            return None
        try:
            return excel_file.parse("EXPORT_CONFIG")
        except Exception as e:
            logger.error("Could not parse EXPORT_CONFIG sheet: %s", e)
            return None

    def _import_all_sheets(
        self,
        excel_file: pd.ExcelFile,
        main_df: pd.DataFrame,
        scenarios_by_column: Dict[Any, Session],
        update_set: set[str],
    ) -> None:
        """Import all data sheets from Excel file."""
        self._inputs.import_from_excel(
            excel_file, main_df, scenarios_by_column, update_set
        )
        gquery_df = excel_utils.parse_excel_sheet(
            excel_file,
            self._query_pack.sheet_name,
            **self._query_pack.excel_read_kwargs(),
        )
        if gquery_df is not None:
            self._query_pack.load_from_dataframe(gquery_df)
        self._import_scenario_specific_sheets(
            excel_file, main_df, scenarios_by_column, update_set
        )
        self._users.import_from_excel(
            excel_file, main_df, scenarios_by_column, update_set
        )

    def _import_main_sheet(self, excel_file: pd.ExcelFile) -> Optional[pd.DataFrame]:
        """Import and validate the main sheet."""
        try:
            main_df = excel_file.parse("MAIN")
            if main_df is None or getattr(main_df, "empty", False):
                return None

            # Filter out completely empty rows
            main_df = main_df.dropna(how="all")

            # Filter out rows where critical fields are all empty/NaN
            # A valid row needs at least ONE of: scenario_id, copy_from, parent, OR (area_code AND end_year)
            def is_valid_row(row: Series) -> bool:
                # Check for existing scenario references
                if (
                    pd.notna(row.get("scenario_id"))
                    or pd.notna(row.get("copy_from"))
                    or pd.notna(row.get("parent"))
                ):
                    return True
                # Check for new scenario creation parameters
                area_code = row.get("area_code")
                end_year = row.get("end_year")
                has_valid_area = pd.notna(area_code) and len(str(area_code).strip()) > 0
                return has_valid_area and pd.notna(end_year)

            main_df = main_df[main_df.apply(is_valid_row, axis=1)]

            return main_df if not main_df.empty else None
        except Exception as e:
            logger.warning("Failed to parse MAIN sheet: %s", e)
            return None

    def _create_scenarios_from_main(
        self, main_df: pd.DataFrame, update_set: Optional[set[str]] = None
    ) -> Dict[Any, Session]:
        """Create scenarios from main sheet rows."""
        scenarios_by_row: Dict[Any, Session] = {}
        for idx, row in main_df.iterrows():
            try:
                scenario = self._create_scenario_from_row(idx, row, update_set)
                if scenario is not None:
                    short_name = row.get("short_name")
                    if short_name is not None and not (
                        isinstance(short_name, float) and pd.isna(short_name)
                    ):
                        scenario.set_short_name(short_name)
                    self.add(scenario)
                    scenarios_by_row[idx] = scenario
            except Exception as e:
                logger.warning("Failed to set up scenario for row '%s': %s", idx, e)
        return scenarios_by_row

    def _create_scenario_from_row(
        self,
        row_idx: Any,
        row_data: pd.Series[Any],
        update_set: Optional[set[str]] = None,
    ) -> Optional[Session]:
        """
        Create a scenario from a main sheet row.

        Uses per-row 'session' column to determine loader:
        - session=True: SessionLoader (IDs refer to ETEngine Sessions)
        - session=False: SavedScenarioLoader (IDs refer to MyETM SavedScenarios)
        - Default: False (SavedScenarioLoader)
        """
        is_session = cast_bool(row_data.get("session")) or False
        loader: ScenarioLoader = cast(
            ScenarioLoader,
            SessionLoader(self) if is_session else SavedScenarioLoader(self),
        )

        row_params = self._extract_row_parameters(row_data, row_idx)

        if row_params["scenario_id"]:
            return self._load_existing_scenario(
                row_params["scenario_id"],
                row_params["area_code"],
                row_params["end_year"],
                row_params["row_label"],
                row_params["metadata"],
                loader,
            )
        if row_params["copy_from"]:
            return self._copy_scenario(
                row_params["copy_from"],
                row_params["row_label"],
                row_params["metadata"],
                loader,
            )
        if row_params["parent"]:
            return self._copy_with_roles(
                row_params["parent"], row_params["row_label"], row_params["metadata"]
            )

        return self._create_new_scenario(
            row_params["area_code"],
            row_params["end_year"],
            row_params["row_label"],
            row_params["metadata"],
            update_set,
            loader,
        )

    def _extract_row_parameters(
        self, row_data: pd.Series[Any], row_idx: Any
    ) -> Dict[str, Any]:
        """Extract scenario parameters from row data."""
        return {
            "scenario_id": cast_int(row_data.get("scenario_id")),
            "parent": cast_int(row_data.get("parent")),
            "copy_from": cast_int(row_data.get("copy_from")),
            "area_code": row_data.get("area_code"),
            "end_year": cast_int(row_data.get("end_year")),
            "metadata": self._extract_metadata_updates(row_data),
            "row_label": str(row_idx),
        }

    def _load_existing_scenario(
        self,
        scenario_id: int,
        area_code: Any,
        end_year: Optional[int],
        row_label: str,
        metadata_updates: Dict[str, Any],
        loader: ScenarioLoader,
    ) -> Optional[Session]:
        """Load an existing scenario by ID and apply metadata updates."""
        return loader.load(
            scenario_id, area_code, end_year, row_label, metadata_updates
        )

    def _copy_scenario(
        self,
        scenario_id: int,
        row_label: str,
        metadata_updates: Dict[str, Any],
        loader: ScenarioLoader,
    ) -> Optional[Session]:
        """Create a deep copy of a scenario (no template link)."""
        return loader.copy(scenario_id, row_label, metadata_updates)

    def _copy_with_roles(
        self,
        scenario_id: int,
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Copy a scenario with roles preserved (maintains template link)."""
        try:
            source_scenario = Session.load(scenario_id)
            copy_metadata = metadata_updates.copy()
            copy_metadata["copy_roles"] = True
            return source_scenario.copy(**copy_metadata)
        except Exception as e:
            logger.warning(
                "Failed to copy from parent '%s' for row '%s': %s",
                scenario_id,
                row_label,
                e,
            )
            return None

    def _create_new_scenario(
        self,
        area_code: Any,
        end_year: Optional[int],
        row_label: str,
        metadata_updates: Dict[str, Any],
        update_set: Optional[set[str]] = None,
        loader: Optional[ScenarioLoader] = None,
    ) -> Optional[Session]:
        """Create a brand new scenario."""
        if loader is None:
            return None
        return loader.create_new(area_code, end_year, row_label, metadata_updates)

    def _load_or_create_scenario(
        self,
        scenario_id: Optional[int],
        area_code: Any,
        end_year: Optional[int],
        column_name: str,
        is_no_updates: bool = False,
        **kwargs: Any,
    ) -> Optional[Session]:
        """Load existing scenario or create new one. Passes all available kwargs to Scenario.new for full metadata."""
        if scenario_id is not None:
            try:
                return Session.load(scenario_id)
            except Exception as e:
                logger.warning(
                    "Failed to load scenario %s for column '%s': %s",
                    scenario_id,
                    column_name,
                    e,
                )
                # In no updates mode, if load fails but we have area/year, create locally
                if is_no_updates and area_code and end_year is not None:
                    logger.info(
                        "Creating local-only scenario for column '%s' in no updates mode",
                        column_name,
                    )
                    # Don't return here, fall through to creation logic

        if area_code and end_year is not None:
            try:
                return Session.new(str(area_code), int(end_year), **kwargs)
            except Exception as e:
                logger.warning(
                    "Failed to create scenario for column '%s' (area_code=%s, end_year=%s): %s",
                    column_name,
                    area_code,
                    end_year,
                    e,
                )

        logger.warning(
            "MAIN row '%s' missing required fields for creation (area_code/end_year)",
            column_name,
        )
        return None

    def _extract_metadata_updates(self, column_data: pd.Series[Any]) -> Dict[str, Any]:
        """Extract metadata updates from column data."""
        metadata: Dict[str, Any] = {}

        # Extract boolean fields
        for bool_field in ["private", "keep_compatible"]:
            value = cast_bool(column_data.get(bool_field))
            if value is not None:
                metadata[bool_field] = value

        # Extract string fields
        for field in ["source", "title"]:
            value = column_data.get(field)
            if isinstance(value, str) and value.strip():
                metadata[field] = value.strip()

        return metadata

    def _apply_metadata_to_scenario(
        self, scenario: Session, metadata: Dict[str, Any]
    ) -> None:
        """Apply metadata updates to scenario."""
        if not metadata:
            return

        try:
            scenario.update_metadata(**metadata)
        except Exception as e:
            logger.warning(
                "Failed to update metadata for '%s': %s",
                scenario.identifier(),
                e,
            )

    def _apply_export_configuration(
        self,
        main_df: pd.DataFrame,
        scenarios_by_column: Dict[Any, Session],
        export_config_df: Optional[pd.DataFrame] = None,
    ) -> None:
        """Apply export configuration to all scenarios. Requires EXPORT_CONFIG sheet."""
        try:
            if export_config_df is None:
                logger.error("EXPORT_CONFIG dataframe is None.")
                return

            config = excel_utils.ExportConfigResolver.extract_from_export_config_sheet(
                export_config_df
            )
            if config is None:
                logger.error(
                    "Failed to read export config from EXPORT_CONFIG sheet - did you the EXPORT_CONFIG sheet?"
                )
                return

            for scenario in scenarios_by_column.values():
                try:
                    if hasattr(scenario, "set_export_config"):
                        scenario.set_export_config(config)
                    else:
                        setattr(scenario, "_export_config", config)
                except Exception:
                    logger.warning(
                        f"Failed to set export config for scenario: {scenario}"
                    )
        except Exception as e:
            logger.error(f"Exception in _apply_export_configuration: {e}")

    def _import_scenario_specific_sheets(
        self,
        excel_file: pd.ExcelFile,
        main_df: pd.DataFrame,
        scenarios_by_column: Dict[Any, Session],
        update_set: Optional[set[str]] = None,
    ) -> None:
        """Import scenario-specific sortables and custom curves sheets."""
        sheet_info = excel_utils.extract_scenario_sheet_info(main_df)
        update_set = update_set or set()

        for column_name, scenario in scenarios_by_column.items():
            key = str(column_name)
            info = sheet_info.get(key, {}) if isinstance(sheet_info, dict) else {}

            # Import sortables
            sortables_sheet = info.get("sortables") if isinstance(info, dict) else None
            if (
                isinstance(sortables_sheet, str)
                and sortables_sheet in excel_file.sheet_names
            ):
                self._sortables.import_scenario_specific_sheet(
                    excel_file, sortables_sheet, scenario, update_set
                )

            # Import custom curves
            curves_sheet = info.get("custom_curves") if isinstance(info, dict) else None
            if isinstance(curves_sheet, str) and curves_sheet in excel_file.sheet_names:
                curves_df = excel_utils.parse_excel_sheet(
                    excel_file,
                    curves_sheet,
                    **self._custom_curves.excel_read_kwargs(),
                )
                if curves_df is not None:
                    self._custom_curves.load_from_dataframe(
                        curves_df,
                        scenario,
                        update_set,
                    )

    def _scenarios(self) -> list[Session]:
        """All scenarios we are packing info for across all packs, in insertion order."""
        seen = set()
        result = []
        for pack in self._packs():
            for scenario in pack.scenarios:
                if scenario not in seen:
                    seen.add(scenario)
                    result.append(scenario)
        return result

    def _packs(self) -> Any:
        """Get all pack instances."""
        yield from (
            self._inputs,
            self._sortables,
            self._custom_curves,
            self._hourly_output_curves,
            self._annual_exports,
            self._query_pack,
            self._users,
        )

    def clear(self) -> None:
        """Clear all scenarios from all packs."""
        for pack in self._packs():
            try:
                pack.clear()
            except Exception:
                pass

    def remove_scenario(self, scenario: Session) -> None:
        """Remove a specific scenario from all collections."""
        for pack in self._packs():
            try:
                pack.discard(scenario)
            except Exception:
                pass
