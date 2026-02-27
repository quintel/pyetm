import pandas as pd
import logging
from pathlib import Path
from os import PathLike
from pydantic import BaseModel
from typing import Optional, Dict, Any, Sequence, List, Union, TYPE_CHECKING
from xlsxwriter import Workbook

from pyetm.models.packables.inputs_pack import InputsPack
from pyetm.models.packables.hourly_output_curves_pack import HourlyOutputCurvesPack
from pyetm.models.packables.annual_exports_pack import AnnualExportsPack
from pyetm.models.packables.query_pack import QueryPack
from pyetm.models.packables.sortable_pack import SortablePack
from pyetm.models.packables.custom_curves_pack import CustomCurvesPack
from pyetm.models.packables.users_pack import UsersPack
from pyetm.models import Session
from pyetm.models.export_config import ExportConfig
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
    def add(self, *scenarios):
        """Add scenarios to all packs. Supports Sessions and Scenarios"""
        self.add_custom_curves(*scenarios)
        self.add_inputs(*scenarios)
        self.add_sortables(*scenarios)
        self.add_hourly_output_curves(*scenarios)
        self.add_annual_exports(*scenarios)
        self._query_pack.add(*scenarios)
        self._users.add(*scenarios)

    def add_custom_curves(self, *scenarios):
        self._custom_curves.add(*scenarios)

    def add_inputs(self, *scenarios):
        self._inputs.add(*scenarios)

    def add_sortables(self, *scenarios):
        self._sortables.add(*scenarios)

    def add_hourly_output_curves(self, *scenarios):
        self._hourly_output_curves.add(*scenarios)

    def add_annual_exports(self, *scenarios):
        self._annual_exports.add(*scenarios)

    def main_info(self) -> pd.DataFrame:
        """Create main info DataFrame by concatenating scenario dataframes."""
        scenarios = self._scenarios()
        if not scenarios:
            return pd.DataFrame()
        return pd.concat([scenario._to_dataframe() for scenario in scenarios], axis=1)

    def inputs(self, fields="user") -> pd.DataFrame:
        return self._inputs.to_dataframe(fields=fields)

    def gquery_results(self, columns="future") -> pd.DataFrame:
        return self._query_pack.to_dataframe(columns=columns)

    def sortables(self) -> pd.DataFrame:
        return self._sortables.to_dataframe()

    def custom_curves(
        self, as_dict: bool = False, curves: Optional[Sequence[str]] = None
    ) -> Union[pd.DataFrame, dict[str, dict[str, pd.Series]]]:
        """
        Get custom curves for all scenarios.

        Args:
            as_dict: If True, returns dict[curve_name, dict[scenario_id, Series]].
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
            validate_hourly_curve_names(curves)
            return list(curves)

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
        if exports is not None:
            exports = validate_export_names(exports)
        return self._annual_exports.to_dict_per_export(exports=exports)

    def couplings(self) -> pd.DataFrame:
        if len(self._scenarios()) == 0:
            return pd.DataFrame()

        return pd.concat(
            [
                scenario.couplings.to_series(scenario.identifier())
                for scenario in self._scenarios()
            ],
            axis=1,
        )

    def add_queries(self, gquery_keys: List[str]):
        self._query_pack.add_queries(gquery_keys)

    def to_excel(
        self,
        path: str,
        *,
        carriers: Optional[Sequence[str]] = None,
        include_inputs: Optional[bool] = None,
        include_sortables: Optional[bool] = None,
        include_custom_curves: Optional[bool] = None,
        include_gqueries: Optional[bool] = None,
        include_hourly_output_curves: Optional[bool] = None,
        include_input_details: Optional[bool] = None,
        include_users: Optional[bool] = None,
        include_annual_exports: Optional[Sequence[str]] = None,
    ):
        """Export scenarios to Excel file."""
        if not self._scenarios():
            raise ValueError("Packer was empty, nothing to export")

        global_config = self._get_global_export_config()
        resolved_flags = self._resolve_export_flags(
            global_config,
            include_inputs,
            include_sortables,
            include_custom_curves,
            include_gqueries,
            include_hourly_output_curves,
            include_users,
            include_annual_exports,
        )

        # Ensure destination directory exists
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        # Create and populate workbook
        workbook = Workbook(path)
        try:
            self._add_main_sheet(workbook)
            self._add_data_sheets(workbook, resolved_flags)

            if resolved_flags["include_gqueries"]:
                self._query_pack.add_to_workbook(workbook)

            if include_input_details:
                self._inputs.add_to_workbook(
                    workbook,
                    include_defaults=True,
                    include_min_max=True,
                    sheet_name="INPUT_DETAILS",
                )
        finally:
            workbook.close()

        # Handle output curves separately
        self._export_hourly_output_curves_if_needed(
            path,
            carriers,
            resolved_flags["include_hourly_output_curves"],
            global_config,
        )

        # Handle annual exports separately
        self._export_annual_exports_if_needed(
            path,
            resolved_flags.get("include_annual_exports"),
            global_config,
        )

    def _get_global_export_config(self) -> Optional[ExportConfig]:
        """Get global export configuration from first scenario that has one."""
        for scenario in self._scenarios():
            config = getattr(scenario, "_export_config", None)
            if config is not None:
                return config
        return None

    def _resolve_export_flags(
        self,
        global_config: Optional[ExportConfig],
        include_inputs: Optional[bool],
        include_sortables: Optional[bool],
        include_custom_curves: Optional[bool],
        include_gqueries: Optional[bool],
        include_hourly_output_curves: Optional[bool],
        include_users: Optional[bool],
        include_annual_exports: Optional[Sequence[str]] = None,
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
            "include_hourly_output_curves": resolver.resolve_boolean(
                include_hourly_output_curves,
                (
                    (getattr(global_config, "output_carriers", None) is not None)
                    if global_config
                    else None
                ),
                False,
            ),
            "inputs_defaults": (
                bool(getattr(global_config, "inputs_defaults", False))
                if global_config
                else False
            ),
            "inputs_min_max": (
                bool(getattr(global_config, "inputs_min_max", False))
                if global_config
                else False
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

    def _add_main_sheet(self, workbook: Workbook):
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

    def _add_data_sheets(self, workbook: Workbook, flags: Dict[str, Any]):
        """Add data sheets to workbook based on flags."""
        if flags["include_inputs"]:
            self._inputs.add_to_workbook(
                workbook,
                include_defaults=flags["inputs_defaults"],
                include_min_max=flags["inputs_min_max"],
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
        carriers: Optional[Sequence[str]],
        include_hourly_output_curves: bool,
        global_config: Optional[ExportConfig],
    ):
        """Export output curves to separate file if needed."""
        if not include_hourly_output_curves:
            return

        # Determine output file path
        base_path = Path(main_path)
        output_path = str(
            base_path.with_name(
                f"{base_path.stem}_hourly_output_curves{base_path.suffix}"
            )
        )

        # Determine carriers to export
        chosen_carriers = list(carriers) if carriers else None
        if chosen_carriers is None and global_config is not None:
            config_carriers = getattr(global_config, "output_carriers", None)
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
    ):
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
        cls, xlsx_path: PathLike | str, update: bool | List[str] = False
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

        # Resolve default location: if a relative path/filename is provided and the
        # file does not exist at that location, look for it in the project /inputs dir.
        path = Path(xlsx_path)
        if not path.is_absolute() and not path.exists():

            def _find_root_with(dir_name: str) -> Path:
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

            root = _find_root_with("inputs")
            relative = path if str(path.parent) != "." else Path(path.name)
            candidate = root / "inputs" / relative
            if candidate.exists():
                path = candidate

        try:
            excel_file = pd.ExcelFile(str(path))
        except Exception as e:
            logger.warning("Could not open Excel file '%s': %s", xlsx_path, e)
            return packer

        # Import main sheet and create scenarios
        main_df = packer._import_main_sheet(excel_file)
        if main_df is None:
            return packer

        scenarios_by_column = packer._create_scenarios_from_main(main_df, update_set)
        if not scenarios_by_column:
            return packer

        # Require EXPORT_CONFIG sheet
        if "EXPORT_CONFIG" not in excel_file.sheet_names:
            logger.error("EXPORT_CONFIG sheet is required but not found in Excel file.")
            return packer
        try:
            export_config_df = excel_file.parse("EXPORT_CONFIG")
        except Exception as e:
            logger.error("Could not parse EXPORT_CONFIG sheet: %s", e)
            return packer

        packer._apply_export_configuration(
            main_df, scenarios_by_column, export_config_df
        )

        packer._inputs.import_from_excel(
            excel_file, main_df, scenarios_by_column, update_set
        )

        # Queries

        packer._query_pack.load_from_dataframe(
            excel_utils.parse_excel_sheet(
                excel_file,
                packer._query_pack.sheet_name,
                **packer._query_pack.excel_read_kwargs(),
            )
        )

        packer._import_scenario_specific_sheets(
            excel_file, main_df, scenarios_by_column, update_set
        )

        # Import users sheet if present
        packer._users.import_from_excel(
            excel_file, main_df, scenarios_by_column, update_set
        )

        return packer

    def _import_main_sheet(self, excel_file: pd.ExcelFile) -> Optional[pd.DataFrame]:
        """Import and validate the main sheet."""
        try:
            main_df = excel_file.parse("MAIN")
            if main_df is None or getattr(main_df, "empty", False):
                return None
            return main_df
        except Exception as e:
            logger.warning("Failed to parse MAIN sheet: %s", e)
            return None

    def _create_scenarios_from_main(
        self, main_df: pd.DataFrame, update_set: set[str] = None
    ) -> Dict[str, Session]:
        """Create scenarios from main sheet rows."""
        scenarios_by_row = {}
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
        self, row_idx, row_data: pd.Series, update_set: set[str] = None
    ) -> Optional[Session]:
        """
        Create a scenario from a main sheet row.

        Uses per-row 'session' column to determine loader:
        - session=True: SessionLoader (IDs refer to ETEngine Sessions)
        - session=False: SavedScenarioLoader (IDs refer to MyETM SavedScenarios)
        - Default: False (SavedScenarioLoader)
        """
        is_session = cast_bool(row_data.get("session"))
        if is_session is None:
            is_session = False
        loader = SessionLoader(self) if is_session else SavedScenarioLoader(self)

        scenario_id = cast_int(row_data.get("scenario_id"))
        parent = cast_int(row_data.get("parent"))
        copy_from = cast_int(row_data.get("copy_from"))
        area_code = row_data.get("area_code")
        end_year = cast_int(row_data.get("end_year"))
        metadata_updates = self._extract_metadata_updates(row_data)
        row_label = str(row_idx)

        # Load existing scenario if scenario_id is provided
        if scenario_id:
            return self._load_existing_scenario(
                scenario_id,
                area_code,
                end_year,
                row_label,
                metadata_updates,
                loader,
            )

        # Copy if copy_from is provided
        if copy_from:
            return self._copy_scenario(copy_from, row_label, metadata_updates, loader)

        # Copy with roles if parent is provided
        if parent:
            return self._copy_with_roles(parent, row_label, metadata_updates)

        # Create new scenario
        return self._create_new_scenario(
            area_code, end_year, row_label, metadata_updates, update_set, loader
        )

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
        update_set: set[str] = None,
        loader: ScenarioLoader = None,
    ) -> Optional[Session]:
        """Create a brand new scenario."""
        return loader.create_new(area_code, end_year, row_label, metadata_updates)

    def _load_or_create_scenario(
        self,
        scenario_id: Optional[int],
        area_code: Any,
        end_year: Optional[int],
        column_name: str,
        is_no_updates: bool = False,
        **kwargs,
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

    def _extract_metadata_updates(self, column_data: pd.Series) -> Dict[str, Any]:
        """Extract metadata updates from column data."""
        metadata = {}

        private = cast_bool(column_data.get("private"))
        if private is not None:
            metadata["private"] = private

        for field in ["source", "title"]:
            value = column_data.get(field)
            if isinstance(value, str) and value.strip():
                metadata[field] = value.strip()

        return metadata

    def _apply_metadata_to_scenario(self, scenario: Session, metadata: Dict[str, Any]):
        """Apply metadata updates to scenario."""
        if not metadata:
            return

        try:
            scenario.update_metadata(**metadata)
        except Exception as e:
            logger.warning(
                "Failed to update metadata for '%s': %s", scenario.identifier(), e
            )

    def _apply_export_configuration(
        self,
        main_df: pd.DataFrame,
        scenarios_by_column: Dict[str, Session],
        export_config_df: Optional[pd.DataFrame] = None,
    ):
        """Apply export configuration to all scenarios. Requires EXPORT_CONFIG sheet."""
        try:
            config = excel_utils.ExportConfigResolver.extract_from_export_config_sheet(
                export_config_df
            )
            if config is None:
                logger.error("Failed to read export config from EXPORT_CONFIG sheet.")
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
        scenarios_by_column: Dict[str, Session],
        update_set: set[str] = None,
    ):
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
                self._custom_curves.load_from_dataframe(
                    excel_utils.parse_excel_sheet(
                        excel_file,
                        curves_sheet,
                        **self._custom_curves.excel_read_kwargs(),
                    ),
                    scenario,
                    update_set,
                )

    def _scenarios(self) -> set[Session]:
        """All scenarios we are packing info for across all packs."""
        return set().union(*[pack.scenarios for pack in self._packs()])

    def _packs(self):
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

    def clear(self):
        """Clear all scenarios from all packs."""
        for pack in self._packs():
            try:
                pack.clear()
            except Exception:
                pass

    def remove_scenario(self, scenario: Session):
        """Remove a specific scenario from all collections."""
        for pack in self._packs():
            try:
                pack.discard(scenario)
            except Exception:
                pass
