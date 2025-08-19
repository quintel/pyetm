import pandas as pd
import logging
from pathlib import Path
from os import PathLike
from pydantic import BaseModel
from typing import Optional, Dict, Any, Sequence, List
from xlsxwriter import Workbook

from pyetm.models.packables.custom_curves_pack import CustomCurvesPack
from pyetm.models.packables.inputs_pack import InputsPack
from pyetm.models.packables.output_curves_pack import OutputCurvesPack
from pyetm.models.packables.query_pack import QueryPack
from pyetm.models.packables.sortable_pack import SortablePack
from pyetm.models import Scenario
from pyetm.models.export_config import ExportConfig
from pyetm.models.custom_curves import CustomCurves
from pyetm.utils.excel import add_frame

logger = logging.getLogger(__name__)


class ExportConfigResolver:
    """Handles resolution of export configuration from various sources."""

    @staticmethod
    def resolve_boolean(
        explicit_value: Optional[bool], config_value: Optional[bool], default: bool
    ) -> bool:
        """Resolve boolean value from explicit parameter, config, or default."""
        if explicit_value is not None:
            return bool(explicit_value)
        if config_value is not None:
            return bool(config_value)
        return default

    @staticmethod
    def extract_from_main_sheet(
        main_df: pd.DataFrame, scenarios: List[Scenario]
    ) -> Optional[ExportConfig]:
        """Extract export configuration from the first scenario column in main sheet."""
        if main_df.empty or not scenarios:
            return None

        try:
            helper_columns = {"description", "helper", "notes"}
            candidate_series = None

            for col in main_df.columns:
                name = str(col).strip().lower()
                if name in helper_columns or name in {"", "nan"}:
                    continue
                candidate_series = main_df[col]
                break

            if candidate_series is None:
                candidate_series = main_df.iloc[:, 0]

            return ExportConfigResolver._parse_config_from_series(candidate_series)
        except Exception as e:
            logger.exception("Error extracting from main sheet: %s", e)
            return None

    @staticmethod
    def _parse_config_from_series(series: pd.Series) -> "ExportConfig":
        """Parse ExportConfig from a pandas Series (column from main sheet)."""

        def _iter_rows():
            for label, value in zip(series.index, series.values):
                yield str(label).strip().lower(), value

        def _value_after_output(name: str) -> Any:
            target = name.strip().lower()
            seen_output = False
            chosen: Any = None
            for lbl, val in _iter_rows():
                if lbl == "output":
                    seen_output = True
                    continue
                if seen_output and lbl == target:
                    chosen = val
            return chosen

        def _value_any(name: str) -> Any:
            target = name.strip().lower()
            chosen: Any = None
            for lbl, val in _iter_rows():
                if lbl == target:
                    chosen = val
            return chosen

        def get_cell_value(name: str) -> Any:
            val = _value_after_output(name)
            return val if val is not None else _value_any(name)

        def parse_bool(value: Any) -> Optional[bool]:
            """Parse boolean from various formats."""
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                try:
                    return bool(int(value))
                except Exception:
                    return None
            if isinstance(value, str):
                normalized = value.strip().lower()
                if normalized in {"true", "yes", "y", "1"}:
                    return True
                if normalized in {"false", "no", "n", "0"}:
                    return False
            return None

        def parse_bool_field(*names: str) -> Optional[bool]:
            """Return the first non-None boolean parsed from the provided field names."""
            for n in names:
                val = parse_bool(get_cell_value(n))
                if val is not None:
                    return val
            return None

        def parse_carriers(value: Any) -> Optional[List[str]]:
            """Parse comma-separated carrier list."""
            if not isinstance(value, str) or not value.strip():
                return None
            return [carrier.strip() for carrier in value.split(",") if carrier.strip()]

        exports_val = get_cell_value("exports")
        carriers_val = get_cell_value("output_carriers")

        exports_bool = parse_bool(exports_val)
        if exports_bool is True:
            output_carriers = ["electricity", "hydrogen", "heat", "methane"]
        elif exports_bool is False:
            output_carriers = None
        else:
            output_carriers = parse_carriers(carriers_val) or parse_carriers(
                exports_val
            )

        config = ExportConfig(
            include_inputs=parse_bool_field("include_inputs", "inputs"),
            include_sortables=parse_bool_field("include_sortables", "sortables"),
            include_custom_curves=parse_bool_field(
                "include_custom_curves", "custom_curves"
            ),
            include_gqueries=(
                parse_bool_field("include_gqueries", "gquery_results", "gqueries")
            ),
            inputs_defaults=parse_bool(get_cell_value("defaults")),
            inputs_min_max=parse_bool(get_cell_value("min_max")),
            output_carriers=output_carriers,
        )
        return config


class ScenarioPacker(BaseModel):
    """
    Packs one or multiple scenarios for export to dataframes or excel
    """

    # Pack collections
    _custom_curves: CustomCurvesPack = CustomCurvesPack()
    _inputs: InputsPack = InputsPack()
    _sortables: SortablePack = SortablePack()
    _output_curves: OutputCurvesPack = OutputCurvesPack()

    # Scenario management methods
    def add(self, *scenarios):
        """Add scenarios to all packs."""
        self.add_custom_curves(*scenarios)
        self.add_inputs(*scenarios)
        self.add_sortables(*scenarios)
        self.add_output_curves(*scenarios)

    def add_custom_curves(self, *scenarios):
        self._custom_curves.add(*scenarios)

    def add_inputs(self, *scenarios):
        self._inputs.add(*scenarios)

    def add_sortables(self, *scenarios):
        self._sortables.add(*scenarios)

    def add_output_curves(self, *scenarios):
        self._output_curves.add(*scenarios)

    def main_info(self) -> pd.DataFrame:
        """Create main info DataFrame by concatenating scenario dataframes."""
        scenarios = self._scenarios()
        if not scenarios:
            return pd.DataFrame()
        return pd.concat([scenario.to_dataframe() for scenario in scenarios], axis=1)

    def inputs(self, columns="user") -> pd.DataFrame:
        return self._inputs._to_dataframe(columns=columns)

    def gquery_results(self, columns="future") -> pd.DataFrame:
        return QueryPack(scenarios=self._scenarios()).to_dataframe(columns=columns)

    def sortables(self) -> pd.DataFrame:
        return self._sortables.to_dataframe()

    async def custom_curves(self) -> pd.DataFrame:
        """Get custom curves DataFrame asynchronously."""
        return await self._custom_curves.to_dataframe()

    def output_curves(self) -> pd.DataFrame:
        return self._output_curves.to_dataframe()

    def to_excel(
        self,
        path: str,
        *,
        carriers: Optional[Sequence[str]] = None,
        include_inputs: Optional[bool] = None,
        include_sortables: Optional[bool] = None,
        include_custom_curves: Optional[bool] = None,
        include_gqueries: Optional[bool] = None,
        include_output_curves: Optional[bool] = None,
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
            include_output_curves,
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
            self._add_data_sheets(workbook, global_config, resolved_flags)
            self._add_gqueries_sheet(workbook, resolved_flags["include_gqueries"])
        finally:
            workbook.close()

        # Handle output curves separately
        self._export_output_curves_if_needed(
            path, carriers, resolved_flags["include_output_curves"], global_config
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
        include_output_curves: Optional[bool],
    ) -> Dict[str, Any]:
        """Resolve all export flags from parameters and configuration."""
        resolver = ExportConfigResolver()

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
            "include_output_curves": resolver.resolve_boolean(
                include_output_curves,
                (
                    getattr(global_config, "output_carriers", None) is not None
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
        }

    def _add_main_sheet(self, workbook: Workbook):
        """Add main scenario information sheet to workbook."""
        main_df = self._build_excel_main_dataframe()
        if not main_df.empty:
            sanitized_df = self._sanitize_dataframe_for_excel(main_df)
            add_frame(
                name="MAIN",
                frame=sanitized_df,
                workbook=workbook,
                column_width=18,
                scenario_styling=True,
            )

    def _add_data_sheets(
        self,
        workbook: Workbook,
        global_config: Optional[ExportConfig],
        flags: Dict[str, Any],
    ):
        """Add data sheets (inputs, sortables, custom_curves) to workbook."""
        if flags["include_inputs"]:
            self._add_inputs_sheet(
                workbook, flags["inputs_defaults"], flags["inputs_min_max"]
            )

        if flags["include_sortables"]:
            self._add_pack_sheet(workbook, self._sortables)

        if flags["include_custom_curves"]:
            self._add_pack_sheet(workbook, self._custom_curves)

    def _add_inputs_sheet(
        self, workbook: Workbook, include_defaults: bool, include_min_max: bool
    ):
        """Add inputs sheet with proper field handling."""
        try:
            df = self._inputs.build_combined_dataframe(
                include_defaults=include_defaults, include_min_max=include_min_max
            )
            if df is not None and not df.empty:
                self._add_dataframe_to_workbook(workbook, self._inputs.sheet_name, df)
        except Exception as e:
            logger.warning("Failed to build inputs DataFrame: %s", e)
            df = self._inputs._to_dataframe(columns="user")
            if df is not None and not df.empty:
                self._add_dataframe_to_workbook(workbook, self._inputs.sheet_name, df)

    def _add_pack_sheet(self, workbook: Workbook, pack):
        """Add a pack's DataFrame to the workbook."""
        df = pack.to_dataframe()
        if df is not None and not df.empty:
            self._add_dataframe_to_workbook(workbook, pack.sheet_name, df)

    def _add_gqueries_sheet(self, workbook: Workbook, include_gqueries: bool):
        """Add gqueries sheet if requested."""
        if not include_gqueries:
            return

        gquery_pack = QueryPack(scenarios=self._scenarios())
        gqueries_df = gquery_pack.to_dataframe(columns="future")
        if not gqueries_df.empty:
            self._add_dataframe_to_workbook(
                workbook, gquery_pack.output_sheet_name, gqueries_df
            )

    def _export_output_curves_if_needed(
        self,
        main_path: str,
        carriers: Optional[Sequence[str]],
        include_output_curves: bool,
        global_config: Optional[ExportConfig],
    ):
        """Export output curves to separate file if needed."""
        if not include_output_curves:
            return

        # Determine output file path (next to the main workbook)
        base_path = Path(main_path)
        output_path = str(
            base_path.with_name(f"{base_path.stem}_exports{base_path.suffix}")
        )

        # Determine carriers to export
        chosen_carriers = list(carriers) if carriers else None
        if chosen_carriers is None and global_config is not None:
            config_carriers = getattr(global_config, "output_carriers", None)
            chosen_carriers = list(config_carriers) if config_carriers else None

        try:
            self._output_curves.to_excel_per_carrier(output_path, chosen_carriers)
        except Exception as e:
            logger.warning("Failed exporting output curves workbook: %s", e)

    def _add_dataframe_to_workbook(
        self, workbook: Workbook, sheet_name: str, df: pd.DataFrame
    ):
        """Add a DataFrame to the workbook as a new sheet."""
        cleaned_df = df.fillna("").infer_objects(copy=False)
        add_frame(
            name=sheet_name,
            frame=cleaned_df,
            workbook=workbook,
            column_width=18,
            scenario_styling=True,
        )

    @classmethod
    async def from_excel(cls, xlsx_path: PathLike | str) -> "ScenarioPacker":
        """Import scenarios from Excel file."""
        packer = cls()

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

        scenarios_by_column = packer._create_scenarios_from_main(main_df)
        if not scenarios_by_column:
            return packer

        packer._apply_export_configuration(main_df, scenarios_by_column)
        await packer._import_data_sheets(excel_file, main_df, scenarios_by_column)

        return packer

    def _import_main_sheet(self, excel_file: pd.ExcelFile) -> Optional[pd.DataFrame]:
        """Import and validate the main sheet."""
        try:
            main_df = excel_file.parse("MAIN", index_col=0)
            if main_df is None or getattr(main_df, "empty", False):
                return None
            return main_df
        except Exception as e:
            logger.warning("Failed to parse MAIN sheet: %s", e)
            return None

    def _create_scenarios_from_main(self, main_df: pd.DataFrame) -> Dict[str, Scenario]:
        """Create scenarios from main sheet columns."""
        scenarios_by_column = {}

        for column_name in main_df.columns:
            column_str = str(column_name) if column_name is not None else ""
            if column_str.strip().lower() in {"description", "helper", "notes"}:
                continue

            try:
                scenario = self._create_scenario_from_column(
                    column_str, main_df[column_name]
                )
                if scenario is not None:
                    self.add(scenario)
                    scenarios_by_column[column_str] = scenario
            except Exception as e:
                logger.warning(
                    "Failed to set up scenario for column '%s': %s", column_name, e
                )

        return scenarios_by_column

    def _create_scenario_from_column(
        self, column_name: str, column_data: pd.Series
    ) -> Optional[Scenario]:
        """Create a scenario from a main sheet column."""
        scenario_id = self._safe_get_int(column_data.get("scenario_id"))
        area_code = column_data.get("area_code")
        end_year = self._safe_get_int(column_data.get("end_year"))
        metadata_updates = self._extract_metadata_updates(column_data)
        scenario = self._load_or_create_scenario(
            scenario_id, area_code, end_year, column_name, **metadata_updates
        )
        if scenario is None:
            return None

        # Metadata already applied in creation, but if needed, can update again here
        self._apply_metadata_to_scenario(scenario, metadata_updates)

        return scenario

    def _safe_get_int(self, value: Any) -> Optional[int]:
        """Safely convert value to integer."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_get_bool(self, value: Any) -> Optional[bool]:
        """Safely convert value to boolean."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            try:
                return bool(int(value))
            except Exception:
                return None
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "yes", "y", "1"}:
                return True
            if normalized in {"false", "no", "n", "0"}:
                return False
        return None

    def _load_or_create_scenario(
        self,
        scenario_id: Optional[int],
        area_code: Any,
        end_year: Optional[int],
        column_name: str,
        **kwargs,
    ) -> Optional[Scenario]:
        """Load existing scenario or create new one. Passes all available kwargs to Scenario.new for full metadata."""
        if scenario_id is not None:
            try:
                return Scenario.load(scenario_id)
            except Exception as e:
                logger.warning(
                    "Failed to load scenario %s for column '%s': %s",
                    scenario_id,
                    column_name,
                    e,
                )

        if area_code and end_year is not None:
            try:
                return Scenario.new(str(area_code), int(end_year), **kwargs)
            except Exception as e:
                logger.warning(
                    "Failed to create scenario for column '%s' (area_code=%s, end_year=%s): %s",
                    column_name,
                    area_code,
                    end_year,
                    e,
                )

        logger.warning(
            "MAIN column '%s' missing required fields for creation (area_code/end_year)",
            column_name,
        )
        return None

    def _extract_metadata_updates(self, column_data: pd.Series) -> Dict[str, Any]:
        """Extract metadata updates from column data."""
        metadata = {}

        private = self._safe_get_bool(column_data.get("private"))
        if private is not None:
            metadata["private"] = private

        template = self._safe_get_int(column_data.get("template"))
        if template is not None:
            metadata["template"] = template

        for field in ["source", "title"]:
            value = column_data.get(field)
            if isinstance(value, str) and value.strip():
                metadata[field] = value.strip()

        return metadata

    def _apply_metadata_to_scenario(self, scenario: Scenario, metadata: Dict[str, Any]):
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
        self, main_df: pd.DataFrame, scenarios_by_column: Dict[str, Scenario]
    ):
        """Apply export configuration to all scenarios."""
        try:
            config = ExportConfigResolver.extract_from_main_sheet(
                main_df, list(scenarios_by_column.values())
            )
            if config is None:
                return

            for scenario in scenarios_by_column.values():
                try:
                    if hasattr(scenario, "set_export_config"):
                        scenario.set_export_config(config)
                    else:
                        setattr(scenario, "_export_config", config)
                except Exception:
                    pass
        except Exception:
            pass

    async def _import_data_sheets(
        self,
        excel_file: pd.ExcelFile,
        main_df: pd.DataFrame,
        scenarios_by_column: Dict[str, Scenario],
    ):
        """Import all data sheets (inputs, gqueries, sortables, custom curves)."""
        # Build short name mapping for inputs
        short_name_map = self._build_short_name_mapping(main_df, scenarios_by_column)
        self._import_inputs_sheet(excel_file, short_name_map)
        self._import_gqueries_sheet(excel_file)
        await self._import_scenario_specific_sheets(
            excel_file, main_df, scenarios_by_column
        )

    def _build_short_name_mapping(
        self, main_df: pd.DataFrame, scenarios_by_column: Dict[str, Scenario]
    ) -> Dict[str, str]:
        """Build mapping of scenario IDs to short names."""
        sheet_info = self._extract_scenario_sheet_info(main_df)
        short_name_map = {}

        for column_name, scenario in scenarios_by_column.items():
            info = (
                sheet_info.get(column_name, {}) if isinstance(sheet_info, dict) else {}
            )
            short_name = info.get("short_name") if isinstance(info, dict) else None

            if short_name is None or (
                isinstance(short_name, float) and pd.isna(short_name)
            ):
                short_name = str(scenario.identifier())

            short_name_map[str(scenario.id)] = str(short_name)

        return short_name_map

    def _import_inputs_sheet(
        self, excel_file: pd.ExcelFile, short_name_map: Dict[str, str]
    ):
        """Import inputs sheet - delegate to InputsPack."""
        try:
            slider_df = excel_file.parse(InputsPack.sheet_name, header=None)
            if slider_df is not None and not slider_df.empty:
                self._inputs.set_scenario_short_names(short_name_map)
                self._inputs.from_dataframe(slider_df)
        except Exception as e:
            logger.warning("Failed to import SLIDER_SETTINGS: %s", e)

    def _import_gqueries_sheet(self, excel_file: pd.ExcelFile):
        """Import gqueries sheet - delegate to QueryPack."""
        for sheet_name in ("GQUERIES", QueryPack.sheet_name):
            if sheet_name in excel_file.sheet_names:
                try:
                    gqueries_df = excel_file.parse(sheet_name, header=None)
                    if gqueries_df is not None and not gqueries_df.empty:
                        query_pack = QueryPack(scenarios=self._scenarios())
                        query_pack.from_dataframe(gqueries_df)
                        return
                except Exception as e:
                    logger.warning("Failed to import GQUERIES: %s", e)

    async def _import_scenario_specific_sheets(
        self,
        excel_file: pd.ExcelFile,
        main_df: pd.DataFrame,
        scenarios_by_column: Dict[str, Scenario],
    ):
        """Import scenario-specific sortables and custom curves sheets."""
        sheet_info = self._extract_scenario_sheet_info(main_df)

        for column_name, scenario in scenarios_by_column.items():
            info = (
                sheet_info.get(column_name, {}) if isinstance(sheet_info, dict) else {}
            )

            # Import sortables (can stay sync)
            sortables_sheet = info.get("sortables") if isinstance(info, dict) else None
            if (
                isinstance(sortables_sheet, str)
                and sortables_sheet in excel_file.sheet_names
            ):
                try:
                    sortables_df = excel_file.parse(sortables_sheet, header=None)
                    self._process_single_scenario_sortables(scenario, sortables_df)
                except Exception as e:
                    logger.warning(
                        "Failed to process SORTABLES sheet '%s' for '%s': %s",
                        sortables_sheet,
                        scenario.identifier(),
                        e,
                    )

            # Import custom curves (now async)
            curves_sheet = info.get("custom_curves") if isinstance(info, dict) else None
            if isinstance(curves_sheet, str) and curves_sheet in excel_file.sheet_names:
                try:
                    curves_df = excel_file.parse(curves_sheet, header=None)
                    await self._process_single_scenario_curves(scenario, curves_df)
                except Exception as e:
                    logger.warning(
                        "Failed to process CUSTOM_CURVES sheet '%s' for '%s': %s",
                        curves_sheet,
                        scenario.identifier(),
                        e,
                    )

    def _scenarios(self) -> set[Scenario]:
        """All scenarios we are packing info for across all packs."""
        all_scenarios = set()
        for pack in self._get_all_packs():
            scenarios = getattr(pack, "scenarios", None)
            if scenarios:
                if isinstance(scenarios, set):
                    all_scenarios.update(scenarios)
                else:
                    try:
                        all_scenarios.update(set(scenarios))
                    except Exception:
                        pass
        return all_scenarios

    def _get_all_packs(self):
        """Get all pack instances."""
        return [self._inputs, self._sortables, self._custom_curves, self._output_curves]

    def clear(self):
        """Clear all scenarios from all packs."""
        for pack in self._get_all_packs():
            try:
                pack.clear()
            except Exception:
                pass

    def remove_scenario(self, scenario: Scenario):
        """Remove a specific scenario from all collections."""
        for pack in self._get_all_packs():
            try:
                pack.discard(scenario)
            except Exception:
                pass

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of what's in the packer."""
        summary = {"total_scenarios": len(self._scenarios())}
        for pack in self._get_all_packs():
            try:
                summary.update(pack.summary())
            except Exception:
                pass
        summary["scenario_ids"] = sorted(
            [getattr(s, "id", None) for s in self._scenarios()]
        )
        return summary

    # Excel and DataFrame processing methods - refactored for consistency
    def _build_excel_main_dataframe(self) -> pd.DataFrame:
        """Build a MAIN sheet DataFrame for Excel export with proper ordering and labeling."""
        main_df = self.main_info()
        if main_df is None or main_df.empty:
            return pd.DataFrame()

        # Apply preferred field ordering
        ordered_df = self._apply_field_ordering(main_df)

        # Apply scenario column labeling
        labeled_df = self._apply_scenario_column_labels(ordered_df)

        return labeled_df

    def _apply_field_ordering(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply preferred field ordering to DataFrame rows."""
        preferred_fields = [
            "title",
            "description",
            "scenario_id",
            "template",
            "area_code",
            "start_year",
            "end_year",
            "keep_compatible",
            "private",
            "source",
            "url",
            "version",
            "created_at",
            "updated_at",
        ]

        present_fields = [field for field in preferred_fields if field in df.index]
        remaining_fields = [field for field in df.index if field not in present_fields]
        ordered_fields = present_fields + remaining_fields

        ordered_df = df.reindex(index=ordered_fields)
        ordered_df.index.name = "scenario"
        return ordered_df

    def _apply_scenario_column_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply human-readable labels to scenario columns."""
        try:
            scenarios = list(self._scenarios())
            column_rename_map = self._build_column_rename_map(scenarios, df.columns)

            if column_rename_map:
                return df.rename(columns=column_rename_map)
            return df
        except Exception:
            # If renaming fails, return original DataFrame
            return df

    def _build_column_rename_map(
        self, scenarios: List[Scenario], columns
    ) -> Dict[Any, str]:
        """Build mapping of column IDs to human-readable labels."""
        rename_map = {}
        scenarios_by_id = {str(getattr(s, "id", "")): s for s in scenarios}

        for column in columns:
            matched_scenario = self._find_matching_scenario(
                column, scenarios, scenarios_by_id
            )
            if matched_scenario is not None:
                label = self._get_scenario_display_label(matched_scenario, column)
                rename_map[column] = label

        return rename_map

    def _find_matching_scenario(
        self, column, scenarios: List[Scenario], scenarios_by_id: Dict[str, Scenario]
    ) -> Optional[Scenario]:
        """Find scenario matching the given column identifier."""
        # Try exact ID match first
        for scenario in scenarios:
            if column == getattr(scenario, "id", None):
                return scenario

        # Try string ID match as fallback
        return scenarios_by_id.get(str(column))

    def _get_scenario_display_label(self, scenario: Scenario, fallback_column) -> str:
        """Get display label for scenario, with fallbacks."""
        try:
            if hasattr(scenario, "identifier"):
                return scenario.identifier()
        except Exception:
            pass

        # Try title attribute
        title = getattr(scenario, "title", None)
        if title:
            return title

        # Try ID attribute
        scenario_id = getattr(scenario, "id", None)
        if scenario_id:
            return str(scenario_id)

        # Final fallback
        return str(fallback_column)

    def _sanitize_dataframe_for_excel(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert DataFrame to Excel-compatible format."""
        if df is None or df.empty:
            return pd.DataFrame()

        sanitized_df = df.copy()

        # Sanitize index and columns
        sanitized_df.index = sanitized_df.index.map(self._sanitize_excel_value)
        sanitized_df.columns = [
            self._sanitize_excel_value(col) for col in sanitized_df.columns
        ]

        # Sanitize cell values
        sanitized_df = sanitized_df.map(self._sanitize_excel_value)

        return sanitized_df

    def _sanitize_excel_value(self, value: Any) -> str:
        """Convert a single value to Excel-safe format."""
        if value is None:
            return ""

        if isinstance(value, (str, int, float, bool)):
            return value

        # Handle datetime objects
        if self._is_datetime_like(value):
            try:
                return str(value)
            except Exception:
                return ""

        # Generic fallback
        try:
            return str(value)
        except Exception:
            return ""

    def _is_datetime_like(self, value: Any) -> bool:
        """Check if value is a datetime-like object."""
        import datetime as dt

        return isinstance(value, (pd.Timestamp, dt.datetime, dt.date))

    def _extract_scenario_sheet_info(
        self, main_df: pd.DataFrame
    ) -> Dict[str, Dict[str, str]]:
        """Extract sheet information for each scenario from main DataFrame."""
        if isinstance(main_df, pd.Series):
            return self._extract_single_scenario_sheet_info(main_df)
        else:
            return self._extract_multiple_scenario_sheet_info(main_df)

    def _extract_single_scenario_sheet_info(
        self, series: pd.Series
    ) -> Dict[str, Dict[str, str]]:
        """Extract sheet info for single scenario (Series case)."""
        identifier = str(series.name)

        return {
            identifier: {
                "short_name": self._get_safe_value(series, "short_name", identifier),
                "sortables": self._get_value_before_output(series, "sortables"),
                "custom_curves": self._get_value_before_output(series, "custom_curves"),
            }
        }

    def _extract_multiple_scenario_sheet_info(
        self, df: pd.DataFrame
    ) -> Dict[str, Dict[str, str]]:
        """Extract sheet info for multiple scenarios (DataFrame case)."""
        scenario_sheets = {}

        for identifier in df.columns:
            column_data = df[identifier]
            scenario_sheets[str(identifier)] = {
                "short_name": self._get_safe_value(
                    column_data, "short_name", str(identifier)
                ),
                "sortables": self._get_value_before_output(column_data, "sortables"),
                "custom_curves": self._get_value_before_output(
                    column_data, "custom_curves"
                ),
            }

        return scenario_sheets

    def _get_safe_value(self, series: pd.Series, key: str, default: str) -> str:
        """Safely get value from series with default fallback."""
        value = series.get(key)
        if pd.notna(value):
            return str(value)
        return default

    def _get_value_before_output(self, series: pd.Series, key: str) -> Optional[str]:
        """Get value from series, but only if it appears before 'output' section."""
        seen_output = False

        for label, value in zip(series.index, series.values):
            normalized_label = str(label).strip().lower()

            if normalized_label == "output":
                seen_output = True

            if normalized_label == key and not seen_output:
                return value if pd.notna(value) else None

        return None

    def _process_single_scenario_sortables(self, scenario: Scenario, df: pd.DataFrame):
        """Process sortables data for a single scenario."""
        normalized_data = self._normalize_sheet(
            df,
            helper_names={"sortables", "hour", "index"},
            reset_index=True,
            rename_map={"heat_network": "heat_network_lt"},
        )

        if normalized_data is None or normalized_data.empty:
            return

        self._apply_sortables_to_scenario(scenario, normalized_data)

    def _apply_sortables_to_scenario(self, scenario: Scenario, data: pd.DataFrame):
        """Apply sortables data to scenario with error handling."""
        try:
            scenario.set_sortables_from_dataframe(data)
            self._log_scenario_warnings(scenario, "_sortables", "Sortables")
        except Exception as e:
            logger.warning(
                "Failed processing sortables for '%s': %s", scenario.identifier(), e
            )

    async def _process_single_scenario_curves(
        self, scenario: Scenario, df: pd.DataFrame
    ):
        """Process custom curves data for a single scenario."""
        normalized_data = self._normalize_sheet(
            df,
            helper_names={"curves", "custom_curves", "hour", "index"},
            reset_index=True,
        )

        if normalized_data is None or normalized_data.empty:
            return

        await self._apply_custom_curves_to_scenario(scenario, normalized_data)

    async def _apply_custom_curves_to_scenario(
        self, scenario: Scenario, data: pd.DataFrame
    ):
        """Apply custom curves to scenario with validation and error handling."""
        try:
            curves = CustomCurves._from_dataframe(data, scenario_id=scenario.id)

            # Log processing warnings
            curves.log_warnings(
                logger,
                prefix=f"Custom curves warning for '{scenario.identifier()}'",
            )

            # Validate curves and log validation issues
            self._validate_and_log_curves(curves, scenario)

            await scenario.update_custom_curves(curves)

        except Exception as e:
            self._handle_curves_processing_error(scenario, e)

    def _validate_and_log_curves(self, curves: CustomCurves, scenario: Scenario):
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

    def _normalize_sheet(
        self,
        df: pd.DataFrame,
        *,
        helper_names: set[str],
        reset_index: bool = True,
        rename_map: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """Normalize a sheet by finding headers and cleaning data."""
        if df is None:
            return pd.DataFrame()

        df = df.dropna(how="all")
        if df.empty:
            return df

        header_position = self._find_first_non_empty_row(df)
        if header_position is None:
            return pd.DataFrame()

        # Extract header and data
        header = df.iloc[header_position].astype(str).map(str.strip)
        data = df.iloc[header_position + 1 :].copy()
        data.columns = header.values

        # Keep only non-helper columns
        columns_to_keep = [
            col for col in data.columns if not self._is_helper_column(col, helper_names)
        ]
        data = data[columns_to_keep]

        # Apply column renaming if provided
        if rename_map:
            data = data.rename(columns=rename_map)

        if reset_index:
            data.reset_index(drop=True, inplace=True)

        return data

    def _handle_curves_processing_error(self, scenario: Scenario, error: Exception):
        """Handle errors during curves processing."""
        logger.warning(
            "Failed processing custom curves for '%s': %s", scenario.identifier(), error
        )

    def _log_scenario_warnings(
        self, scenario: Scenario, attribute_name: str, context: str
    ):
        """Log warnings from scenario attributes if available."""
        try:
            attribute = getattr(scenario, attribute_name, None)
            if attribute is not None and hasattr(attribute, "log_warnings"):
                attribute.log_warnings(
                    logger,
                    prefix=f"{context} warning for '{scenario.identifier()}'",
                )
        except Exception:
            # Warning logging failures should not interrupt processing
            pass

    def _find_first_non_empty_row(self, df: pd.DataFrame) -> Optional[int]:
        """Find the first row that contains non-empty data."""
        if df is None:
            return None

        for index, (_, row) in enumerate(df.iterrows()):
            try:
                if not row.isna().all():
                    return index
            except Exception:
                # Fallback check for non-standard empty values
                if any(value not in (None, "", float("nan")) for value in row):
                    return index

        return None

    def _is_helper_column(self, column_name: Any, helper_names: set[str]) -> bool:
        """Check if a column is a helper column that should be ignored."""
        if not isinstance(column_name, str):
            return True

        normalized_name = column_name.strip().lower()
        return normalized_name in (helper_names or set()) or normalized_name in {
            "",
            "nan",
        }
