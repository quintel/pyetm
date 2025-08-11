import pandas as pd
import logging
from os import PathLike
from pydantic import BaseModel
from typing import Optional, Dict, Any
from xlsxwriter import Workbook

from pyetm.models.packables.custom_curves_pack import CustomCurvesPack
from pyetm.models.packables.inputs_pack import InputsPack
from pyetm.models.packables.output_curves_pack import OutputCurvesPack
from pyetm.models.packables.query_pack import QueryPack
from pyetm.models.packables.sortable_pack import SortablePack
from pyetm.models import Scenario
from pyetm.models.custom_curves import CustomCurves
from pyetm.utils.excel import add_frame_with_scenario_styling

logger = logging.getLogger(__name__)


class ScenarioPacker(BaseModel):
    """
    Packs one or multiple scenarios for export to dataframes or excel
    """

    # To avoid keeping all in memory, the packer only remembers which scenarios
    # to pack what info for later
    _custom_curves: "CustomCurvesPack" = CustomCurvesPack()
    _inputs: "InputsPack" = InputsPack()
    _sortables: "SortablePack" = SortablePack()
    _output_curves: "OutputCurvesPack" = OutputCurvesPack()

    # Setting up a packer

    def add(self, *scenarios):
        """
        Shorthand method for adding all extractions for the scenario
        """
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

    # DataFrame outputs

    def main_info(self) -> pd.DataFrame:
        """Create main info DataFrame"""
        if len(self._scenarios()) == 0:
            return pd.DataFrame()

        return pd.concat(
            [scenario.to_dataframe() for scenario in self._scenarios()], axis=1
        )

    def inputs(self, columns="user") -> pd.DataFrame:
        return self._inputs.to_dataframe(columns=columns)

    def gquery_results(self, columns="future") -> pd.DataFrame:
        return QueryPack(scenarios=self._scenarios()).to_dataframe(columns=columns)

    def sortables(self) -> pd.DataFrame:
        return self._sortables.to_dataframe()

    def custom_curves(self) -> pd.DataFrame:
        return self._custom_curves.to_dataframe()

    def output_curves(self) -> pd.DataFrame:
        return self._output_curves.to_dataframe()

    def to_excel(self, path: str):
        if len(self._scenarios()) == 0:
            raise ValueError("Packer was empty, nothing to export")

        workbook = Workbook(path)

        # Main info sheet (handled separately as it doesn't use a pack)
        df = self.main_info()
        if not df.empty:
            df_filled = df.fillna("").infer_objects(copy=False)
            add_frame_with_scenario_styling(
                name="MAIN",
                frame=df_filled,
                workbook=workbook,
                column_width=18,
                scenario_styling=True,
            )

        for pack in self.all_pack_data():
            df = pack.to_dataframe()
            if not df.empty:
                df_filled = df.fillna("").infer_objects(copy=False)
                add_frame_with_scenario_styling(
                    name=pack.sheet_name,
                    frame=df_filled,
                    workbook=workbook,
                    column_width=18,
                    scenario_styling=True,
                )

        workbook.close()

    def _scenarios(self) -> set["Scenario"]:
        """
        All scenarios we are packing info for: for these we need to insert
        their metadata
        """
        return set.union(*map(set, (pack.scenarios for pack in self.all_pack_data())))

    def all_pack_data(self):
        """Yields each subpack"""
        # TODO: we can also do this with model dump?
        yield self._inputs
        yield self._sortables
        yield self._custom_curves
        yield self._output_curves

    def clear(self):
        """Clear all scenarios"""
        for pack in self.all_pack_data():
            pack.clear()

    def remove_scenario(self, scenario: "Scenario"):
        """Remove a specific scenario from all collections"""
        for pack in self.all_pack_data():
            pack.discard(scenario)

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of what's in the packer"""
        summary = {"total_scenarios": len(self._scenarios())}

        for pack in self.all_pack_data():
            summary.update(pack.summary())

        summary["scenario_ids"] = sorted([s.id for s in self._scenarios()])

        return summary

    #  Create stuff

    def _extract_scenario_sheet_info(
        self, main_df: pd.DataFrame
    ) -> Dict[str, Dict[str, str]]:
        """Extract sortables and custom_curves sheet names for each scenario from MAIN sheet.
        Also extracts short_name mapping for parameter sheet identification.
        Returns dict with scenario identifier as key and info dict containing:
        - 'short_name': short name for parameter sheet identification
        - 'sortables': sortables sheet name
        - 'custom_curves': custom curves sheet name
        """
        scenario_sheets = {}

        if isinstance(main_df, pd.Series):
            # Single scenario
            identifier = str(main_df.name)
            short_name = main_df.get("short_name")
            sortables_sheet = main_df.get("sortables")
            custom_curves_sheet = main_df.get("custom_curves")

            scenario_sheets[identifier] = {
                "short_name": short_name if pd.notna(short_name) else identifier,
                "sortables": sortables_sheet if pd.notna(sortables_sheet) else None,
                "custom_curves": (
                    custom_curves_sheet if pd.notna(custom_curves_sheet) else None
                ),
            }
        else:
            # Multiple scenarios
            for identifier in main_df.columns:
                col_data = main_df[identifier]
                short_name = col_data.get("short_name")
                sortables_sheet = col_data.get("sortables")
                custom_curves_sheet = col_data.get("custom_curves")

                scenario_sheets[str(identifier)] = {
                    "short_name": (
                        short_name if pd.notna(short_name) else str(identifier)
                    ),
                    "sortables": sortables_sheet if pd.notna(sortables_sheet) else None,
                    "custom_curves": (
                        custom_curves_sheet if pd.notna(custom_curves_sheet) else None
                    ),
                }

        return scenario_sheets

    def _process_single_scenario_sortables(
        self, scenario: "Scenario", df: pd.DataFrame
    ):
        """Process sortables for a single scenario from its dedicated sheet.
        Simplified parsing since there's only one scenario per sheet.
        Tolerates a leading index column (e.g. 'hour' or blank), and maps
        'heat_network' → 'heat_network_lt' for convenience.
        """
        # Drop completely empty rows
        df = df.dropna(how="all")
        if df.empty:
            return

        # Find the first non-empty row -> header with sortable names
        non_empty_idx = [
            i for i, (_, r) in enumerate(df.iterrows()) if not r.isna().all()
        ]
        if not non_empty_idx:
            return

        header_pos = non_empty_idx[0]
        header = df.iloc[header_pos].astype(str).map(lambda s: s.strip())
        data = df.iloc[header_pos + 1 :].copy()

        # Set column names from header
        data.columns = header.values

        # Helper to detect columns to drop
        def _is_empty_or_helper(col_name: Any) -> bool:
            if not isinstance(col_name, str):
                return True
            name = col_name.strip().lower()
            return name in {"", "nan", "sortables", "hour", "index"}

        # Drop empty/helper columns
        keep_cols = [col for col in data.columns if not _is_empty_or_helper(col)]
        data = data[keep_cols]

        # Map bare 'heat_network' to 'heat_network_lt' if present
        if "heat_network" in data.columns and "heat_network_lt" not in data.columns:
            data = data.rename(columns={"heat_network": "heat_network_lt"})

        # Reset index to numeric starting at 0
        data.reset_index(drop=True, inplace=True)

        # Apply to scenario
        scenario.set_sortables_from_dataframe(data)

    def _process_single_scenario_curves(self, scenario: "Scenario", df: pd.DataFrame):
        """Process custom curves for a single scenario from its dedicated sheet.
        Simplified parsing since there's only one scenario per sheet.
        Tolerates a leading index column (e.g. 'hour' or blank).
        """
        # Drop completely empty rows
        df = df.dropna(how="all")
        if df.empty:
            return

        # Find the first non-empty row -> header with curve keys
        non_empty_idx = [
            i for i, (_, r) in enumerate(df.iterrows()) if not r.isna().all()
        ]
        if not non_empty_idx:
            return

        header_pos = non_empty_idx[0]
        header = df.iloc[header_pos].astype(str).map(lambda s: s.strip())
        data = df.iloc[header_pos + 1 :].copy()

        # Set column names from header
        data.columns = header.values

        # Helper to detect columns to drop
        def _is_empty_or_helper(col_name: Any) -> bool:
            if not isinstance(col_name, str):
                return True
            name = col_name.strip().lower()
            return name in {"", "nan", "curves", "custom_curves", "hour", "index"}

        # Drop empty/helper columns
        keep_cols = [col for col in data.columns if not _is_empty_or_helper(col)]
        data = data[keep_cols]

        # Reset index to numeric starting at 0
        data.reset_index(drop=True, inplace=True)

        if data.empty:
            return

        # Build CustomCurves collection and apply
        try:
            curves = CustomCurves._from_dataframe(data, scenario_id=scenario.id)
            scenario.update_custom_curves(curves)
        except Exception as e:
            logger.warning(
                "Failed processing custom curves for '%s': %s", scenario.identifier(), e
            )

    @classmethod
    def from_excel(cls, xlsx_path: PathLike | str) -> "ScenarioPacker":
        """Create/load scenarios and apply updates from an Excel workbook.
        Behavior (new layout):
        - MAIN sheet contains one column per scenario; rows may include: scenario_id, short_name,
          area_code, end_year, private, template, title, sortables, custom_curves.
        - PARAMETERS uses short_name headers above a 'user' header; values never repeat across scenarios.
        - GQUERIES always repeat (same keys applied to each scenario column present).
        - Sortables and custom curves are read from per-scenario sheets named in MAIN.
        Returns a ScenarioPacker containing all touched scenarios.
        """
        packer = cls()

        # Try to open the workbook; if missing, return empty packer (keeps tests tolerant)
        try:
            xls = pd.ExcelFile(xlsx_path)
        except Exception as e:
            logger.warning("Could not open Excel file '%s': %s", xlsx_path, e)
            return packer

        # MAIN sheet
        try:
            main_df = xls.parse("MAIN", index_col=0)
        except Exception as e:
            logger.warning("Failed to parse MAIN sheet: %s", e)
            return packer

        if main_df is None or getattr(main_df, "empty", False):
            return packer

        # Build scenarios per MAIN column
        scenarios_by_col: Dict[str, Scenario] = {}
        for col in main_df.columns:
            try:
                scenario = packer._setup_scenario_from_main_column(
                    str(col), main_df[col]
                )
            except Exception as e:
                logger.warning("Failed to set up scenario for column '%s': %s", col, e)
                continue

            if scenario is not None:
                packer.add(scenario)
                scenarios_by_col[str(col)] = scenario

        if len(scenarios_by_col) == 0:
            return packer

        # Extract per-scenario sheet info and short_name mapping
        sheet_info = packer._extract_scenario_sheet_info(main_df)
        short_name_map: Dict[str, str] = {}
        for col_name, scenario in scenarios_by_col.items():
            info = sheet_info.get(col_name, {}) if isinstance(sheet_info, dict) else {}
            short = info.get("short_name") if isinstance(info, dict) else None
            if short is None or (isinstance(short, float) and pd.isna(short)):
                short = str(scenario.identifier())
            short_name_map[str(scenario.id)] = str(short)

        # PARAMETERS (inputs) sheet
        params_df = None
        try:
            # Read raw to allow our normalization to detect header rows
            params_df = xls.parse(InputsPack.sheet_name, header=None)
        except Exception:
            params_df = None

        if params_df is not None and not params_df.empty:
            try:
                packer._inputs.set_scenario_short_names(short_name_map)
                packer._inputs.from_dataframe(params_df)
            except Exception as e:
                logger.warning("Failed to import PARAMETERS: %s", e)

        # GQUERIES sheet (keys to attach to scenarios)
        gq_df = None
        for sheet in ("GQUERIES", QueryPack.sheet_name):
            if sheet in xls.sheet_names:
                try:
                    gq_df = xls.parse(sheet, header=None)
                    break
                except Exception:
                    gq_df = None
        if gq_df is not None and not gq_df.empty:
            try:
                qp = QueryPack(scenarios=packer._scenarios())
                norm_gq = qp._normalize_queries_dataframe(gq_df)
                if norm_gq is not None and not norm_gq.empty:
                    # Case 1: single column (global queries for all scenarios)
                    if len(norm_gq.columns) == 1:
                        col = norm_gq.columns[0]
                        values = [
                            v
                            for v in norm_gq[col].tolist()
                            if isinstance(v, str) and v.strip()
                        ]
                        seen = set()
                        keys = []
                        for v in values:
                            if v not in seen:
                                seen.add(v)
                                keys.append(v)
                        if keys:
                            for s in packer._scenarios():
                                try:
                                    s.add_queries(keys)
                                except Exception as e:
                                    logger.warning(
                                        "Failed adding global gqueries to '%s': %s",
                                        s.identifier(),
                                        e,
                                    )
                    else:
                        # Multiple columns: attempt identifier match, fallback to short_name
                        for col in norm_gq.columns:
                            scenario = next(
                                (
                                    s
                                    for s in packer._scenarios()
                                    if str(s.identifier()) == str(col)
                                ),
                                None,
                            )
                            if scenario is None:
                                # fallback: short_name mapping
                                match_id = next(
                                    (
                                        sid
                                        for sid, sn in short_name_map.items()
                                        if str(sn) == str(col)
                                    ),
                                    None,
                                )
                                if match_id is not None:
                                    scenario = next(
                                        (
                                            s
                                            for s in packer._scenarios()
                                            if str(s.id) == str(match_id)
                                        ),
                                        None,
                                    )
                            if scenario is None:
                                logger.warning(
                                    "Could not find scenario for gqueries column '%s' (identifier or short_name)",
                                    col,
                                )
                                continue
                            values = [
                                v
                                for v in norm_gq[col].tolist()
                                if isinstance(v, str) and v.strip()
                            ]
                            seen = set()
                            keys = []
                            for v in values:
                                if v not in seen:
                                    seen.add(v)
                                    keys.append(v)
                            if keys:
                                try:
                                    scenario.add_queries(keys)
                                except Exception as e:
                                    logger.warning(
                                        "Failed adding gqueries to '%s': %s",
                                        scenario.identifier(),
                                        e,
                                    )
            except Exception as e:
                logger.warning("Failed to import GQUERIES: %s", e)

        # Per-scenario Sortables and Custom Curves
        for col_name, scenario in scenarios_by_col.items():
            info = sheet_info.get(col_name, {}) if isinstance(sheet_info, dict) else {}

            sortables_sheet = info.get("sortables") if isinstance(info, dict) else None
            if isinstance(sortables_sheet, str) and sortables_sheet in xls.sheet_names:
                try:
                    s_df = xls.parse(sortables_sheet, header=None)
                    self_ref = packer  # clarity
                    self_ref._process_single_scenario_sortables(scenario, s_df)
                except Exception as e:
                    logger.warning(
                        "Failed to process SORTABLES sheet '%s' for '%s': %s",
                        sortables_sheet,
                        scenario.identifier(),
                        e,
                    )

            curves_sheet = info.get("custom_curves") if isinstance(info, dict) else None
            if isinstance(curves_sheet, str) and curves_sheet in xls.sheet_names:
                try:
                    c_df = xls.parse(curves_sheet, header=None)
                    self_ref = packer
                    self_ref._process_single_scenario_curves(scenario, c_df)
                except Exception as e:
                    logger.warning(
                        "Failed to process CUSTOM_CURVES sheet '%s' for '%s': %s",
                        curves_sheet,
                        scenario.identifier(),
                        e,
                    )

        return packer

    def _setup_scenario_from_main_column(
        self, col_name: str, col_data: pd.Series
    ) -> Optional[Scenario]:
        """Create or load a Scenario from a MAIN sheet column and apply metadata.
        Preference: if 'scenario_id' present -> load; otherwise create with area_code/end_year.
        """

        # Helper conversions
        def _to_bool(v: Any) -> Optional[bool]:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return bool(int(v))
            if isinstance(v, str):
                s = v.strip().lower()
                if s in {"true", "yes", "y", "1"}:
                    return True
                if s in {"false", "no", "n", "0"}:
                    return False
            return None

        def _to_int(v: Any) -> Optional[int]:
            try:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return int(float(v))
            except Exception:
                return None

        scenario_id = (
            col_data.get("scenario_id") if isinstance(col_data, pd.Series) else None
        )
        area_code = (
            col_data.get("area_code") if isinstance(col_data, pd.Series) else None
        )
        end_year = (
            _to_int(col_data.get("end_year"))
            if isinstance(col_data, pd.Series)
            else None
        )
        private = (
            _to_bool(col_data.get("private"))
            if isinstance(col_data, pd.Series)
            else None
        )
        template = (
            _to_int(col_data.get("template"))
            if isinstance(col_data, pd.Series)
            else None
        )
        title = col_data.get("title") if isinstance(col_data, pd.Series) else None

        scenario: Optional[Scenario] = None

        # Load or create
        sid = _to_int(scenario_id)
        if sid is not None:
            try:
                scenario = Scenario.load(sid)
            except Exception as e:
                logger.warning(
                    "Failed to load scenario %s for column '%s': %s", sid, col_name, e
                )
                scenario = None
        else:
            if area_code and end_year is not None:
                try:
                    scenario = Scenario.new(str(area_code), int(end_year))
                except Exception as e:
                    logger.warning(
                        "Failed to create scenario for column '%s' (area_code=%s, end_year=%s): %s",
                        col_name,
                        area_code,
                        end_year,
                        e,
                    )
                    scenario = None
            else:
                logger.warning(
                    "MAIN column '%s' missing required fields for creation (area_code/end_year)",
                    col_name,
                )
                scenario = None

        if scenario is None:
            return None

        # Apply metadata updates if provided
        meta_updates: Dict[str, Any] = {}
        if private is not None:
            meta_updates["private"] = private
        if template is not None:
            meta_updates["template"] = template
        if isinstance(title, str) and title.strip() != "":
            meta_updates["title"] = title.strip()

        if meta_updates:
            try:
                scenario.update_metadata(**meta_updates)
            except Exception as e:
                logger.warning(
                    "Failed to update metadata for '%s': %s", scenario.identifier(), e
                )

        return scenario
