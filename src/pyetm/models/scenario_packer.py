import pandas as pd
import logging
from pathlib import Path
from os import PathLike
from pydantic import BaseModel
from typing import Optional, Dict, Any, Sequence
from xlsxwriter import Workbook

from pyetm.models.packables.custom_curves_pack import CustomCurvesPack
from pyetm.models.packables.inputs_pack import InputsPack
from pyetm.models.packables.output_curves_pack import OutputCurvesPack
from pyetm.models.packables.query_pack import QueryPack
from pyetm.models.packables.sortable_pack import SortablePack
from pyetm.models import Scenario
from pyetm.models.custom_curves import CustomCurves
from pyetm.utils.excel import add_frame

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
        """Create main info DataFrame by concatenating scenario dataframes."""
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

    def to_excel(
        self,
        path: str,
        *,
        export_output_curves: bool = True,
        output_curves_path: Optional[str] = None,
        carriers: Optional[Sequence[str]] = None,
    ):
        if len(self._scenarios()) == 0:
            raise ValueError("Packer was empty, nothing to export")

        workbook = Workbook(path)

        # Main info sheet: enrich with metadata and friendly headers for Excel only
        df_main = self._build_excel_main_dataframe()
        if not df_main.empty:
            df_main = self._sanitize_dataframe_for_excel(df_main)
            add_frame(
                name="MAIN",
                frame=df_main,
                workbook=workbook,
                column_width=18,
                scenario_styling=True,
            )

        for pack in self.all_pack_data():
            # Skip output curves in the main workbook; exported separately
            if getattr(pack, "key", None) == OutputCurvesPack.key:
                continue
            df = pack.to_dataframe()
            if not df.empty:
                df_filled = df.fillna("").infer_objects(copy=False)
                add_frame(
                    name=pack.sheet_name,
                    frame=df_filled,
                    workbook=workbook,
                    column_width=18,
                    scenario_styling=True,
                )

        workbook.close()

        # Export output curves to a separate workbook with one sheet per carrier
        if export_output_curves:
            oc_path = output_curves_path
            if oc_path is None:
                base = Path(path)
                oc_path = str(base.with_name(f"{base.stem}_output_curves{base.suffix}"))
            try:
                self._output_curves.to_excel_per_carrier(oc_path, carriers)
            except Exception as e:
                logger.warning("Failed exporting output curves workbook: %s", e)

    def _scenarios(self) -> set["Scenario"]:
        """
        All scenarios we are packing info for across all packs.
        """
        all_scenarios: set["Scenario"] = set()
        for pack in self.all_pack_data():
            try:
                items = getattr(pack, "scenarios", None)
                if not items:
                    continue
                if isinstance(items, set):
                    all_scenarios.update(items)
                else:
                    try:
                        all_scenarios.update(list(items))
                    except TypeError:
                        continue
            except Exception:
                continue
        return all_scenarios

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

    def _first_non_empty_row_index(self, df: pd.DataFrame) -> Optional[int]:
        if df is None:
            return None
        for idx, (_, row) in enumerate(df.iterrows()):
            if not row.isna().all():
                return idx
        return None

    def _is_empty_or_helper(self, col_name: Any, helper_names: set[str]) -> bool:
        if not isinstance(col_name, str):
            return True
        name = col_name.strip().lower()
        return name in (helper_names or set()) or name in {"", "nan"}

    def _normalize_sheet(
        self,
        df: pd.DataFrame,
        *,
        helper_names: set[str],
        reset_index: bool = True,
        rename_map: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()
        df = df.dropna(how="all")
        if df.empty:
            return df

        header_pos = self._first_non_empty_row_index(df)
        if header_pos is None:
            return pd.DataFrame()

        header = df.iloc[header_pos].astype(str).map(lambda s: s.strip())
        data = df.iloc[header_pos + 1 :].copy()
        data.columns = header.values

        keep_cols = [
            col
            for col in data.columns
            if not self._is_empty_or_helper(col, helper_names)
        ]
        data = data[keep_cols]

        if rename_map:
            data = data.rename(columns=rename_map)

        if reset_index:
            data.reset_index(drop=True, inplace=True)

        return data

    def _coerce_bool(self, v: Any) -> Optional[bool]:
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

    def _coerce_int(self, v: Any) -> Optional[int]:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None

    def _load_or_create_scenario(
        self,
        sid: Optional[int],
        area_code: Any,
        end_year: Optional[int],
        col_name: str,
    ) -> Optional["Scenario"]:
        scenario: Optional[Scenario] = None
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
        return scenario

    def _collect_meta_updates(
        self,
        private: Optional[bool],
        template: Optional[int],
        source: Any,
        title: Any,
    ) -> Dict[str, Any]:
        meta_updates: Dict[str, Any] = {}
        if private is not None:
            meta_updates["private"] = private
        if template is not None:
            meta_updates["template"] = template
        if isinstance(source, str) and source.strip() != "":
            meta_updates["source"] = source.strip()
        if isinstance(title, str) and title.strip() != "":
            meta_updates["title"] = title.strip()
        return meta_updates

    def _apply_metadata(
        self, scenario: "Scenario", meta_updates: Dict[str, Any]
    ) -> None:
        if not meta_updates:
            return
        try:
            scenario.update_metadata(**meta_updates)
        except Exception as e:
            logger.warning(
                "Failed to update metadata for '%s': %s", scenario.identifier(), e
            )

    def _extract_scenario_sheet_info(
        self, main_df: pd.DataFrame
    ) -> Dict[str, Dict[str, str]]:
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
        data = self._normalize_sheet(
            df,
            helper_names={"sortables", "hour", "index"},
            reset_index=True,
            rename_map={"heat_network": "heat_network_lt"},
        )
        if data is None or data.empty:
            return
        try:
            scenario.set_sortables_from_dataframe(data)
        except Exception as e:
            logger.warning(
                "Failed processing sortables for '%s': %s", scenario.identifier(), e
            )
        else:
            try:
                if hasattr(scenario, "_sortables") and scenario._sortables is not None:
                    scenario._sortables.log_warnings(
                        logger,
                        prefix=f"Sortables warning for '{scenario.identifier()}'",
                    )
            except Exception:
                pass

    def _process_single_scenario_curves(self, scenario: "Scenario", df: pd.DataFrame):
        data = self._normalize_sheet(
            df,
            helper_names={"curves", "custom_curves", "hour", "index"},
            reset_index=True,
        )
        if data is None or data.empty:
            return
        # Build CustomCurves collection and apply
        try:
            curves = CustomCurves._from_dataframe(data, scenario_id=scenario.id)
            curves.log_warnings(
                logger,
                prefix=f"Custom curves warning for '{scenario.identifier()}'",
            )
            try:
                validation = curves.validate_for_upload()
                for key, collector in (validation or {}).items():
                    for w in collector:
                        logger.warning(
                            "Custom curve validation for '%s' in '%s' [%s]: %s",
                            key,
                            scenario.identifier(),
                            getattr(w, "field", key),
                            getattr(w, "message", str(w)),
                        )
            except Exception:
                pass

            scenario.update_custom_curves(curves)
        except Exception as e:
            try:
                if "curves" in locals():
                    curves.log_warnings(
                        logger,
                        prefix=f"Custom curves warning for '{scenario.identifier()}'",
                    )
            except Exception:
                pass
            logger.warning(
                "Failed processing custom curves for '%s': %s", scenario.identifier(), e
            )

    @classmethod
    def from_excel(cls, xlsx_path: PathLike | str) -> "ScenarioPacker":
        packer = cls()

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
                qp.from_dataframe(gq_df)
            except Exception as e:
                logger.warning("Failed to import GQUERIES: %s", e)

        # Sortables and Custom Curves
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
        scenario_id = (
            col_data.get("scenario_id") if isinstance(col_data, pd.Series) else None
        )
        area_code = (
            col_data.get("area_code") if isinstance(col_data, pd.Series) else None
        )
        end_year = (
            self._coerce_int(col_data.get("end_year"))
            if isinstance(col_data, pd.Series)
            else None
        )
        private = (
            self._coerce_bool(col_data.get("private"))
            if isinstance(col_data, pd.Series)
            else None
        )
        template = (
            self._coerce_int(col_data.get("template"))
            if isinstance(col_data, pd.Series)
            else None
        )
        source = col_data.get("source") if isinstance(col_data, pd.Series) else None
        title = col_data.get("title") if isinstance(col_data, pd.Series) else None
        sid = self._coerce_int(scenario_id)
        scenario = self._load_or_create_scenario(sid, area_code, end_year, col_name)

        if scenario is None:
            return None
        meta_updates = self._collect_meta_updates(private, template, source, title)
        self._apply_metadata(scenario, meta_updates)

        return scenario

    def _build_excel_main_dataframe(self) -> pd.DataFrame:
        """Build a MAIN sheet DataFrame with rich metadata for Excel export only."""
        scenarios = list(self._scenarios())
        if not scenarios:
            return pd.DataFrame()

        def id_or_title(s):
            try:
                return s.identifier()
            except Exception:
                return getattr(s, "id", None)

        columns: dict[Any, dict[str, Any]] = {}
        all_keys: list[str] = []

        def add_key(k: str):
            if k not in all_keys:
                all_keys.append(k)

        for s in scenarios:
            info: dict[str, Any] = {}

            # Core identifiers and common fields
            info["scenario_id"] = getattr(s, "id", None)
            info["area_code"] = getattr(s, "area_code", None)
            info["end_year"] = getattr(s, "end_year", None)
            info["start_year"] = getattr(s, "start_year", None)
            info["keep_compatible"] = getattr(s, "keep_compatible", None)
            info["private"] = getattr(s, "private", None)
            info["template"] = getattr(s, "template", None)
            info["source"] = getattr(s, "source", None)
            try:
                info["title"] = s.title
            except Exception:
                info["title"] = None
            meta = getattr(s, "metadata", None)
            if isinstance(meta, dict):
                desc = meta.get("description")
                if desc is not None:
                    info["description"] = desc
            info["url"] = getattr(s, "url", None)
            try:
                info["version"] = s.version
            except Exception:
                info["version"] = None
            info["created_at"] = getattr(s, "created_at", None)
            info["updated_at"] = getattr(s, "updated_at", None)

            # Flatten all other metadata keys
            if isinstance(meta, dict):
                for k, v in meta.items():
                    if k in ("title", "description"):
                        continue
                    info[f"metadata.{k}"] = v

            label = id_or_title(s)
            columns[label] = info
            for k in info.keys():
                add_key(k)

        # Preferred key order at top
        preferred = [
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
        remaining = [k for k in all_keys if k not in preferred]
        index_order = preferred + remaining

        df = pd.DataFrame(
            {col: {k: columns[col].get(k) for k in index_order} for col in columns}
        )
        df.index.name = "scenario"
        return df

    def _sanitize_dataframe_for_excel(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert DataFrame values into Excel-safe primitives (str/number/bool/None)."""
        import datetime as _dt

        def _safe(v: Any):
            if v is None:
                return ""
            if isinstance(v, (str, int, float, bool)):
                return v
            # Pandas Timestamp / datetime
            if isinstance(v, (pd.Timestamp, _dt.datetime, _dt.date)):
                try:
                    return str(v)
                except Exception:
                    return ""
            # Convert everything else to string
            try:
                return str(v)
            except Exception:
                return ""

        out = df.copy()
        out.index = out.index.map(lambda x: str(x) if x is not None else "")
        out.columns = [str(c) if c is not None else "" for c in out.columns]
        return out.map(_safe)
