import pandas as pd
import logging
from os import PathLike
from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any, Set, Literal, ClassVar
from xlsxwriter import Workbook

from pyetm.models.base import Base
from pyetm.models import Scenario
from pyetm.models.custom_curves import CustomCurves
from pyetm.utils.excel import add_frame_with_scenario_styling

logger = logging.getLogger(__name__)


class Packable(BaseModel):
    # Use a proper default set and keep the type consistent
    scenarios: Set["Scenario"] = Field(default_factory=set)
    key: ClassVar[str] = "base_pack"
    sheet_name: ClassVar[str] = "SHEET"

    def add(self, *scenarios):
        "Adds one or more scenarios to the packable"
        self.scenarios.update(scenarios)

    def discard(self, scenario):
        "Removes a scenario from the pack"
        self.scenarios.discard(scenario)

    def clear(self):
        # Reset to an empty set
        self.scenarios.clear()

    def summary(self) -> dict:
        return {self.key: {"scenario_count": len(self.scenarios)}}

    def to_dataframe(self, columns="") -> pd.DataFrame:
        """Convert the pack into a dataframe"""
        if len(self.scenarios) == 0:
            return pd.DataFrame()

        return self._to_dataframe(columns=columns)

    def from_dataframe(self, df):
        """Should parse the df and call correct setters on identified scenarios"""

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """Base implementation - kids should implement this"""
        return pd.DataFrame()

    def _find_by_identifier(self, identifier: str):
        ident_str = str(identifier)
        return next(
            (s for s in self.scenarios if str(s.identifier()) == ident_str),
            None,
        )


class InputsPack(Packable):
    key: ClassVar[str] = "inputs"
    sheet_name: ClassVar[str] = "PARAMETERS"

    def __init__(self, **data):
        super().__init__(**data)
        self._scenario_short_names: Dict[str, str] = {}  # scenario_id -> short_name mapping

    def set_scenario_short_names(self, scenario_short_names: Dict[str, str]):
        """Set the mapping of scenario identifiers to their short names."""
        self._scenario_short_names = scenario_short_names

    def _find_by_short_name(self, short_name: str):
        """Find scenario by its short name."""
        short_name_str = str(short_name)
        for scenario in self.scenarios:
            scenario_id = str(scenario.identifier())
            if self._scenario_short_names.get(scenario_id) == short_name_str:
                return scenario
        return None

    def _to_dataframe(self, columns="user", **kwargs):
        return pd.concat(
            [
                scenario.inputs.to_dataframe(columns=columns)
                for scenario in self.scenarios
            ],
            axis=1,
            keys=[self._scenario_short_names.get(str(scenario.identifier()), str(scenario.identifier())) 
                  for scenario in self.scenarios],
        )

    def _normalize_inputs_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize various inputs sheet shapes into canonical shape:
        - Drop leading completely blank rows.
        - Detect two header rows (short_name row above a row containing 'user').
        - Support 1- or 2-level row index (input[, unit]).
        Returns a DataFrame with:
          index -> Index or MultiIndex (input[, unit])
          columns -> MultiIndex (short_name, 'user')
        """
        # Drop completely empty rows
        df = df.dropna(how="all")
        if df.empty:
            return df

        # Locate the row containing 'user' (case-insensitive)
        user_row_pos = None
        for pos, (_, row) in enumerate(df.iterrows()):
            if any(isinstance(v, str) and v.strip().lower() == "user" for v in row):
                user_row_pos = pos
                break
        if user_row_pos is None:
            # Fallback: assume row 1 is the 'user' row
            user_row_pos = 1 if len(df) > 1 else 0

        header_start = max(user_row_pos - 1, 0)
        header_end = user_row_pos  # inclusive

        headers = df.iloc[header_start : header_end + 1].astype(str)
        data = df.iloc[header_end + 1 :].copy()

        # Build MultiIndex columns from the two header rows
        data.columns = pd.MultiIndex.from_arrays(
            [headers.iloc[0].values, headers.iloc[1].values]
        )

        # Identify index columns as those whose second-level header is not 'user'
        idx_cols = [
            col
            for col in data.columns
            if not (isinstance(col[1], str) and col[1].strip().lower() == "user")
        ]

        # Choose input and optional unit columns
        if len(idx_cols) == 0:
            # No explicit index columns: assume first column is inputs
            input_col = data.columns[0]
            unit_col = None
        else:
            input_col = idx_cols[0]
            unit_col = idx_cols[1] if len(idx_cols) > 1 else None

        # Construct index
        input_series = data[input_col].astype(str)
        if unit_col is not None:
            unit_series = data[unit_col].astype(str)
            index = pd.MultiIndex.from_arrays(
                [input_series.values, unit_series.values], names=["input", "unit"]
            )
        else:
            index = pd.Index(input_series.values, name="input")

        # Drop index columns and keep only scenario columns
        keep_cols = [
            c
            for c in data.columns
            if c not in {input_col} and (unit_col is None or c != unit_col)
        ]
        canonical = data[keep_cols]
        canonical.index = index

        # Ensure second level equals 'user'; if not, set it
        if isinstance(canonical.columns, pd.MultiIndex):
            lvl1 = canonical.columns.get_level_values(1)
            if not all(
                isinstance(v, str) and v.strip().lower() == "user" for v in lvl1
            ):
                canonical.columns = pd.MultiIndex.from_arrays(
                    [
                        canonical.columns.get_level_values(0),
                        ["user"] * len(canonical.columns),
                    ]
                )
        else:
            canonical.columns = pd.MultiIndex.from_arrays(
                [canonical.columns, ["user"] * len(canonical.columns)]
            )

        return canonical

    def from_dataframe(self, df):
        """
        Sets the inputs on the scenarios from the packed df (comes from excel)
        Tolerates optional unit column and leading blank rows.
        Uses short_name for scenario identification.
        """
        if df is None or getattr(df, "empty", False):
            return

        try:
            df = self._normalize_inputs_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize inputs sheet: %s", e)
            return

        if df is None or df.empty:
            return

        # Now df has columns MultiIndex (short_name, 'user') and index input or (input, unit)
        short_names = df.columns.get_level_values(0).unique()

        for short_name in short_names:
            scenario = self._find_by_short_name(short_name)
            if scenario is None:
                logger.warning(
                    "Could not find scenario for short_name '%s'", short_name
                )
                continue

            scenario_df = df[short_name]
            # Ensure DataFrame with a 'user' column
            if isinstance(scenario_df, pd.Series):
                scenario_df = scenario_df.to_frame(name="user")
            else:
                if list(scenario_df.columns) != ["user"]:
                    scenario_df = scenario_df.copy()
                    first_col = scenario_df.columns[0]
                    scenario_df = scenario_df.rename(columns={first_col: "user"})

            scenario.set_user_values_from_dataframe(scenario_df)


class QueryPack(Packable):
    key: ClassVar[str] = "gquery"
    sheet_name: ClassVar[str] = "GQUERIES_RESULTS"

    def _to_dataframe(
        self, columns="future", **kwargs
    ) -> pd.DataFrame:  # Make sure **kwargs is here
        if not self.scenarios:
            return pd.DataFrame()

        return pd.concat(
            [scenario.results(columns=columns) for scenario in self.scenarios],
            axis=1,
            keys=[scenario.identifier() for scenario in self.scenarios],
            copy=False,
        )

    def _normalize_queries_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize a GQUERIES sheet into a simple shape:
        - Drop leading completely blank rows and columns.
        - Detect 1 or 2 header rows.
        - Return a DataFrame with columns = scenario identifiers and rows listing gquery keys.
        We ignore any leftmost index/helper columns whose header is empty or looks like a label.
        """
        if df is None:
            return pd.DataFrame()

        # Drop completely empty rows/cols
        df = df.dropna(how="all")
        if df.empty:
            return df
        df = df.dropna(axis=1, how="all")

        # Find non-empty rows for potential headers
        non_empty_rows = [
            i for i, (_, r) in enumerate(df.iterrows()) if not r.isna().all()
        ]
        if not non_empty_rows:
            return pd.DataFrame()

        # Heuristic: if the second non-empty row contains the word 'gquery'/'gqueries', use 2 header rows
        header_rows = 1
        if len(non_empty_rows) > 1:
            second = (
                df.iloc[non_empty_rows[1]].astype(str).str.strip().str.lower().tolist()
            )
            if any(val in {"gquery", "gqueries", "key", "queries"} for val in second):
                header_rows = 2

        header_start = non_empty_rows[0]
        header_end = header_start + header_rows - 1
        headers = df.iloc[header_start : header_end + 1].astype(str)
        data = df.iloc[header_end + 1 :].copy()

        # Assign columns: MultiIndex if 2 header rows else single level
        if header_rows == 2:
            cols = pd.MultiIndex.from_arrays(
                [headers.iloc[0].values, headers.iloc[1].values]
            )
        else:
            cols = pd.Index(headers.iloc[0].values)
        data.columns = cols

        def _is_empty(v):
            return (
                (not isinstance(v, str))
                or (v.strip() == "")
                or (v.strip().lower() == "nan")
            )

        def _is_helper_label(v):
            return isinstance(v, str) and v.strip().lower() in {
                "gquery",
                "gqueries",
                "queries",
                "key",
            }

        # Build a simple DataFrame with columns = scenario identifiers
        if isinstance(data.columns, pd.MultiIndex):
            keep = [
                c
                for c in data.columns
                if not _is_empty(c[0]) and not _is_helper_label(c[0])
            ]
            data = data[keep]
            # Collapse to single level (identifier)
            data.columns = [c[0] for c in data.columns]
        else:
            keep = [
                c
                for c in data.columns
                if isinstance(c, str) and not _is_empty(c) and not _is_helper_label(c)
            ]
            data = data[keep]

        # Ensure string values and drop rows that are completely blank across all kept columns
        for c in data.columns:
            data[c] = data[c].apply(lambda x: None if pd.isna(x) else (str(x).strip()))
        data = data.dropna(how="all")

        return data

    def from_dataframe(self, df: pd.DataFrame):
        """Collect gquery keys for each scenario from a GQUERIES sheet and attach them.
        The sheet is expected to have one column per scenario (by identifier/title),
        with each row containing a gquery key. Blank rows are ignored.
        """
        if df is None or getattr(df, "empty", False):
            return

        try:
            df = self._normalize_queries_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize gqueries sheet: %s", e)
            return

        if df is None or df.empty:
            return

        for identifier in df.columns:
            scenario = self._find_by_identifier(identifier)
            if scenario is None:
                logger.warning(
                    "Could not find scenario for identifier '%s'", identifier
                )
                continue

            # Extract non-empty keys, preserve order, remove duplicates while preserving order
            values = [
                v
                for v in df[identifier].tolist()
                if isinstance(v, str) and v.strip() != ""
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
                    logger.warning("Failed to add gqueries to '%s': %s", identifier, e)


class SortablePack(Packable):
    key: ClassVar[str] = "sortables"
    sheet_name: ClassVar[str] = "SORTABLES"

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """Pack sortables data for all scenarios with multi-index support"""
        if not self.scenarios:
            return pd.DataFrame()

        sortables_dfs = []
        scenario_keys = []

        for scenario in self.scenarios:
            df = scenario.sortables.to_dataframe()
            if not df.empty:
                sortables_dfs.append(df)
                scenario_keys.append(scenario.identifier())

        if not sortables_dfs:
            return pd.DataFrame()

        return pd.concat(
            sortables_dfs,
            axis=1,
            keys=scenario_keys,
        )

    def _normalize_sortables_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize various sortables sheet shapes.
        Assumptions:
        - Two header rows: first row = scenario identifier/title, second row = sortable name.
        - Leading blank rows may exist and are ignored.
        - Optional leftmost index column(s) are present with empty first header cell(s). They are dropped.
        Returns a DataFrame with columns MultiIndex(identifier, sortable_name) and
        simple Index rows with order positions.
        """
        # Drop completely empty rows
        df = df.dropna(how="all")
        if df.empty:
            return df

        # Find the first two non-empty rows -> headers
        non_empty_idx = [
            i for i, (_, r) in enumerate(df.iterrows()) if not r.isna().all()
        ]
        if not non_empty_idx:
            return pd.DataFrame()
        header0_pos = non_empty_idx[0]
        header1_pos = non_empty_idx[1] if len(non_empty_idx) > 1 else header0_pos + 1

        headers = df.iloc[[header0_pos, header1_pos]].astype(str)
        data = df.iloc[header1_pos + 1 :].copy()

        # Build MultiIndex columns
        col_level0 = headers.iloc[0].values
        col_level1 = headers.iloc[1].values
        columns = pd.MultiIndex.from_arrays([col_level0, col_level1])
        data.columns = columns

        # Drop columns where identifier (level 0) is missing/empty, and drop any
        # column which is clearly the helper/index label (e.g. level1 == 'sortables').
        def _is_empty(v):
            return (
                (not isinstance(v, str))
                or (v.strip() == "")
                or (v.strip().lower() == "nan")
            )

        def _is_helper_label(v):
            return isinstance(v, str) and v.strip().lower() in {"sortables"}

        keep_cols = [
            c
            for c in data.columns
            if not _is_empty(c[0]) and not _is_helper_label(c[1])
        ]
        canonical = data[keep_cols].copy()

        # Result: MultiIndex columns (identifier, sortable_name), index as row number
        canonical.reset_index(drop=True, inplace=True)
        return canonical

    def from_dataframe(self, df: pd.DataFrame):
        """Unpack and update sortables for each scenario from the sheet.
        The sheet may contain optional leading blank rows and optional index columns.
        """
        if df is None or getattr(df, "empty", False):
            return

        try:
            df = self._normalize_sortables_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize sortables sheet: %s", e)
            return

        if df is None or df.empty or not isinstance(df.columns, pd.MultiIndex):
            return

        identifiers = df.columns.get_level_values(0).unique()
        for identifier in identifiers:
            scenario = self._find_by_identifier(identifier)
            if scenario is None:
                logger.warning(
                    "Could not find scenario for identifier '%s'", identifier
                )
                continue

            block = df[identifier]
            try:
                scenario.set_sortables_from_dataframe(block)
            except Exception as e:
                logger.warning("Failed to update sortables for '%s': %s", identifier, e)


class CustomCurvesPack(Packable):
    key: ClassVar[str] = "custom_curves"
    sheet_name: ClassVar[str] = "CUSTOM_CURVES"

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """Pack custom curves data for all scenarios with multi-index support"""
        if not self.scenarios:
            return pd.DataFrame()

        curves_dfs = []
        scenario_keys = []

        for scenario in self.scenarios:
            series_list = list(scenario.custom_curves_series())
            if len(series_list) > 0:
                df = pd.concat(series_list, axis=1)
                curves_dfs.append(df)
                scenario_keys.append(scenario.identifier())

        if not curves_dfs:
            return pd.DataFrame()

        return pd.concat(
            curves_dfs,
            axis=1,
            keys=scenario_keys,
        )

    def _normalize_curves_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize custom curves sheet shapes.
        Assumptions:
        - Two header rows: first row = scenario identifier/title, second row = curve key.
        - Leading blank rows may exist and are ignored.
        - Optional leftmost index column(s) are present with empty first header cell(s); they are dropped.
        - Rows are the hourly values (0..8759) or arbitrary length; we keep as-is.
        Returns a DataFrame with columns MultiIndex(identifier, curve_key) and numeric rows.
        """
        df = df.dropna(how="all")
        if df.empty:
            return df

        # Find header rows
        non_empty_idx = [
            i for i, (_, r) in enumerate(df.iterrows()) if not r.isna().all()
        ]
        if not non_empty_idx:
            return pd.DataFrame()
        header0_pos = non_empty_idx[0]
        header1_pos = non_empty_idx[1] if len(non_empty_idx) > 1 else header0_pos + 1

        headers = df.iloc[[header0_pos, header1_pos]].astype(str)
        data = df.iloc[header1_pos + 1 :].copy()

        # Assign columns
        columns = pd.MultiIndex.from_arrays(
            [headers.iloc[0].values, headers.iloc[1].values]
        )
        data.columns = columns

        # Drop non-scenario columns where identifier is empty and drop helper/index
        # columns where level1 looks like a label (e.g., 'sortables')
        def _is_empty(v):
            return (
                (not isinstance(v, str))
                or (v.strip() == "")
                or (v.strip().lower() == "nan")
            )

        def _is_helper_label(v):
            return isinstance(v, str) and v.strip().lower() in {"sortables"}

        keep_cols = [
            c
            for c in data.columns
            if not _is_empty(c[0]) and not _is_helper_label(c[1])
        ]
        canonical = data[keep_cols].copy()

        # Reset index to numeric starting at 0
        canonical.reset_index(drop=True, inplace=True)
        return canonical

    def from_dataframe(self, df: pd.DataFrame):
        """Unpack and update custom curves for each scenario.
        Sheet may contain leading blank rows and optional index columns.
        """
        if df is None or getattr(df, "empty", False):
            return

        try:
            df = self._normalize_curves_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize custom curves sheet: %s", e)
            return

        if df is None or df.empty or not isinstance(df.columns, pd.MultiIndex):
            return

        identifiers = df.columns.get_level_values(0).unique()
        for identifier in identifiers:
            scenario = self._find_by_identifier(identifier)
            if scenario is None:
                logger.warning(
                    "Could not find scenario for identifier '%s'", identifier
                )
                continue

            block = df[identifier]
            # Build a CustomCurves collection from the block columns
            try:
                curves = CustomCurves._from_dataframe(block)
            except Exception as e:
                logger.warning(
                    "Failed to build custom curves for '%s': %s", identifier, e
                )
                continue

            # Validate and upload
            try:
                scenario.update_custom_curves(curves)
            except Exception as e:
                logger.warning(
                    "Failed to update custom curves for '%s': %s", identifier, e
                )


class OutputCurvesPack(Packable):
    key: ClassVar[str] = "output_curves"
    sheet_name: ClassVar[str] = "OUTPUT_CURVES"

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """Pack output curves data for all scenarios with multi-index support"""
        if not self.scenarios:
            return pd.DataFrame()

        curves_dfs = []
        scenario_keys = []

        for scenario in self.scenarios:
            series_list = list(scenario.all_output_curves())
            if len(series_list) > 0:
                df = pd.concat(series_list, axis=1)
                curves_dfs.append(df)
                scenario_keys.append(scenario.identifier())

        if not curves_dfs:
            return pd.DataFrame()

        return pd.concat(
            curves_dfs,
            axis=1,
            keys=scenario_keys,
        )


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

    def _extract_scenario_sheet_info(self, main_df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
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
            short_name = main_df.get('short_name')
            sortables_sheet = main_df.get('sortables')
            custom_curves_sheet = main_df.get('custom_curves')
            
            scenario_sheets[identifier] = {
                'short_name': short_name if pd.notna(short_name) else identifier,
                'sortables': sortables_sheet if pd.notna(sortables_sheet) else None,
                'custom_curves': custom_curves_sheet if pd.notna(custom_curves_sheet) else None
            }
        else:
            # Multiple scenarios
            for identifier in main_df.columns:
                col_data = main_df[identifier]
                short_name = col_data.get('short_name')
                sortables_sheet = col_data.get('sortables')
                custom_curves_sheet = col_data.get('custom_curves')
                
                scenario_sheets[str(identifier)] = {
                    'short_name': short_name if pd.notna(short_name) else str(identifier),
                    'sortables': sortables_sheet if pd.notna(sortables_sheet) else None,
                    'custom_curves': custom_curves_sheet if pd.notna(custom_curves_sheet) else None
                }
        
        return scenario_sheets

    def _process_single_scenario_sortables(self, scenario: "Scenario", df: pd.DataFrame):
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
        non_empty_idx = [i for i, (_, r) in enumerate(df.iterrows()) if not r.isna().all()]
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
        non_empty_idx = [i for i, (_, r) in enumerate(df.iterrows()) if not r.isna().all()]
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
            curves = CustomCurves._from_dataframe(data)
            scenario.update_custom_curves(curves)
        except Exception as e:
            logger.warning("Failed processing custom curves for '%s': %s", scenario.identifier(), e)

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
                scenario = packer._setup_scenario_from_main_column(str(col), main_df[col])
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
                QueryPack(scenarios=packer._scenarios()).from_dataframe(gq_df)
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

    def _setup_scenario_from_main_column(self, col_name: str, col_data: pd.Series) -> Optional[Scenario]:
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

        scenario_id = col_data.get("scenario_id") if isinstance(col_data, pd.Series) else None
        area_code = col_data.get("area_code") if isinstance(col_data, pd.Series) else None
        end_year = _to_int(col_data.get("end_year")) if isinstance(col_data, pd.Series) else None
        private = _to_bool(col_data.get("private")) if isinstance(col_data, pd.Series) else None
        template = _to_int(col_data.get("template")) if isinstance(col_data, pd.Series) else None
        title = col_data.get("title") if isinstance(col_data, pd.Series) else None

        scenario: Optional[Scenario] = None

        # Load or create
        sid = _to_int(scenario_id)
        if sid is not None:
            try:
                scenario = Scenario.load(sid)
            except Exception as e:
                logger.warning("Failed to load scenario %s for column '%s': %s", sid, col_name, e)
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
                logger.warning("Failed to update metadata for '%s': %s", scenario.identifier(), e)

        return scenario
