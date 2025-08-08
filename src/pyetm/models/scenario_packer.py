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
        return next((s for s in self.scenarios if s.identifier() == identifier), None)


class InputsPack(Packable):
    key: ClassVar[str] = "inputs"
    sheet_name: ClassVar[str] = "PARAMETERS"

    def _to_dataframe(self, columns="user", **kwargs):
        # TODO: index on title if avaliable
        return pd.concat(
            [
                scenario.inputs.to_dataframe(columns=columns)
                for scenario in self.scenarios
            ],
            axis=1,
            keys=[scenario.identifier() for scenario in self.scenarios],
        )

    def _normalize_inputs_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize various inputs sheet shapes into canonical shape:
        - Drop leading completely blank rows.
        - Detect two header rows (identifier row above a row containing 'user').
        - Support 1- or 2-level row index (input[, unit]).
        Returns a DataFrame with:
          index -> Index or MultiIndex (input[, unit])
          columns -> MultiIndex (identifier, 'user')
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
        """
        if df is None or getattr(df, "empty", False):
            return

        # Canonicalize the incoming shape first
        try:
            df = self._normalize_inputs_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize inputs sheet: %s", e)
            return

        if df is None or df.empty:
            return

        # Now df has columns MultiIndex (identifier, 'user') and index input or (input, unit)
        identifiers = df.columns.get_level_values(0).unique()

        for identifier in identifiers:
            scenario = self._find_by_identifier(identifier)
            if scenario is None:
                logger.warning(
                    "Could not find scenario for identifier '%s'", identifier
                )
                continue

            scenario_df = df[identifier]
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

    @classmethod
    def from_excel(
        cls,
        filepath: str | PathLike,
        sheet_names: Optional[Dict[str, str]] = None,
    ):
        """
        Build a ScenarioPacker from an Excel file.
        sheet_names optionally maps logical pack keys to sheet titles, e.g.:
        {"main": "MAIN", "inputs": "PARAMETERS"}
        """
        packer = cls()
        sheet_names = sheet_names or {}

        with pd.ExcelFile(filepath) as xlsx:
            # Open main tab - create scenarios from there
            main_sheet = sheet_names.get("main", "MAIN")
            main_df = packer.read_sheet(xlsx, main_sheet, index_col=0)
            scenarios = packer.scenarios_from_df(main_df)

            # Inputs (optional): only process when the sheet exists
            inputs_sheet = sheet_names.get("inputs", packer._inputs.sheet_name)
            packer._inputs.add(*scenarios)
            # Read raw to tolerate blank header rows; we'll canonicalize inside from_dataframe
            inputs_df = packer.read_sheet(
                xlsx,
                inputs_sheet,
                required=False,
                header=None,
            )
            if isinstance(inputs_df, pd.DataFrame) and not inputs_df.empty:
                packer._inputs.from_dataframe(inputs_df)

            # Sortables (optional)
            sortables_sheet = (
                sheet_names.get("sortables", packer._sortables.sheet_name)
                if sheet_names
                else packer._sortables.sheet_name
            )
            sortables_df = packer.read_sheet(
                xlsx, sortables_sheet, required=False, header=None
            )
            if isinstance(sortables_df, pd.DataFrame) and not sortables_df.empty:
                packer._sortables.add(*scenarios)
                packer._sortables.from_dataframe(sortables_df)

            # Custom curves (optional)
            curves_sheet = (
                sheet_names.get("custom_curves", packer._custom_curves.sheet_name)
                if sheet_names
                else packer._custom_curves.sheet_name
            )
            curves_df = packer.read_sheet(
                xlsx, curves_sheet, required=False, header=None
            )
            if isinstance(curves_df, pd.DataFrame) and not curves_df.empty:
                packer._custom_curves.add(*scenarios)
                packer._custom_curves.from_dataframe(curves_df)

            # TODO: gqueries unpack not supported (read-only)

        return packer

    @staticmethod
    def scenarios_from_df(df: pd.DataFrame) -> list["Scenario"]:
        """Converts one df (MAIN) into a list of scenarios"""
        scenarios: list[Scenario] = []

        if isinstance(df, pd.Series):
            identifier = df.name
            data = df.to_dict()
            scenarios.append(ScenarioPacker.setup_scenario(identifier, data))
            return scenarios

        # DataFrame: columns are scenario identifiers (id or a custom title)
        for identifier in df.columns:
            col_data = df[identifier].to_dict()
            scenarios.append(ScenarioPacker.setup_scenario(identifier, col_data))

        return scenarios

    @staticmethod
    def setup_scenario(identifier, data):
        """Returns a scenario from data dict.
        If the identifier is an int (or str-int), load; otherwise create new.
        """
        # Normalize NA values to None
        data = {k: (None if pd.isna(v) else v) for k, v in data.items()}

        scenario: Scenario
        scenario_title: Optional[str] = (
            identifier if isinstance(identifier, str) and identifier != "" else None
        )

        # Try to interpret identifier as an ID
        scenario_id: Optional[int] = None
        if isinstance(identifier, (int, float)) and not pd.isna(identifier):
            try:
                scenario_id = int(identifier)
            except Exception:
                scenario_id = None
        elif isinstance(identifier, str):
            try:
                scenario_id = int(identifier)
            except ValueError:
                scenario_id = None

        if scenario_id is not None:
            # Load existing
            scenario = Scenario.load(scenario_id)
        else:
            # Create new (requires end_year and area_code)
            area_code = data.get("area_code")
            end_year = data.get("end_year")
            if area_code is None or end_year is None:
                raise ValueError(
                    "Cannot create a new Scenario without 'area_code' and 'end_year'"
                )
            scenario = Scenario.new(area_code=area_code, end_year=int(end_year))

        # Set a title when a non-numeric identifier is provided
        if scenario_title is not None:
            scenario.title = scenario_title

        # TODO: update metadata with the rest of the stuff in data!! (keep minimal for now)
        return scenario

    # NOTE: Move to utils?
    # Straight from Rob
    @staticmethod
    def read_sheet(
        xlsx: pd.ExcelFile, sheet_name: str, required: bool = True, **kwargs
    ) -> pd.Series:
        """read list items"""

        if not sheet_name in xlsx.sheet_names:
            if required:
                raise ValueError(
                    f"Could not load required sheet '{sheet_name}' from {xlsx.io}"
                )
            logger.warning(
                "Could not load optional sheet '%s' from '%s'", sheet_name, xlsx.io
            )
            return pd.Series(name=sheet_name, dtype=str)

        # Use the ExcelFile.parse API directly to avoid pandas' engine guard
        values = xlsx.parse(sheet_name=sheet_name, **kwargs).squeeze(axis=1)

        return values  # .rename(sheet_name)
