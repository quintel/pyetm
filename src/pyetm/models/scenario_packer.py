import pandas as pd
from os import PathLike
from pydantic import BaseModel
from typing import Optional, Dict, List, Any, Set, Literal, ClassVar
from xlsxwriter import Workbook

from pyetm.models.base import Base
from pyetm.models import Scenario
from pyetm.utils.excel import add_frame


class Packable(BaseModel):
    scenarios: Optional[set["Scenario"]] = set()
    key: ClassVar[str] = "base_pack"

    def add(self, *scenarios):
        "Adds one or more scenarios to the packable"
        self.scenarios.update(scenarios)

    def discard(self, scenario):
        "Removes a scenario from the pack"
        self.scenarios.discard(scenario)

    def clear(self):
        self.scenarios = []

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

    def _to_dataframe(self, columns="user", **kwargs):
        # TODO: index on title if avaliable
        return pd.concat(
            [scenario.inputs.to_dataframe(columns=columns) for scenario in self.scenarios],
            axis=1,
            keys=[scenario.identifier() for scenario in self.scenarios],
        )

    def from_dataframe(self, df):
        """
        Sets the inputs on the scenarios from the packed df (comes from excel)
        In case came it came from a df containing defaults etc, lets drop them
        """
        user_values = df.xs('user', level=1, axis=1, drop_level=False)
        for identifier, _ in user_values:
            breakpoint()
            scenario = self._find_by_identifier(identifier)
            scenario.set_user_values_from_dataframe(user_values[identifier])


class QueryPack(Packable):
    key: ClassVar[str] = "gquery"

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

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            return scenario.sortables.to_dataframe()


class CustomCurvesPack(Packable):
    key: ClassVar[str] = "custom_curves"

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            series_list = list(scenario.custom_curves_series())
            if len(series_list) == 0:
                continue
            return pd.concat(series_list, axis=1)
        return pd.DataFrame()


class OutputCurvesPack(Packable):
    key: ClassVar[str] = "output_curves"

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            series_list = list(scenario.all_output_curves())
            if len(series_list) == 0:
                continue
            return pd.concat(series_list, axis=1)
        return pd.DataFrame()


class ScenarioPacker(BaseModel):
    """
    Packs one or multiple scenarios for export to dataframes or excel

    TODO: This class doesn't inherit from Base so it doesn't use the warning system yet - consider this further.
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

        return pd.concat([scenario.to_dataframe() for scenario in self._scenarios()], axis=1)

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
        """Export to Excel with simplified approach"""
        if len(self._scenarios()) == 0:
            raise ValueError("Packer was empty, nothing to export")

        workbook = Workbook(path)

        # TODO: this should come from the packs, they should know their sheet names
        # Like this they can pack and unpack
        sheet_configs = [
            ("MAIN", self.main_info),
            ("PARAMETERS", self.inputs),
            ("GQUERIES_RESULTS", self.gquery_results),
            ("SORTABLES", self.sortables),
            ("CUSTOM_CURVES", self.custom_curves),
            ("OUTPUT_CURVES", self.output_curves),
        ]

        for sheet_name, data_method in sheet_configs:
            df = data_method()
            if not df.empty:
                add_frame(sheet_name, df.fillna(""), workbook, column_width=18)

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
    def from_excel(cls, filepath: str | PathLike):
        # TODO: Make the sheet name an attr on the packs,
        # which user can customize if needed, from args in this method!!
        packer = cls()

        with pd.ExcelFile(filepath) as xlsx:
            # Open main tab - create scenarios from there
            scenarios = packer.scenarios_from_df(packer.read_sheet(xlsx, "MAIN", index_col=0))

            # TODO: add some kind of IF, is the inputs sheet available?
            packer._inputs.add(*scenarios)
            packer._inputs.from_dataframe(packer.read_sheet(xlsx, "PARAMETERS", header=[0,1], index_col=[0,1]))

            # TODO: continue for sortables, curves and gqueries


    @staticmethod
    def scenarios_from_df(df: pd.DataFrame) -> list["Scenario"]:
        """Converts one df into a list of scenarios"""
        return [ScenarioPacker.setup_scenario(title, data) for title, data in df.to_dict().items()]

    @staticmethod
    def setup_scenario(title, data):
        """Returns a scenario from data dict"""
        # TODO: take care of NaN values in data(frame)! Make sure they'll be None!
        # TODO: when there is no id in the data, we should call 'new'
        # else 'load' + 'update_metadata'
        scenario = Scenario.load(data['id'])
        # TODO: update metadata with the rest of the stuff in data!!
        scenario.title = title
        return scenario

    # NOTE: Move to utils?
    # Straight from Rob
    @staticmethod
    def read_sheet(
        xlsx: pd.ExcelFile,
        sheet_name: str,
        required: bool = True,
        **kwargs
    ) -> pd.Series:
        """read list items"""

        if not sheet_name in xlsx.sheet_names:
            if required:
                raise ValueError(f"Could not load required sheet '{sheet_name}' from {xlsx.io}")
            logger.warning("Could not load optional sheet '%s' from '%s'", sheet_name, xlsx.io)
            return pd.Series(name=sheet_name, dtype=str)

        values = pd.read_excel(xlsx, sheet_name, **kwargs).squeeze(axis=1)
        # if not isinstance(values, pd.Series):
        #     raise TypeError("Unexpected Outcome")

        return values#.rename(sheet_name)
