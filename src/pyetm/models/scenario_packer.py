import pandas as pd
from pydantic import BaseModel
from typing import Optional, Dict, List, Any, Set, Literal, ClassVar
from xlsxwriter import Workbook

from pyetm.models.base import Base
from pyetm.models import Scenario
from pyetm.utils.excel import add_frame


class Packable(Base):
    scenarios: Optional[set["Scenario"]] = set()
    key: ClassVar[str] = 'base_pack'

    def add(self, *scenarios):
        "Adds one or more scenarios to the packable"
        self.scenarios.update(scenarios)

    def discard(self, scenario):
        "Removes a scenario from the pack"
        self.scenarios.discard(scenario)

    def clear(self):
        self.scenarios = []

    def summary(self) -> dict:
        return {self.key: {'scenario_count': len(self.scenarios)}}

    def to_dataframe(self, values='') -> pd.DataFrame:
        """Convert the pack into a dataframe"""
        if len(self.scenarios) == 0:
            return pd.DataFrame()

        return self._to_dataframe(values=values)

    # private

    def _to_dataframe(self, values='') -> pd.DataFrame:
        """Base: kids should implement this method"""
        return pd.DataFrame()


class InputsPack(Packable):
    key: ClassVar[str] = 'inputs'

    def _to_dataframe(self, values=''):
        return pd.concat(
            [scenario.inputs.to_dataframe() for scenario in self.scenarios],
            axis=1,
            keys=[scenario.id for scenario in self.scenarios]
        )


class QueryPack(Packable):
    key: ClassVar[str] = 'gquery'

    def _to_dataframe(self, values='future') -> pd.DataFrame:
        return pd.concat(
            [scenario.results(values=values) for scenario in self.scenarios],
            axis=1,
            keys=[scenario.id for scenario in self.scenarios],
            copy=False
        )


class SortablePack(Packable):
    key: ClassVar[str] = 'sortables'

    def _to_dataframe(self, values='') -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            return scenario.sortables.to_dataframe()


class CustomCurvesPack(Packable):
    key: ClassVar[str] = 'custom_curves'

    def _to_dataframe(self, values='') -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            series_list = list(scenario.custom_curves_series())
            if len(series_list) == 0:
                continue
            return pd.concat(series_list, axis=1)
        return pd.DataFrame()


class OutputCurvesPack(Packable):
    key: ClassVar[str] = 'output_curves'

    def _to_dataframe(self, values='') -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            series_list = list(scenario.carrier_curves_series())
            if len(series_list) == 0:
                continue
            return pd.concat(series_list, axis=1)
        return pd.DataFrame()


class ScenarioPacker(BaseModel):
    """Packs one or multiple scenarios for export to dataframes or excel"""

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
            [scenario.to_dataframe() for scenario in self._scenarios()],
            axis=1
        )

    def inputs(self) -> pd.DataFrame:
        return self._inputs.to_dataframe()

    def gquery_results(self, values='future') -> pd.DataFrame:
        return QueryPack(scenarios=self._scenarios()).to_dataframe(values=values)

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

        sheet_configs = [
            ("MAIN", self.main_info),
            ("PARAMETERS", self._inputs.to_dataframe),
            ("GQUERIES_RESULTS", self.gquery_results),
            ("SORTABLES", self._sortables.to_dataframe),
            ("CUSTOM_CURVES", self._custom_curves.to_dataframe),
            ("output_CURVES", self._output_curves.to_dataframe),
        ]

        for sheet_name, data_method in sheet_configs:
            df = data_method()
            if not df.empty:
                add_frame(sheet_name, df.fillna(''), workbook, column_width=18)

        workbook.close()

    def _scenarios(self) -> set["Scenario"]:
        """
        All scenarios we are packing info for: for these we need to insert
        their metadata
        """
        return set.union(
            *map(set, (pack.scenarios for pack in self.all_pack_data()))
        )

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
