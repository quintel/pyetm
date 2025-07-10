import pandas as pd

from pydantic import BaseModel
from typing import Optional
from xlsxwriter import Workbook

from pyetm.models import Scenario
from pyetm.utils.excel import add_frame


class ScenarioPacker(BaseModel):
    """Packs one or multiple scenarios for export to dataframes or excel"""

    # To avoid keeping all in memory, the packer only remembers which scenarios
    # to pack what info for later
    _custom_curves: Optional[list["Scenario"]] = []
    _inputs: Optional[list["Scenario"]] = []
    _sortables: Optional[list["Scenario"]] = []
    _carrier_curves: Optional[list["Scenario"]] = []

    def add(self, *scenarios):
        """
        Shorthand method for adding all extractions for the scenario
        """
        self.add_custom_curves(*scenarios)
        self.add_inputs(*scenarios)
        self.add_sortables(*scenarios)
        self.add_carrier_curves(*scenarios)

    def add_custom_curves(self, *scenarios):
        self._custom_curves.extend(scenarios)

    def add_inputs(self, *scenarios):
        self._inputs.extend(scenarios)

    def add_sortables(self, *scenarios):
        self._sortables.extend(scenarios)

    def add_carrier_curves(self, *scenarios):
        self._carrier_curves.extend(scenarios)

    # TODO: NTH – ability to remove data from packer as well

    def main_info(self):
        """
        Main info to dataframe
        For now just for the first scenario!!
        """
        for scenario in self._scenarios():
            return scenario.to_dataframe()

    def inputs(self):
        """
        For now just for the first scenario!!
        TODO: think how to combine min/max of different datasets that may
        appear when multiple scenarios are added - maybe make exception for
        same-region collections
        """
        for scenario in self._inputs:
            # Just return the first one for now - later they need to be combined
            # with a multi-index for different IDs
            return scenario.inputs.to_dataframe()

    def gquery_results(self):
        '''
        For now just for the first scenario!!
        '''
        for scenario in self._scenarios():
            return scenario.results()

    def sortables(self):
        for scenario in self._sortables:
            return scenario.sortables.to_dataframe()

    def custom_curves(self):
        """
        Custom curves together!
        For now just for the first scenario!!
        """
        if len(self._custom_curves) == 0:
            return pd.DataFrame()

        for scenario in self._custom_curves:
            series_list = list(scenario.custom_curves_series())
            if len(series_list) == 0:
                return pd.DataFrame()
            return pd.concat(series_list, axis=1)

    def carrier_curves(self):
        """
        Carrier curves
        For now just for the first scenario!!
        """
        if len(self._carrier_curves) == 0:
            return pd.DataFrame()

        for scenario in self._carrier_curves:
            series_list = list(scenario.carrier_curves_series())
            if len(series_list) == 0:
                return pd.DataFrame()
            return pd.concat(series_list, axis=1)

    # TODO: check which excel workbooks we need later // which tabs
    # ["MAIN", "PARAMETERS", "GQUERIES", "PRICES", "CUSTOM_CURVES"]
    def to_excel(self, path):
        if len(self._scenarios()) == 0:
            raise ValueError("Packer was empty, nothing to export")

        # TODO: extend workbook class to allow add frame to be called on it...?
        workbook = Workbook(path, {"nan_inf_to_errors": True})

        add_frame("MAIN", self.main_info(), workbook)

        if len(self._inputs) > 0:
            add_frame(
                "PARAMETERS",
                self.inputs(),
                workbook,
                # index_width=[80, 18], # Add in when we have multi-index
                column_width=18,
            )

        if any((scenario.queries_requested() for scenario in self._scenarios())):
            add_frame(
                "GQUERIES_RESULTS",
                self.gquery_results(),
                workbook,
                # index_width=[80, 18], # Add in when we have multi-index
                column_width=18
            )

        # "CARRIER_CURVES_RESULTS"

        if len(self._sortables) > 0:
            add_frame(
                "SORTABLES",
                self.sortables(),
                workbook,
                # index_width=[80, 18], # Add in when we have multi-index
                column_width=18,
            )
        if len(self._custom_curves) > 0:
            add_frame(
                "CUSTOM_CURVES",
                self.custom_curves(),
                workbook,
                # index_width=[80, 18],
                # column_width=18
            )
        if len(self._carrier_curves) > 0:
            add_frame(
                "CARRIER_CURVES",
                self.carrier_curves(),
                workbook,
                # index_width=[80, 18],
                # column_width=18
            )

        workbook.close()

    def _scenarios(self) -> set["Scenario"]:
        """
        All scenarios we are packing info for: for these we need to insert
        their metadata
        """
        return set(
            self._custom_curves + self._inputs + self._sortables + self._carrier_curves
        )
