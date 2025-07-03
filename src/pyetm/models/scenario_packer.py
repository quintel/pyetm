import pandas as pd

from pydantic import BaseModel
from typing import Optional
from xlsxwriter import Workbook

from pyetm.models import Scenario
from pyetm.utils.excel import add_frame

class ScenarioPacker(BaseModel):
    '''Packs one or multiple scenarios for export to dataframes or excel'''

    # To avoid keeping all in memory, the packer only remembers which scenarios
    # to pack what info for later
    _curves:    Optional[list['Scenario']] = []
    _inputs:    Optional[list['Scenario']] = []
    _sortables: Optional[list['Scenario']] = []


    def add_curves(self, *scenarios):
        self._curves.extend(scenarios)

    def add_inputs(self, *scenarios):
        self._inputs.extend(scenarios)

    def add_sortables(self, *scenarios):
        self._sortables.extend(scenarios)

    def main_info(self):
        '''
        Main info to dataframe
        For now just for the first scenario!!
        '''
        for scenario in self._scenarios():
            return scenario.to_dataframe()

    def inputs(self):
        '''
        For now just for the first scenario!!
        TODO: think how to combine min/max of different datasets that may
        appear when multiple scenarios are added - maybe make exception for
        same-region collections
        '''
        for scenario in self._inputs:
            # Just return the first one for now - later they need to be combined
            # with a multi-index for different IDs
            return scenario.inputs.to_dataframe()

    def custom_curves(self):
        '''
        Custom curves together!
        For now just for the first scenario!!
        '''
        # TODO: what if it was empty?
        for scenario in self._curves:
            return pd.concat([series for series in scenario.curves_series()], axis=1)


    # TODO: check which excel workbooks we need later // which tabs
    # ["MAIN", "PARAMETERS", "GQUERIES", "PRICES", "CUSTOM_CURVES"]
    def to_excel(self, path):
        # TODO: extend workbook class to allow add frame to be called on it...?
        workbook = Workbook(path, {"nan_inf_to_errors": True})

        add_frame(
            "MAIN",
            self.main_info(),
            workbook
        )

        add_frame(
            "PARAMETERS",
            self.inputs(),
            workbook,
            # index_width=[80, 18],
            # column_width=18
        )

        add_frame(
            "CUSTOM_CURVES",
            self.custom_curves(),
            workbook,
            # index_width=[80, 18],
            # column_width=18
        )

        workbook.close()


    def _scenarios(self) -> set['Scenario']:
        '''
        All scenarios we are packing info for: for these we need to insert
        their metadata
        '''
        return set(self._curves + self._inputs + self._sortables)


