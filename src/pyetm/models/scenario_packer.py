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


    def collective_inputs(self):
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

    # TODO: check which excel workbooks we need later // which tabs
    # ["MAIN", "PARAMETERS", "GQUERIES", "PRICES", "CUSTOM_CURVES"]
    def to_excel(self, path):
        workbook = Workbook(path)

        add_frame(
            "PARAMETERS",
            self.collective_inputs(),
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


