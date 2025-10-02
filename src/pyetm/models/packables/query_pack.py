import logging
from typing import ClassVar, Any, Dict, Optional, List, Iterable
from xlsxwriter import Workbook
import pandas as pd
from pydantic import PrivateAttr
from pyetm.models.packables.packable import Packable
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class QueryPack(Packable):
    """
    A packable for managing queries (gqueries) across scenarios.
    """

    key: ClassVar[str] = "gquery"
    sheet_name: ClassVar[str] = "GQUERIES"
    output_sheet_name: ClassVar[str] = "GQUERIES_RESULTS"

    _query_keys: list[str] = PrivateAttr(default=[])

    @staticmethod
    def excel_read_kwargs():
        """
        Returns a dict representing the excel read kwargs like the header
        Availabale to overload for users own implementation
        """
        return {"header": None}

    def add(self, *scenarios):
        """Add scenarios and ensure they receive any requested queries."""
        super().add(*scenarios)

        if not scenarios or not self.queries_requested():
            return

        for scenario in scenarios:
            scenario.add_queries(self._query_keys)

    def queries(self) -> List[str]:
        """Get the list of query keys."""
        return self._query_keys.copy()

    def add_queries(self, gquery_keys: Iterable[str]):
        if not gquery_keys:
            return

        self._push_query_keys(*gquery_keys)

        # Apply to existing scenarios
        for scenario in self.scenarios:
            scenario.add_queries(gquery_keys)

    def queries_requested(self) -> bool:
        """Any gqueries requested?"""
        return len(self._query_keys) > 0

    def _build_dataframe_for_scenario(
        self, scenario: Any, columns: str = "future", **kwargs
    ):
        """Build dataframe for a single scenario - the scenario handles query execution."""
        return scenario.results(columns=columns)

    def _to_dataframe(self, columns="future", **kwargs) -> pd.DataFrame:
        """Build dataframe with query results from all scenarios."""
        # TODO: this build one should be private :) why else we have two?
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def add_to_workbook(self, workbook: Workbook, columns: str = "future"):
        """Add gqueries results to workbook."""
        gqueries_df = self.to_dataframe(columns=columns)
        if not gqueries_df.empty:
            self._add_dataframe_to_workbook(
                workbook, self.output_sheet_name, gqueries_df
            )

    def load_from_dataframe(self, df: pd.DataFrame):
        """Import query definitions from dataframe."""
        if df is None or df.empty:
            return

        first_col = df.iloc[:, 0].dropna().astype(str).str.strip()
        filtered = [q for q in first_col if q and q.lower() != "nan"]
        self.add_queries(list(dict.fromkeys(filtered)))

    def clear(self):
        """Clear all scenarios and query definitions."""
        super().clear()
        self._query_keys.clear()

    def _push_query_keys(self, *keys):
        new_keys = set(keys) - set(self._query_keys)
        self._query_keys.extend(list(new_keys))
