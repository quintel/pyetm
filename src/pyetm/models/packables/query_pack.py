import logging
from typing import ClassVar, Any

import pandas as pd

from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class QueryPack(Packable):
    key: ClassVar[str] = "gquery"
    sheet_name: ClassVar[str] = "GQUERIES"
    output_sheet_name: ClassVar[str] = "GQUERIES_RESULTS"

    def _build_dataframe_for_scenario(
        self, scenario: Any, columns: str = "future", **kwargs
    ):
        try:
            return scenario.results(columns=columns)
        except Exception as e:
            logger.warning(
                "Failed building gquery results for %s: %s", scenario.identifier(), e
            )
            return None

    def _to_dataframe(self, columns="future", **kwargs) -> pd.DataFrame:
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def from_dataframe(self, df: pd.DataFrame):
        if df is None or df.empty:
            return

        first_col = df.iloc[:, 0].dropna().astype(str).str.strip()
        filtered = [q for q in first_col if q and q.lower() != "nan"]
        unique_queries = list(dict.fromkeys(filtered))

        # Apply unique queries to all scenarios
        if unique_queries:
            for scenario in self.scenarios:
                scenario.add_queries(unique_queries)
