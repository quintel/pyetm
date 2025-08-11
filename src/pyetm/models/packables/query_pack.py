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

        # Get the first column and extract non-empty query strings
        first_col = df.iloc[:, 0]
        queries = []

        for value in first_col:
            if pd.notna(value):
                query = str(value).strip()
                if query and query.lower() != "nan":
                    queries.append(query)

        # Remove duplicates while preserving order
        unique_queries = []
        seen = set()
        for query in queries:
            if query not in seen:
                seen.add(query)
                unique_queries.append(query)

        if unique_queries:
            # Apply the same queries to all scenarios in the pack
            for scenario in self.scenarios:
                scenario.add_queries(unique_queries)
