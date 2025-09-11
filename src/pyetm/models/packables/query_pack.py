import logging
from typing import ClassVar, Any, Dict, Optional, List, Iterable
from xlsxwriter import Workbook
import pandas as pd
from pyetm.models.packables.packable import Packable
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class QueryPack(Packable):
    key: ClassVar[str] = "gquery"
    sheet_name: ClassVar[str] = "GQUERIES"
    output_sheet_name: ClassVar[str] = "GQUERIES_RESULTS"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._query_definitions: List[str] = []
        self._requested_queries: List[str] = []

    def add(self, *scenarios):
        """Add scenarios and ensure they receive any requested queries."""
        super().add(*scenarios)
        if not scenarios:
            return
        # Apply effective queries to newly added scenarios (dedup automatically in Scenario)
        effective = self._effective_query_keys()
        if effective:
            for scenario in scenarios:
                try:
                    scenario.add_queries(list(effective))
                except Exception:
                    pass

    def get_query_definitions(self) -> List[str]:
        """Get the list of query definitions."""
        return self._query_definitions.copy()

    def add_queries(self, gquery_keys: Iterable[str]):
        if not gquery_keys:
            return
        # Maintain insertion order and deduplicate
        existing = set(self._requested_queries)
        for q in gquery_keys:
            if q and q not in existing:
                self._requested_queries.append(q)
                existing.add(q)

        # Apply to existing scenarios
        for scenario in list(self.scenarios):
            try:
                scenario.add_queries(list(gquery_keys))
            except Exception:
                pass

    def queries_requested(self) -> bool:
        return len(self._effective_query_keys()) > 0

    def execute_queries(self):
        for scenario in list(self.scenarios):
            try:
                if scenario.queries_requested():
                    scenario.execute_queries()
            except Exception:
                pass

    def _build_dataframe_for_scenario(
        self, scenario: Any, columns: str = "future", **kwargs
    ):
        """Build dataframe for a single scenario - the scenario handles query execution."""
        try:
            # Use scenario's results method which handles execution internally
            df = scenario.results(columns=columns)
            self.log_scenario_warnings(scenario, "_queries", "Queries")
            return df
        except Exception as e:
            logger.warning(
                "Failed building gquery results for %s: %s", scenario.identifier(), e
            )
            return None

    def to_dataframe(self, columns="future", **kwargs) -> pd.DataFrame:
        """Build dataframe with query results from all scenarios."""
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def add_to_workbook(self, workbook: Workbook, columns: str = "future"):
        """Add gqueries results to workbook."""
        gqueries_df = self.to_dataframe(columns=columns)
        if not gqueries_df.empty:
            self._add_dataframe_to_workbook(
                workbook, self.output_sheet_name, gqueries_df
            )

    def import_from_excel(
        self,
        excel_file: pd.ExcelFile,
        main_df: Optional[pd.DataFrame] = None,
        scenarios_by_column: Optional[Dict[str, Any]] = None,
    ):
        """Import gqueries sheet from Excel file and store query definitions."""
        for sheet_name in ("GQUERIES", self.sheet_name):
            df = excel_utils.parse_excel_sheet(excel_file, sheet_name, header=None)
            if df is not None and not df.empty:
                self.from_dataframe(df)
                if self._query_definitions:
                    self.add_queries(self._query_definitions)
                return

    def from_dataframe(self, df: pd.DataFrame):
        """Import query definitions from dataframe."""
        if df is None or df.empty:
            return

        first_col = df.iloc[:, 0].dropna().astype(str).str.strip()
        filtered = [q for q in first_col if q and q.lower() != "nan"]
        unique_queries = list(dict.fromkeys(filtered))

        if unique_queries:
            self._query_definitions = unique_queries
            self.add_queries(unique_queries)

    def export_query_definitions_to_dataframe(self) -> pd.DataFrame:
        """Export current query definitions to a dataframe for Excel export."""
        if not self._query_definitions:
            return pd.DataFrame()

        return pd.DataFrame(self._query_definitions, columns=["Query"])

    def add_queries_sheet_to_workbook(self, workbook: Workbook):
        """Add the queries definition sheet to workbook."""
        queries_df = self.export_query_definitions_to_dataframe()
        if not queries_df.empty:
            self._add_dataframe_to_workbook(workbook, self.sheet_name, queries_df)

    def clear(self):
        """Clear all scenarios and query definitions."""
        super().clear()
        self._query_definitions.clear()

    # --- Helpers ----------------------------------------------------------------------
    def _effective_query_keys(self) -> List[str]:
        """Combined unique list of queries from imported definitions and requested queries."""
        if not self._query_definitions and not self._requested_queries:
            return []
        seen = set()
        combined: List[str] = []
        for q in [*self._requested_queries, *self._query_definitions]:
            if q and q not in seen:
                combined.append(q)
                seen.add(q)
        return combined
