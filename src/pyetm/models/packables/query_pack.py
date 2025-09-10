import logging
from typing import ClassVar, Any, Dict, Optional
from openpyxl import Workbook
import pandas as pd
from pyetm.models.packables.packable import Packable
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class QueryPack(Packable):
    key: ClassVar[str] = "gquery"
    sheet_name: ClassVar[str] = "GQUERIES"
    output_sheet_name: ClassVar[str] = "GQUERIES_RESULTS"

    def _build_dataframe_for_scenario(
        self, scenario: Any, columns: str = "future", **kwargs
    ):
        try:
            df = scenario.results(columns=columns)
            self.log_scenario_warnings(scenario, "_queries", "Queries")
            return df
        except Exception as e:
            logger.warning(
                "Failed building gquery results for %s: %s", scenario.identifier(), e
            )
            return None

    def _to_dataframe(self, columns="future", **kwargs) -> pd.DataFrame:
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
        """Import gqueries sheet from Excel file."""
        for sheet_name in ("GQUERIES", self.sheet_name):
            df = excel_utils.parse_excel_sheet(excel_file, sheet_name, header=None)
            if df is not None and not df.empty:
                self.from_dataframe(df)
                return

    def from_dataframe(self, df: pd.DataFrame):
        if df is None or df.empty:
            return

        first_col = df.iloc[:, 0].dropna().astype(str).str.strip()
        filtered = [q for q in first_col if q and q.lower() != "nan"]
        unique_queries = list(dict.fromkeys(filtered))

        # Apply unique queries to all scenarios
        if unique_queries:
            for scenario in self.scenarios:
                try:
                    scenario.add_queries(unique_queries)
                finally:
                    self.log_scenario_warnings(scenario, "_queries", "Queries")
