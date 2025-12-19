import logging
from typing import ClassVar, Any
import pandas as pd
from pyetm.models.packables.packable import Packable
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class SortablePack(Packable):
    """
    A packable for managing sortables data.

    SortablePack handles the extraction, processing, and application of sortable data
    for scenarios.
    """

    key: ClassVar[str] = "sortables"
    sheet_name: ClassVar[str] = "SORTABLES"

    def _build_dataframe_for_scenario(self, scenario: Any, columns: str = "", **kwargs):
        try:
            df = scenario.sortables.to_dataframe()
            self.log_scenario_warnings(scenario, "_sortables", "Sortables")
        except Exception as e:
            logger.warning(
                "Failed extracting sortables for %s: %s", scenario.identifier(), e
            )
            return None
        return df if not df.empty else None

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def import_scenario_specific_sheet(
        self, excel_file: pd.ExcelFile, sheet_name: str, scenario: "Any", update_set: set[str] = None
    ):
        """Import sortables from a scenario-specific sheet."""
        df = excel_utils.parse_excel_sheet(excel_file, sheet_name, header=None)
        if df is not None and not df.empty:
            self.process_single_scenario_sortables(scenario, df, update_set)

    def process_single_scenario_sortables(self, scenario: "Any", df: pd.DataFrame, update_set: set[str] = None):
        """Process sortables data for a single scenario."""
        normalized_data = excel_utils.normalize_sheet(
            df,
            helper_names={"sortables", "hour", "index"},
            reset_index=True,
            rename_map={"heat_network": "heat_network_lt"},
        )

        if normalized_data is None or normalized_data.empty:
            return

        self.apply_sortables_to_scenario(scenario, normalized_data, update_set)

    def apply_sortables_to_scenario(self, scenario: "Any", data: pd.DataFrame, update_set: set[str] = None):
        """Apply sortables data to scenario with error handling."""
        skip_upload = not self._should_include_upload(update_set)

        try:
            scenario.set_sortables_from_dataframe(data, skip_upload=skip_upload)
            self.log_scenario_warnings(scenario, "_sortables", "Sortables")
        except Exception as e:
            logger.warning(
                "Failed processing sortables for '%s': %s", scenario.identifier(), e
            )

    def from_dataframe(self, df: pd.DataFrame):
        """Unpack and update sortables for each scenario from the sheet."""
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = self._normalize_single_header_sheet(
                df,
                helper_columns={"sortables"},
                drop_empty=True,
                reset_index=False,
            )
        except Exception as e:
            logger.warning("Failed to normalize sortables sheet: %s", e)
            return
        if df is None or df.empty:
            return

        def _apply(scenario, block: pd.DataFrame):
            scenario.set_sortables_from_dataframe(block)
            self.log_scenario_warnings(scenario, "_sortables", "Sortables")

        if isinstance(df.columns, pd.MultiIndex):
            self.apply_identifier_blocks(df, _apply)
        else:
            for scenario in self.scenarios:
                _apply(scenario, df)
