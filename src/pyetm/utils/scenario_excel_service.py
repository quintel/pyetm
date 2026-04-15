from pathlib import Path
from typing import List, Optional, Sequence, Union
from os import PathLike
import logging

from pyetm.models.session import Session
from pyetm.models.scenario import Scenario
from pyetm.models.scenario_packer import ScenarioPacker
from pyetm.models.export_data_collection import ExportDataCollection

logger = logging.getLogger(__name__)


class ScenarioExcelService:
    """
    Manage Excel import/export operations.
    """

    @classmethod
    def export_to_excel(
        cls,
        scenarios: List[Union[Session, Scenario]],
        path: PathLike | str,
        *,
        carriers: Optional[Sequence[str]] = None,
        include_inputs: bool | None = None,
        include_sortables: bool | None = None,
        include_custom_curves: bool | None = None,
        include_gqueries: bool | None = None,
        include_hourly_output_curves: bool | None = None,
    ) -> None:
        """
        Export scenarios to Excel file.

        Accepts both Scenario and SavedScenario objects. SavedScenarios will export
        their underlying session data.

        Args:
            scenarios: List of Scenario or SavedScenario objects to export
            path: Output file path
            **kwargs: Export configuration options
        """
        if not scenarios:
            raise ValueError("No scenarios provided for export")

        # Use ScenarioPacker for DataFrame conversion
        packer = ScenarioPacker()
        packer.add(*scenarios)

        out_path = Path(path).expanduser().resolve()
        packer.to_excel(
            str(out_path),
            carriers=carriers,
            include_inputs=include_inputs,
            include_sortables=include_sortables,
            include_custom_curves=include_custom_curves,
            include_gqueries=include_gqueries,
            include_hourly_output_curves=include_hourly_output_curves,
        )

    @classmethod
    def collect_export_data(
        cls,
        scenarios: List[Union[Session, Scenario]],
        *,
        carriers: Optional[Sequence[str]] = None,
        include_inputs: Optional[bool] = None,
        include_sortables: Optional[bool] = None,
        include_custom_curves: Optional[bool] = None,
        include_gqueries: Optional[bool] = None,
        include_hourly_output_curves: Optional[bool] = None,
        include_users: Optional[bool] = None,
        include_annual_exports: Optional[Sequence[str]] = None,
    ) -> ExportDataCollection:
        """
        Collect export data from scenarios in a format-agnostic structure.

        This method provides a convenient way to collect data from multiple scenarios
        without writing to any specific file format. The returned ExportDataCollection
        contains pandas DataFrames and dictionaries that can be exported to any format.

        Args:
            scenarios: List of Scenario or Session objects to collect data from
            carriers: Carrier types for hourly output curves (e.g., ["electricity", "heat"])
            include_inputs: Include input parameters
            include_sortables: Include sortable technologies
            include_custom_curves: Include custom curves
            include_gqueries: Include query results
            include_hourly_output_curves: Include hourly output curves
            include_users: Include user permissions
            include_annual_exports: List of annual export names to include

        Returns:
            ExportDataCollection: Format-agnostic collection of all export data

        Raises:
            ValueError: If no scenarios are provided

        Example:
            >>> from pyetm.utils.scenario_excel_service import ScenarioExcelService
            >>>
            >>> # Collect data from scenarios
            >>> export_data = ScenarioExcelService.collect_export_data(
            ...     scenarios=[scenario1, scenario2],
            ...     include_inputs=True,
            ...     include_gqueries=True,
            ...     carriers=["electricity"],
            ...     include_annual_exports=["energy_flow"]
            ... )
            >>>
            >>> # Export to any format
            >>> export_data.main_info.to_parquet("scenarios.parquet")
            >>> export_data.inputs.to_csv("inputs.csv")
            >>>
            >>> # Access specific data
            >>> for curve_name, scenarios_data in export_data.hourly_output_curves.items():
            ...     for scenario_id, df in scenarios_data.items():
            ...         df.to_parquet(f"{curve_name}_{scenario_id}.parquet")
        """
        if not scenarios:
            raise ValueError("No scenarios provided for data collection")

        # Use ScenarioPacker to collect data
        packer = ScenarioPacker()
        packer.add(*scenarios)

        return packer.collect_export_data(
            carriers=carriers,
            include_inputs=include_inputs,
            include_sortables=include_sortables,
            include_custom_curves=include_custom_curves,
            include_gqueries=include_gqueries,
            include_hourly_output_curves=include_hourly_output_curves,
            include_users=include_users,
            include_annual_exports=include_annual_exports,
        )

    @classmethod
    def import_from_excel(
        cls, xlsx_path: PathLike | str, update: bool | List[str] = False
    ) -> List[Session]:
        """
        Import scenarios from Excel file.

        Args:
            xlsx_path: Path to Excel file
            update: If True, upload all data. If list, upload only specified types. If False (default), skip all uploads.
                    Valid types: 'user_values', 'custom_curves', 'sortables'

        Returns:
            List of Scenario objects

        Note:
            SavedScenario objects can be used interchangeably with Scenario objects
            due to delegation. The main difference is persistence metadata.
        """
        path = Path(xlsx_path).expanduser().resolve()
        packer = ScenarioPacker.from_excel(str(path), update=update)
        scenarios = list(packer._scenarios())

        if not scenarios:
            logger.warning(f"No scenarios found in Excel file: {path}")

        return scenarios
