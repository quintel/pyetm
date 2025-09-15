from pathlib import Path
from typing import List, Optional, Sequence
from os import PathLike
import logging

from pyetm.models.scenario import Scenario
from pyetm.models.scenario_packer import ScenarioPacker

logger = logging.getLogger(__name__)


class ScenarioExcelService:
    """
    Manage Excel import/export operations.
    """

    @classmethod
    def export_to_excel(
        cls,
        scenarios: List[Scenario],
        path: PathLike | str,
        *,
        carriers: Optional[Sequence[str]] = None,
        include_inputs: bool | None = None,
        include_sortables: bool | None = None,
        include_custom_curves: bool | None = None,
        include_gqueries: bool | None = None,
        include_exports: bool | None = None,
    ) -> None:
        """
        Export scenarios to Excel file.

        Args:
            scenarios: List of Scenario objects to export
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
            include_exports=include_exports,
        )

    @classmethod
    def import_from_excel(cls, xlsx_path: PathLike | str) -> List[Scenario]:
        """
        Import scenarios from Excel file.

        Args:
            xlsx_path: Path to Excel file

        Returns:
            List of Scenario objects
        """
        path = Path(xlsx_path).expanduser().resolve()
        packer = ScenarioPacker.from_excel(str(path))
        scenarios = list(packer._scenarios())

        if not scenarios:
            logger.warning(f"No scenarios found in Excel file: {path}")

        return scenarios
