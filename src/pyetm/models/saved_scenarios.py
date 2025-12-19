from __future__ import annotations
from os import PathLike
from pathlib import Path
from typing import Iterable, Iterator, List
from pydantic import Field
from models.session import Session
from pyetm.models.base import Base
from .saved_scenario import SavedScenario, SavedScenarioError


class SavedScenarios(Base):
    """
    A collection of SavedScenario objects.
    """

    items: List[SavedScenario] = Field(default_factory=list)

    def __iter__(self) -> Iterator[SavedScenario]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> SavedScenario:
        return self.items[index]

    def add(self, *saved_scenarios: SavedScenario) -> None:
        self.items.extend(saved_scenarios)

    def extend(self, saved_scenarios: Iterable[SavedScenario]) -> None:
        self.items.extend(list(saved_scenarios))

    @property
    def sessions(self) -> List["Session"]:
        """
        Get the underlying ETEngine Scenario objects for all SavedScenarios.

        Returns:
            List of Scenario instances (the underlying sessions)
        """
        from models.session import Session

        return [saved.session for saved in self.items]

    @classmethod
    def load_many(cls, saved_scenario_ids: Iterable[int]) -> "SavedScenarios":
        """
        Load multiple SavedScenario objects by their MyETM saved scenario IDs.

        Args:
            saved_scenario_ids: Iterable of MyETM saved scenario IDs to load

        Returns:
            SavedScenarios collection containing the loaded SavedScenario objects
        """
        saved_scenarios = []
        for ssid in saved_scenario_ids:
            try:
                saved_scenarios.append(SavedScenario.load(ssid))
            except SavedScenarioError as e:
                print(f"Could not load saved scenario {ssid}: {e}")
        return cls(items=saved_scenarios)

    def to_excel(self, path: PathLike | str, **export_options) -> None:
        """
        Export all saved scenarios to Excel.

        Note: This exports the underlying session data from each SavedScenario.
        The scenario_id column will contain SavedScenario IDs (MyETM IDs).
        """
        from pyetm.utils.scenario_excel_service import ScenarioExcelService

        if not self.items:
            raise ValueError("No saved scenarios to export")

        resolved_path = Path(path).expanduser().resolve()
        ScenarioExcelService.export_to_excel(
            self.items, str(resolved_path), **export_options
        )

    @classmethod
    def from_excel(cls, xlsx_path: PathLike | str) -> "SavedScenarios":
        """
        Import SavedScenarios from Excel file.

        Only loads scenarios where the 'session' column is False or missing.
        Scenarios with session=True are ignored.
        """
        from pyetm.models.scenario_packer import ScenarioPacker

        resolved_path = Path(xlsx_path).expanduser().resolve()

        packer = ScenarioPacker.from_excel(str(resolved_path))
        all_scenarios = list(packer._scenarios())
        saved_scenarios = [s for s in all_scenarios if isinstance(s, SavedScenario)]

        if not saved_scenarios:
            print(f"No SavedScenarios found in Excel file: {resolved_path}")

        saved_scenarios.sort(key=lambda s: s.id if hasattr(s, "id") else 0)
        return cls(items=saved_scenarios)
