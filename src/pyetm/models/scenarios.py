from __future__ import annotations
from os import PathLike
from pathlib import Path
from typing import Iterable, Iterator, List, Union
from pydantic import Field
from pyetm.models.session import Session
from pyetm.models.base import Base
from .scenario import Scenario, SavedScenarioError


class Scenarios(Base):
    """
    A collection of Scenario and Session objects.

    This class can hold both SavedScenario (Scenario) instances from MyETM
    and Session instances from ETEngine. Since Scenario delegates all operations
    to its underlying Session, both types can be used interchangeably.
    """

    items: List[Union[Scenario, Session]] = Field(default_factory=list)

    def __iter__(self) -> Iterator[Union[Scenario, Session]]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> Union[Scenario, Session]:
        return self.items[index]

    def add(self, *scenarios: Union[Scenario, Session]) -> None:
        self.items.extend(scenarios)

    def extend(self, scenarios: Iterable[Union[Scenario, Session]]) -> None:
        self.items.extend(list(scenarios))

    @property
    def sessions(self) -> List["Session"]:
        """
        Get the underlying ETEngine Session objects for all items.

        For Scenario (SavedScenario) instances, returns the underlying session.
        For Session instances, returns them directly.

        Returns:
            List of Session instances
        """
        from pyetm.models.session import Session

        return [
            item.session if isinstance(item, Scenario) else item for item in self.items
        ]

    @classmethod
    def load_many(cls, saved_scenario_ids: Iterable[int]) -> "Scenarios":
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
                saved_scenarios.append(Scenario.load(ssid))
            except SavedScenarioError as e:
                print(f"Could not load saved scenario {ssid}: {e}")
        return cls(items=saved_scenarios)

    def to_excel(self, path: PathLike | str, **export_options) -> None:
        """
        Export all scenarios to Excel.

        Exports both Scenario (SavedScenario) and Session instances.
        For SavedScenario instances, the scenario_id column will contain
        the MyETM SavedScenario ID. For Session instances, it will contain
        the ETEngine session ID.

        Args:
            path: Output path for the Excel file
            **export_options: Additional export options to pass to ScenarioExcelService
        """
        from pyetm.utils.scenario_excel_service import ScenarioExcelService

        if not self.items:
            raise ValueError("No scenarios to export")

        resolved_path = Path(path).expanduser().resolve()
        ScenarioExcelService.export_to_excel(
            self.items, str(resolved_path), **export_options
        )

    @classmethod
    def from_excel(cls, xlsx_path: PathLike | str) -> "Scenarios":
        """
        Import all scenarios from Excel file.

        Loads both SavedScenarios (Scenario instances from MyETM) and Sessions
        (Session instances from ETEngine) based on the 'session' column value.
        All scenarios are included regardless of type.

        Args:
            xlsx_path: Path to Excel file

        Returns:
            Scenarios collection containing all loaded scenarios (both types)
        """
        from pyetm.models.scenario_packer import ScenarioPacker

        resolved_path = Path(xlsx_path).expanduser().resolve()

        packer = ScenarioPacker.from_excel(str(resolved_path))
        all_scenarios = list(packer._scenarios())

        if not all_scenarios:
            print(f"No scenarios found in Excel file: {resolved_path}")

        all_scenarios.sort(key=lambda s: s.id)
        return cls(items=all_scenarios)
