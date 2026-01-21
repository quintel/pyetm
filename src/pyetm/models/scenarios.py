from __future__ import annotations
from os import PathLike
from pathlib import Path
from typing import Iterable, Iterator, List, Union
from pydantic import Field
from pyetm.models.session import Session
from pyetm.models.base import Base
from pyetm.clients import BaseClient
from .scenario import Scenario, SavedScenarioError


class Scenarios(Base):
    """
    A collection of SavedScenario and/or Session objects.

    Can hold both Scenario and Session objects to support
    mixed collections loaded from Excel or other sources.
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
        Get the underlying ETEngine Session objects from all items.
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

    @classmethod
    def create_many(
        cls,
        scenario_params: Iterable[dict],
        area_code: str | None = None,
        end_year: int | None = None,
        client: BaseClient | None = None,
    ) -> "Scenarios":
        """
        Create multiple SavedScenario objects from parameter dicts.

        If scenario_id is not provided in params, creates a new Session first.

        Args:
            scenario_params: Iterable of dicts, each containing at minimum:
                - title: Title for the saved scenario
                - scenario_id: (Optional) ETEngine scenario ID to save. If not provided,
                  a new Session will be created using area_code and end_year
                - area_code: (Optional if using defaults) Area code for new session
                - end_year: (Optional if using defaults) End year for new session
            area_code: Default area_code for all scenarios (if not in params)
            end_year: Default end_year for all scenarios (if not in params)
            client: Optional BaseClient instance (shared across all creates)

        Returns:
            Scenarios collection containing the created SavedScenario objects
        """
        saved_scenarios = []
        for params in scenario_params:
            title = params.get("title")

            if title is None:
                print(
                    f"Could not create saved scenario with {params}: "
                    "Missing required 'title'"
                )
                continue

            try:
                # If scenario_id is provided, use existing session
                if "scenario_id" in params:
                    saved_scenarios.append(Scenario.create(params, client=client))
                else:
                    # Create new session first
                    area = params.get("area_code") or area_code
                    year = params.get("end_year") or end_year

                    if area is None or year is None:
                        print(
                            f"Could not create saved scenario with {params}: "
                            "Missing area_code or end_year. Provide them in each dict or as defaults."
                        )
                        continue

                    # Extract session creation params
                    session_params = {
                        k: v
                        for k, v in params.items()
                        if k
                        not in (
                            "title",
                            "private",
                            "area_code",
                            "end_year",
                        )
                    }

                    # Create the session
                    session = Session.new(area, year, **session_params)

                    # Extract SavedScenario-specific params
                    saved_scenario_params = {
                        k: v for k, v in params.items() if k == "private"
                    }

                    # Create SavedScenario from the session
                    saved_scenarios.append(
                        Scenario.from_scenario(
                            session, title, client=client, **saved_scenario_params
                        )
                    )

            except (SavedScenarioError, Exception) as e:
                print(f"Could not create saved scenario with {params}: {e}")

        return cls(items=saved_scenarios)

    def to_excel(self, path: PathLike | str, **export_options) -> None:
        """
        Export all scenarios to Excel.

        Note: This exports the underlying session data from each SavedScenario.
        The scenario_id column will contain Scenario IDs (MyETM).
        """
        from pyetm.utils.scenario_excel_service import ScenarioExcelService

        if not self.items:
            raise ValueError("No scenarios to export")

        resolved_path = Path(path).expanduser().resolve()
        ScenarioExcelService.export_to_excel(
            self.items, str(resolved_path), **export_options
        )

    @classmethod
    def from_excel(
        cls, xlsx_path: PathLike | str, update: bool | List[str] = False
    ) -> "Scenarios":
        """
        Import all scenarios from Excel file.

        Loads all scenarios from the Excel file, including both:
        - SavedScenarios (where 'session' column is False or missing)
        - Sessions (where 'session' column is True)

        Returns:
            Scenarios collection containing mixed Scenario and Session objects
        """
        from pyetm.models.scenario_packer import ScenarioPacker

        resolved_path = Path(xlsx_path).expanduser().resolve()

        packer = ScenarioPacker.from_excel(str(resolved_path), update=update)
        all_scenarios = list(packer._scenarios())

        if not all_scenarios:
            print(f"No scenarios found in Excel file: {resolved_path}")

        all_scenarios.sort(key=lambda s: s.id if hasattr(s, "id") else 0)
        return cls(items=all_scenarios)
