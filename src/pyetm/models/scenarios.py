from __future__ import annotations
from os import PathLike
from pathlib import Path
from typing import Iterable, Iterator, List
from pydantic import Field
from pyetm.models.base import Base
from .scenario import Scenario, ScenarioError
from pathlib import Path


class Scenarios(Base):
    """
    A collection of Scenario objects
    """

    items: List[Scenario] = Field(default_factory=list)

    def __iter__(self) -> Iterator[Scenario]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> Scenario:
        return self.items[index]

    def add(self, *scenarios: Scenario) -> None:
        self.items.extend(scenarios)

    def extend(self, scenarios: Iterable[Scenario]) -> None:
        self.items.extend(list(scenarios))

    @classmethod
    def load_many(cls, scenario_ids: Iterable[int]) -> "Scenarios":
        scenarios = []
        for sid in scenario_ids:
            try:
                scenarios.append(Scenario.load(sid))
            except ScenarioError as e:
                print(f"Could not load scenario {sid}: {e}")
        return cls(items=scenarios)

    @classmethod
    def create_many(
        cls,
        scenario_params: Iterable[dict],
        area_code: str | None = None,
        end_year: int | None = None,
    ) -> "Scenarios":
        """Create multiple Scenario objects from parameter dicts."""
        scenarios = []
        for params in scenario_params:
            area = params.get("area_code") or area_code
            year = params.get("end_year") or end_year
            if area is None or year is None:
                print(
                    f"Could not create scenario with {params}: Missing area_code or end_year. Provide them in each dict or as defaults."
                )
                continue
            try:
                extra = {
                    k: v
                    for k, v in params.items()
                    if k not in ("area_code", "end_year")
                }
                scenarios.append(Scenario.new(area, year, **extra))
            except (ScenarioError, ValueError) as e:
                print(f"Could not create scenario with {params}: {e}")
        return cls(items=scenarios)

    def to_excel(self, path: PathLike | str, **export_options) -> None:
        """
        Export all scenarios to Excel.
        """
        from pyetm.utils.scenario_excel_service import ScenarioExcelService

        if not self.items:
            raise ValueError("No scenarios to export")

        resolved_path = Path(path).expanduser().resolve()
        ScenarioExcelService.export_to_excel(
            self.items, str(resolved_path), **export_options
        )

    @classmethod
    def from_excel(cls, xlsx_path: PathLike | str, update: bool | list[str] = False) -> "Scenarios":
        """
        Import scenarios from Excel.

        Args:
            xlsx_path: Path to Excel file
            update: If True, upload all data. If list, upload only specified types. If False (default), skip all uploads.
                    Valid types: 'user_values', 'custom_curves', 'sortables'
        """
        from pyetm.utils.scenario_excel_service import ScenarioExcelService

        resolved_path = Path(xlsx_path).expanduser().resolve()
        scenarios = ScenarioExcelService.import_from_excel(str(resolved_path), update=update)
        scenarios.sort(key=lambda s: s.id)
        return cls(items=scenarios)
