from __future__ import annotations
from os import PathLike
from pathlib import Path
from typing import Iterable, Iterator, List
from pydantic import Field
from pyetm.models.base import Base
from .session import Session, ScenarioError
from pathlib import Path


class Sessions(Base):
    """
    A collection of Scenario objects
    """

    items: List[Session] = Field(default_factory=list)

    def __iter__(self) -> Iterator[Session]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> Session:
        return self.items[index]

    def add(self, *scenarios: Session) -> None:
        self.items.extend(scenarios)

    def extend(self, scenarios: Iterable[Session]) -> None:
        self.items.extend(list(scenarios))

    @classmethod
    def load_many(cls, scenario_ids: Iterable[int]) -> "Sessions":
        scenarios = []
        for sid in scenario_ids:
            try:
                scenarios.append(Session.load(sid))
            except ScenarioError as e:
                print(f"Could not load scenario {sid}: {e}")
        return cls(items=scenarios)

    @classmethod
    def create_many(
        cls,
        scenario_params: Iterable[dict],
        area_code: str | None = None,
        end_year: int | None = None,
    ) -> "Sessions":
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
                scenarios.append(Session.new(area, year, **extra))
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
    def from_excel(cls, xlsx_path: PathLike | str) -> "Sessions":
        """
        Import scenarios (Sessions) from Excel file.

        Only loads scenarios where the 'session' column is True.
        SavedScenarios (session=False or missing) are ignored.

        Args:
            xlsx_path: Path to Excel file

        Returns:
            Scenarios collection containing only Session instances
        """
        from pyetm.models.scenario_packer import ScenarioPacker
        from pyetm.models.scenario import Scenario

        resolved_path = Path(xlsx_path).expanduser().resolve()
        packer = ScenarioPacker.from_excel(str(resolved_path))
        all_scenarios = list(packer._scenarios())
        sessions = [s for s in all_scenarios if not isinstance(s, Scenario)]

        if not sessions:
            print(f"No Sessions found in Excel file: {resolved_path}")

        sessions.sort(key=lambda s: s.id if hasattr(s, "id") else 0)
        return cls(items=sessions)
