from __future__ import annotations
from os import PathLike
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence
from pydantic import Field
from pyetm.models.base import Base
from .scenario import Scenario, ScenarioError


class Scenarios(Base):
    """
    A simple collection of Scenario objects with convenience utilities.
    #TODO: Make a nice repr or stats functions
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
            # Prefer explicit param, then fallback to method default
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

    def to_excel(
        self,
        path: PathLike | str,
        *,
        carriers: Optional[Sequence[str]] = None,
        include_inputs: bool | None = None,
        include_sortables: bool | None = None,
        include_custom_curves: bool | None = None,
        include_gqueries: bool | None = None,
        include_output_curves: bool | None = None,
    ) -> None:
        from .scenario_packer import ScenarioPacker
        from pyetm.utils.paths import PyetmPaths

        packer = ScenarioPacker()
        if self.items:
            packer.add(*self.items)

        resolver = PyetmPaths()
        out_path = resolver.resolve_for_write(path, default_dir="outputs")

        packer.to_excel(
            str(out_path),
            carriers=carriers,
            include_inputs=include_inputs,
            include_sortables=include_sortables,
            include_custom_curves=include_custom_curves,
            include_gqueries=include_gqueries,
            include_output_curves=include_output_curves,
        )

    @classmethod
    def from_excel(cls, xlsx_path: PathLike | str) -> "Scenarios":
        """
        Load or create scenarios from an Excel workbook and wrap them in Scenarios.
        """
        scenarios = Scenario.from_excel(xlsx_path)
        return cls(items=scenarios)
