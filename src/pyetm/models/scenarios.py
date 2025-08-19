from __future__ import annotations
from os import PathLike
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence
from pydantic import Field
from pyetm.models.base import Base
from .scenario import Scenario


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
    async def from_excel(cls, xlsx_path: PathLike | str) -> "Scenarios":
        """
        Load or create scenarios from an Excel workbook and wrap them in Scenarios.
        """
        scenarios = await Scenario.from_excel(xlsx_path)
        return cls(items=scenarios)
