from __future__ import annotations
from os import PathLike
from typing import Iterable, Iterator, List, Optional, Sequence
from pydantic import BaseModel, Field
from .scenario import Scenario


class Scenarios(BaseModel):
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
        export_output_curves: bool = True,
        output_curves_path: Optional[str] = None,
        carriers: Optional[Sequence[str]] = None,
    ) -> None:
        """
        Export all scenarios in this collection to an Excel workbook.
        Output curves are written to a separate workbook by default (one sheet per carrier).
        """
        from .scenario_packer import ScenarioPacker

        packer = ScenarioPacker()
        if self.items:
            packer.add(*self.items)
        packer.to_excel(
            str(path),
            export_output_curves=export_output_curves,
            output_curves_path=output_curves_path,
            carriers=carriers,
        )

    @classmethod
    def from_excel(cls, xlsx_path: PathLike | str) -> "Scenarios":
        """
        Load or create scenarios from an Excel workbook and wrap them in Scenarios.
        """
        scenarios = Scenario.from_excel(xlsx_path)
        return cls(items=scenarios)
