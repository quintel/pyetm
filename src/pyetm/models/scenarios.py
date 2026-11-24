"""Collection and bulk operations for scenarios."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator, Sequence
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import pandas as pd
from pydantic import Field, PrivateAttr

from pyetm.clients import BaseClient, get_client
from pyetm.models.base import Base
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.types import AnnualExportType, CarrierType

from .scenario import Scenario, ScenarioError, apply_data

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyetm.models.scenario_packer import ScenarioPacker

DATA_PARAMS = ("user_values", "custom_curves", "sortables")


class ScenarioCreationParams(TypedDict, total=False):
    """
    Parameters for one scenario in Scenarios.create_many(): a title, and either an area_code and
    end_year, or a source saved scenario ID to copy.
    """

    title: str | None
    area_code: str | None
    end_year: int | None
    source: int | None
    private: bool | None
    user_values: dict[str, Any] | None
    custom_curves: dict[str, Any] | None
    sortables: dict[str, list[Any]] | None


class Scenarios(Base):
    """A collection of Scenarios, for bulk operations and exports."""

    items: list[Scenario] = Field(default_factory=list)
    _packer: ScenarioPacker | None = PrivateAttr(default=None)

    def __iter__(self) -> Iterator[Scenario]:  # type: ignore[override]
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> Scenario:
        return self.items[index]

    def add(self, *scenarios: Scenario) -> None:
        self.items.extend(scenarios)

    def extend(self, scenarios: Iterable[Scenario]) -> None:
        self.items.extend(list(scenarios))

    @property
    def combine(self) -> ScenarioPacker:
        """
        A packer over every scenario, e.g. scenarios.combine.inputs()
        or scenarios.combine.to_excel().
        """
        from pyetm.models.scenario_packer import ScenarioPacker

        if self._packer is None:
            self._packer = ScenarioPacker()
            self._packer.add(*self.items)
        return self._packer

    def get_hourly_output_curves(self, carrier_type: CarrierType) -> dict[str, dict[str, pd.DataFrame]]:
        """Hourly output curves for every scenario by carrier (electricity, heat, hydrogen, methane)."""
        for item in self.items:
            try:
                item.get_hourly_curves([carrier_type])
            except Exception as e:
                logger.warning("Failed to fetch %s curves for scenario %s: %s", carrier_type, item.identifier(), e)

        return self.combine.hourly_output_curves(carrier_type)

    def get_annual_exports(
        self, exports: AnnualExportType | Sequence[AnnualExportType] | None = None
    ) -> dict[str, dict[str, pd.DataFrame]]:
        """Annual exports for every scenario, keyed by export name then scenario."""
        return self.combine.annual_exports(exports)

    @classmethod
    def load_all(cls, client: BaseClient | None = None) -> Scenarios:
        """Every saved scenario the caller can access."""
        from pyetm.services.scenario_runners.fetch_user_saved_scenarios import FetchUserSavedScenariosRunner

        client = client or get_client()
        result = FetchUserSavedScenariosRunner.run(client)
        if not result.success:
            raise ScenarioError(f"Failed to fetch user saved scenarios: {'; '.join(result.errors)}")

        return cls(items=[Scenario._from_myetm(data, client) for data in result.data or []])

    @classmethod
    def load_many(cls, saved_scenario_ids: Iterable[int], client: BaseClient | None = None) -> Scenarios:
        """Load saved scenarios by ID. One that fails to load becomes a warning."""
        scenarios = cls(items=[])
        scenarios.set_bulk_context(True)

        for saved_scenario_id in saved_scenario_ids:
            try:
                scenario = Scenario.load(saved_scenario_id, client=client)
                scenario.set_bulk_context(True)
                scenarios.items.append(scenario)
            except ScenarioError as e:
                scenarios.add_warning("load_many", str(e), severity="error")

        if len(scenarios.warnings) > 0:
            scenarios.show_warnings()
        return scenarios

    @classmethod
    def create_many(
        cls,
        scenario_params: Iterable[ScenarioCreationParams],
        area_code: str | None = None,
        end_year: int | None = None,
        client: BaseClient | None = None,
    ) -> Scenarios:
        """
        Create many scenarios, then send their inputs, curves and sortables concurrently.
        area_code and end_year are defaults for params that give neither them nor a source.
        A scenario that cannot be created becomes a warning.
        """
        client = client or get_client()
        scenarios = cls(items=[])
        scenarios.set_bulk_context(True)

        created: list[tuple[Scenario, dict[str, Any]]] = []
        for params in scenario_params:
            scenario = scenarios._create_one(dict(params), area_code, end_year, client)
            if scenario is not None:
                created.append((scenario, {key: params.get(key) for key in DATA_PARAMS}))

        for warning in apply_data([item for item in created if any(item[1].values())], client):
            scenarios.add_warning("data_application", warning, severity="error")
        for scenario, _ in created:
            scenario.set_bulk_context(True)
            scenarios.items.append(scenario)
            scenarios._merge_submodel_warnings(scenario)

        if len(scenarios.warnings) > 0:
            print("\n=== Batch Creation Summary ===")
            scenarios.show_warnings()
        return scenarios

    def _create_one(
        self, params: dict[str, Any], area_code: str | None, end_year: int | None, client: BaseClient
    ) -> Scenario | None:
        try:
            return self._create_from_params(params, area_code, end_year, client)
        except Exception as e:  # one failed scenario must not stop the rest of the batch
            self.add_warning("create_many", f"Could not create saved scenario with {params}: {e}", severity="error")
            return None

    @staticmethod
    def _create_from_params(
        params: dict[str, Any], area_code: str | None, end_year: int | None, client: BaseClient
    ) -> Scenario:
        title, private = params.get("title"), params.get("private")
        if not title:
            raise ValueError("Missing required 'title'")
        if params.get("source"):
            source = Scenario.load(params["source"], client=client)
            return source.copy_with_preset(title=title, private=private)

        area, year = params.get("area_code") or area_code, params.get("end_year") or end_year
        if area is None or year is None:
            raise ValueError("Missing area_code or end_year. Provide them in each dict or as defaults.")
        return Scenario.new(title=title, area_code=area, end_year=year, private=private, client=client)

    def to_excel(self, path: PathLike[str] | str, **export_options: Any) -> None:
        """Export every scenario to one workbook."""
        if not self.items:
            raise ValueError("No scenarios to export")
        from pyetm.models.scenario_packer import ScenarioPacker

        packer = ScenarioPacker()
        packer.add(*self.items)
        packer.to_excel(str(Path(path).expanduser().resolve()), **export_options)

    @classmethod
    def from_excel(cls, xlsx_path: PathLike[str] | str, update: bool | list[str] = False) -> Scenarios:
        """
        Load, copy or create every scenario a workbook's MAIN sheet describes, and apply its other
        sheets. With update, the sheets named (or all of them, for True) are uploaded.
        """
        from pyetm.models.scenario_packer import ScenarioPacker

        resolved_path = Path(xlsx_path).expanduser().resolve()
        packer = ScenarioPacker.from_excel(str(resolved_path), update=update)
        scenarios = cls(items=sorted(packer._scenarios(), key=lambda s: s.id or 0))
        if not scenarios.items:
            scenarios.add_warning("from_excel", f"No scenarios found in Excel file: {resolved_path}", severity="warning")

        scenarios._packer = packer
        scenarios._merge_submodel_warnings(*scenarios.items, key_attr="id")
        if len(scenarios.warnings) > 0:
            scenarios.show_warnings()
        return scenarios

    @classmethod
    def discard_many(cls, saved_scenario_ids: list[int], client: BaseClient | None = None) -> dict[str, Any]:
        """Discard saved scenarios concurrently. Returns the IDs that succeeded and failed."""
        from pyetm.services.scenario_runners.discard_saved_scenario import DiscardSavedScenarioRunner

        client = client or get_client()
        requests = [DiscardSavedScenarioRunner.build_request(saved_scenario_id=ssid) for ssid in saved_scenario_ids]
        results = BaseRunner._make_batch_requests(client, requests) if requests else []

        outcome: dict[str, Any] = {"successful": [], "failed": []}
        for ssid, result in zip(saved_scenario_ids, results):
            outcome["successful" if result.success else "failed"].append(ssid)
            if not result.success:
                logger.warning("Failed to discard saved scenario %s: %s", ssid, "; ".join(result.errors))
        return outcome
