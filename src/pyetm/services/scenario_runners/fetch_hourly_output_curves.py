"""Service for fetch hourly output curves operations."""

from __future__ import annotations

import io
from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.scenario_runners.fetch_curves_generic import (
    GenericCurveBulkRunner,
    GenericCurveDownloadRunner,
)
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DownloadHourlyOutputCurveRunner(BaseRunner[io.StringIO]):
    """Download a specific hourly output curve."""

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, curve_name: str, **kwargs: Any
    ) -> ServiceResult[Any]:
        return GenericCurveDownloadRunner.run(client, scenario, curve_name, curve_type="output")


class FetchAllHourlyOutputCurvesRunner(BaseRunner[Dict[str, io.StringIO]]):
    """Download all known hourly output curves."""

    # Known curve types from the Rails controller
    CURVE_TYPES = [
        "electricity_profiles",
        "electricity_price",
        "heat_network_profiles",
        "agriculture_heat",
        "household_heat",
        "buildings_heat",
        "hydrogen_profiles",
        "network_gas_profiles",
        "residual_load",
        "hydrogen_integral_cost",
        "electricity_capacities",
        "heat_network_capacities",
        "hydrogen_capacities",
        "network_gas_capacities",
    ]

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, batch_size: int | None = None, **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        return GenericCurveBulkRunner.run(
            client,
            scenario,
            FetchAllHourlyOutputCurvesRunner.CURVE_TYPES,
            curve_type="output",
            batch_size=batch_size,
        )
