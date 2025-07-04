import io
from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.scenario_runners.fetch_curves_generic import (
    GenericCurveBulkRunner,
    GenericCurveDownloadRunner,
)
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DownloadCarrierCurveRunner(BaseRunner[io.StringIO]):
    """Download a specific output curve."""

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_name: str,
    ) -> ServiceResult[io.StringIO]:
        return GenericCurveDownloadRunner.run(
            client, scenario, curve_name, curve_type="output"
        )


class FetchAllCarrierCurvesRunner(BaseRunner[Dict[str, io.StringIO]]):
    """Download all known output curves."""

    # Known curve types from the Rails controller
    CURVE_TYPES = [
        "merit_order",
        "electricity_price",
        "heat_network",
        "agriculture_heat",
        "household_heat",
        "buildings_heat",
        "hydrogen",
        "network_gas",
        "residual_load",
        "hydrogen_integral_cost",
    ]

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
    ) -> ServiceResult[Dict[str, io.StringIO]]:
        return GenericCurveBulkRunner.run(
            client,
            scenario,
            FetchAllCarrierCurvesRunner.CURVE_TYPES,
            curve_type="output",
        )
