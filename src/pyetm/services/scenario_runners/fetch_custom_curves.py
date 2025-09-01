import io
from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.scenario_runners.fetch_curves_generic import (
    GenericCurveDownloadRunner,
)
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DownloadCustomCurveRunner(BaseRunner[io.StringIO]):
    """
    Runner for downloading a specific custom curve as CSV data.
    GET /api/v3/scenarios/{scenario_id}/custom_curves/{curve_name}.csv

    Returns:
        ServiceResult.ok(data) where `data` is a StringIO object containing the CSV data.
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_name: str,
    ) -> ServiceResult[Any]:
        return GenericCurveDownloadRunner.run(
            client, scenario, curve_name, curve_type="custom"
        )


class FetchAllCustomCurveDataRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for fetching metadata for all custom curves on a scenario.
    GET /api/v3/scenarios/{scenario_id}/custom_curves

    Returns:
        ServiceResult.ok(data) where `data` is a dict containing curve metadata.
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        return FetchAllCustomCurveDataRunner._make_request(
            client=client, method="get", path=f"/scenarios/{scenario.id}/custom_curves"
        )
