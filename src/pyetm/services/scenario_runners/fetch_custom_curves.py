import io
from typing import Any, Dict
from pyetm.services.scenario_runners.async_base_runner import (
    AsyncBaseRunner,
)
from pyetm.services.scenario_runners.fetch_curves_generic import (
    GenericCurveDownloadRunner,
)
from ..service_result import ServiceResult
from pyetm.clients.async_base_client import AsyncBaseClient


class DownloadCustomCurveRunner(AsyncBaseRunner[io.StringIO]):
    """
    Runner for downloading a specific custom curve as CSV data.
    GET /api/v3/scenarios/{scenario_id}/custom_curves/{curve_name}.csv

    Returns:
        ServiceResult.ok(data) where `data` is a StringIO object containing the CSV data.
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    async def run(
        client: AsyncBaseClient,
        scenario: Any,
        curve_name: str,
    ) -> ServiceResult[io.StringIO]:
        return await GenericCurveDownloadRunner.run(
            client, scenario, curve_name, curve_type="custom"
        )


class FetchAllCustomCurveDataRunner(AsyncBaseRunner[Dict[str, Any]]):
    """
    Runner for fetching metadata for all custom curves on a scenario.
    GET /api/v3/scenarios/{scenario_id}/custom_curves

    Returns:
        ServiceResult.ok(data) where `data` is a dict containing curve metadata.
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    async def run(
        client: AsyncBaseClient,
        scenario: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        return await FetchAllCustomCurveDataRunner._make_request(
            client=client, method="get", path=f"/scenarios/{scenario.id}/custom_curves"
        )
