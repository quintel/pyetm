"""Service for fetch custom curves operations."""

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
    """

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, curve_name: str, **kwargs: Any
    ) -> ServiceResult[Any]:
        """Execute the custom curve download operation.

        Returns:
            ServiceResult[io.StringIO]: Success case contains StringIO with CSV data;
                failure case contains error messages.
        """
        return GenericCurveDownloadRunner.run(client, scenario, curve_name, curve_type="custom")


class FetchAllCustomCurveDataRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for fetching metadata for all custom curves on a scenario.
    GET /api/v3/scenarios/{scenario_id}/custom_curves
    """

    @staticmethod
    def run(client: BaseClient, scenario: Any, **kwargs: Any) -> ServiceResult[Dict[str, Any]]:
        """Execute the fetch all custom curves metadata operation.

        Returns:
            ServiceResult[Dict[str, Any]]: Success case contains dict with curve metadata;
                failure case contains error messages.
        """
        return FetchAllCustomCurveDataRunner._make_request(
            client=client,
            method="get",
            path=f"/scenarios/{scenario.id}/custom_curves",
            payload={"include_internal": "true"},
        )
