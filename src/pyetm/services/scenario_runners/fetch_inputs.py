"""Service for fetch inputs operations."""

from typing import Any, Dict, Optional
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchInputsRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for reading *all* inputs on a scenario.

    GET /api/v3/scenarios/{scenario_id}/inputs
    """

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, defaults: Optional[str] = None, **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        """Execute the fetch inputs operation.

        Returns:
            ServiceResult[Dict[str, Any]]: Success case contains dict with input
                configurations (keys are input names, values contain min, max, default,
                unit, and other fields); failure case contains error messages.
        """
        params = {"defaults": defaults} if defaults else None

        return FetchInputsRunner._make_request(
            client=client,
            method="get",
            path=f"/scenarios/{scenario.id}/inputs",
            params=params,
        )
