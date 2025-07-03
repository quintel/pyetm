from typing import Any, Dict, Optional
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchInputsRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for reading *all* inputs on a scenario.

    GET /api/v3/scenarios/{scenario_id}/inputs

    Returns:
        ServiceResult.ok(data) where `data` is a dict like:
            {
                "input_key_1": {
                    "min": float,
                    "max": float,
                    "default": float,
                    "unit": str,
                    …other fields…
                },
                "input_key_2": { … },
                …
            }
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        defaults: Optional[str] = None,
    ) -> ServiceResult[Dict[str, Any]]:
        params = {"defaults": defaults} if defaults else None

        return FetchInputsRunner._make_request(
            client=client,
            method="get",
            path=f"/scenarios/{scenario.id}/inputs",
            params=params,
        )
