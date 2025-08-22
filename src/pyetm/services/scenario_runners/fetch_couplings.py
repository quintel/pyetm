from typing import Any, Dict

from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchCouplingsRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for reading the coupling information of a scenario.

    GET /api/v3/scenarios/{scenario_id}
    """

    COUPLING_KEYS = [
        "active_couplings",
        "inactive_couplings",
    ]

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        result = FetchCouplingsRunner._make_request(
            client=client, method="get", path=f"/scenarios/{scenario.id}"
        )

        if not result.success:
            return result

        # Extract coupling-related data from response
        body = result.data
        coupling_data: Dict[str, Any] = {}
        warnings: list[str] = []

        for key in FetchCouplingsRunner.COUPLING_KEYS:
            if key in body:
                coupling_data[key] = body[key]
            else:
                # non-breaking: warning for missing coupling data
                coupling_data[key] = None
                warnings.append(f"Missing coupling field in response: {key!r}")

        return ServiceResult.ok(data=coupling_data, errors=warnings)
