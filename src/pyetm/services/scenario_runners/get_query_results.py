from typing import Any, Dict

from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class GetQueryResultsRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for calculating queries on a scenario.

    PUT /api/v3/scenarios/{scenario_id}

    Returns:
        ServiceResult.ok(data) where `data` is a dict like:
            {
                "query_key_1": 50.0,
                "query_key_2": 1.2,
                …
            }
        ServiceResult.fail(errors) on any breaking error.
    """
    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        gquery_keys: list[str]
    ) -> ServiceResult[Dict[str, Any]]:
        response = GetQueryResultsRunner._make_request(
            client=client,
            method="put",
            path=f"/scenarios/{scenario.id}",
            json={'gqueries': gquery_keys}
        )

        if not response.success:
            return response

        return ServiceResult.ok(data=response.data["gqueries"])
