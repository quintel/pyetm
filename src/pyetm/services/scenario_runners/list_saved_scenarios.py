from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class ListSavedScenariosRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for listing saved scenarios belonging to the authenticated user.

    GET /api/v3/saved_scenarios?page=<page>&limit=<limit>

    ETEngine proxies this to MyETM and hydrates each saved scenario with
    its underlying ETEngine scenario data. The response envelope is:
        { data: [...], links: {} }

    This runner returns only the data array; callers can paginate by
    passing page/limit explicitly.
    """

    # Signature intentionally omits the `scenario` parameter required by
    # BaseRunner.run: this is a collection-level listing endpoint, not a
    # per-scenario operation.
    @staticmethod
    def run(
        client: BaseClient, page: int = 1, limit: int = 25, **kwargs
    ) -> ServiceResult[List[Dict[str, Any]]]:
        limit = max(1, min(limit, 100))

        result = ListSavedScenariosRunner._make_request(
            client=client,
            method="get",
            path="/saved_scenarios",
            payload={"page": page, "limit": limit},
            **kwargs,
        )

        if not result.success:
            return result

        # Unwrap the paginated envelope if present
        data = result.data
        if isinstance(data, dict) and "data" in data:
            data = data["data"]

        if not isinstance(data, list):
            return ServiceResult.fail(
                ["Unexpected response format from /saved_scenarios index"]
            )

        return ServiceResult.ok(data=data)
