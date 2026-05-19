from typing import Any, Dict, List, Optional
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchUserScenariosRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for fetching the authenticated user's sessions (ETEngine scenarios).

    GET /api/v3/scenarios
    """

    @staticmethod
    def run(
        client: BaseClient,
        page: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Fetch a list of scenarios (sessions) for the authenticated user.

        Args:
            client: HTTP client
            page: Page number for pagination
            limit: Number of results per page (default 25, max 100)

        Returns:
            ServiceResult with list of scenario data dicts
        """
        params = {}
        if page is not None:
            params["page"] = page
        if limit is not None:
            params["limit"] = limit

        result = FetchUserScenariosRunner._make_request(
            client=client,
            method="get",
            path="/scenarios",
            payload=params or None,
        )

        if not result.success:
            return result

        return ServiceResult.ok(data=result.data.get("data", []))
