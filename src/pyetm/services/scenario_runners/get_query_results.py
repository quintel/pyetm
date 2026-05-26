"""Service for get query results operations."""

from typing import Any, Dict

from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class GetQueryResultsRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for calculating queries on a scenario.

    PUT /api/v3/scenarios/{scenario_id}
    """

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, gquery_keys: list[str], **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        """Execute the query calculation operation.

        Returns:
            ServiceResult[Dict[str, Any]]: Success case contains dict mapping query keys
                to calculated float values; failure case contains error messages.
        """
        response = GetQueryResultsRunner._make_request(
            client=client,
            method="put",
            path=f"/scenarios/{scenario.id}",
            payload={"gqueries": gquery_keys},
        )

        if not response.success:
            return response

        return ServiceResult.ok(data=response.data["gqueries"])  # type: ignore[index]  # type: ignore[index]  # type: ignore[index]
