"""Service for fetch sortables operations."""

from typing import Any, Dict

from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchSortablesRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for reading all user sortables on a scenario.

    GET /api/v3/scenarios/{scenario_id}/user_sortables
    """

    @staticmethod
    def run(client: BaseClient, scenario: Any, **kwargs: Any) -> ServiceResult[Dict[str, Any]]:
        """Execute the fetch sortables operation.

        Returns:
            ServiceResult[Dict[str, Any]]: Success case contains dict with sortables
                (forecast_storage, hydrogen_supply, hydrogen_demand, space_heating, and
                heat_network with lt/mt/ht subtypes as lists of ints); failure case
                contains error messages.
        """
        return FetchSortablesRunner._make_request(
            client=client, method="get", path=f"/scenarios/{scenario.id}/user_sortables"
        )
