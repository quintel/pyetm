from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateSortablesRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for updating a single user sortable on a scenario.

    PUT /api/v3/scenarios/{scenario_id}/user_sortables/{sortable_type}
    PUT /api/v3/scenarios/{scenario_id}/user_sortables/{sortable_type}?subtype={subtype}

    Args:
        client: The HTTP client to use
        scenario: The scenario object (must have an 'id' attribute)
        sortable_type: The type of sortable (e.g., "demand", "heat_network")
        order: The new order for the sortable
        subtype: Optional subtype for heat_network (e.g., "lt", "mt", "ht")
        **kwargs: Additional arguments passed to the request
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        sortable_type: str,
        order: List[Any],
        subtype: str = None,
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update a single sortable for a scenario - the endpoint doesn't handle bulk updates.

        """
        path = f"/scenarios/{scenario.id}/user_sortables/{sortable_type}"
        if subtype:
            path += f"?subtype={subtype}"

        payload = {"order": order}

        return UpdateSortablesRunner._make_request(
            client=client,
            method="put",
            path=path,
            payload=payload,
        )
