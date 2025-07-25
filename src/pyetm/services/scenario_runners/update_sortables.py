from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateSortablesRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for updating sortable orders on a scenario through the user sortables endpoint.
    PUT /api/v3/scenarios/{scenario_id}/user_sortables/{sortable}
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update the order of a sortable type for a scenario.

        Required kwargs:
            sortable: str - Type of sortable to update
            order: List[str] - List of items in desired order
        Optional kwargs:
            subtype: str - Subtype for heat_network (lt, mt, ht)
        """
        # Extract required parameters from kwargs
        sortable = kwargs.pop("sortable")
        order = kwargs.pop("order")
        subtype = kwargs.pop("subtype", None)

        path = f"/scenarios/{scenario.id}/user_sortables/{sortable}"

        params = {}
        if subtype:
            params["subtype"] = subtype

        payload = {"order": order}

        return UpdateSortablesRunner._make_request(
            client=client,
            method="put",
            path=path,
            payload=payload,
            params=params if params else None,
            **kwargs,
        )
