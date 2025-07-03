from typing import Any, Dict

from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchSortablesRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for reading all user sortables on a scenario.

    GET /api/v3/scenarios/{scenario_id}/user_sortables

    Returns:
        ServiceResult.ok(data) where `data` is a dict like:
            {
              "forecast_storage": List[int],
              "hydrogen_supply":  List[int],
              "hydrogen_demand":  List[int],
              "space_heating":    List[int],
              "heat_network": {
                "lt": List[int],
                "mt": List[int],
                "ht": List[int],
              },
            }

        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        return FetchSortablesRunner._make_request(
            client=client, method="get", path=f"/scenarios/{scenario.id}/user_sortables"
        )
