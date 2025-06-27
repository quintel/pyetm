from typing import Any, Dict
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchSortablesRunner:
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
        """
        :param client:   API client
        :param scenario: domain object with an `id` attribute
        :returns:
          - ServiceResult.ok(data) on success
          - ServiceResult.fail(errors) on any breaking error
        """
        try:
            resp = client.session.get(f"/scenarios/{scenario.id}/user_sortables")

            if resp.ok:
                return ServiceResult.ok(data=resp.json())

            # HTTP-level failure is breaking
            return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

        except Exception as e:
            # any unexpected exception is a breaking error
            return ServiceResult.fail([str(e)])
