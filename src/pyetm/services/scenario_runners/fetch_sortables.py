from typing import Any, Dict
from ..service_result import ServiceResult, GenericError
from pyetm.clients.base_client import BaseClient


class FetchSortablesRunner:
    """
    Runner for reading all sortables on a scenario.

    GET /api/v3/scenarios/{scenario_id}/user_sortables
    Returns a dict of sortable_type → order (and for heat_network a nested dict of subtype → order).
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        :param client:   API client
        :param scenario: domain object with an `id` attribute
        :returns:        ServiceResult.success=True with `.data` a dict matching:
                         {
                           "forecast_storage": [...],
                           "hydrogen_supply":  [...],
                           "hydrogen_demand":  [...],
                           "space_heating":    [...],
                           "heat_network": {
                             "lt": [...],
                             "mt": [...],
                             "ht": [...]
                           }
                         }
        """
        try:
            resp = client.session.get(f"/scenarios/{scenario.id}/user_sortables")

            if resp.ok:
                return ServiceResult(
                    success=True, data=resp.json(), status_code=resp.status_code
                )

            return ServiceResult(
                success=False,
                errors=[f"{resp.status_code}: {resp.text}"],
                status_code=resp.status_code,
            )

        except GenericError as error:
            msg = str(error)
            try:
                code = int(msg.split()[1].rstrip(":"))
            except Exception:
                code = None
            return ServiceResult(success=False, errors=[msg], status_code=code)
        except Exception as e:
            return ServiceResult(success=False, errors=[str(e)])
