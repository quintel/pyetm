
from typing import Any, Dict, Optional
from ..service_result import ServiceResult, GenericError
from pyetm.clients.base_client import BaseClient
from pyetm.models import Scenario

class FetchInputsRunner:
    """
    Runner for reading *all* inputs on a scenario.

    GET /api/v3/scenarios/{scenario_id}/inputs
    Returns a dict of input_key → input_definition.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: "Scenario",
        defaults: Optional[str] = None,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        :param client:   API client
        :param scenario: domain object with an `id` attribute
        :param defaults: if set to "original", returns dataset defaults instead of inherited
        :returns:        ServiceResult.success=True with `.data` a dict of
                         {input_key: {min, max, default, unit, …}}
        """
        params = {"defaults": defaults} if defaults else None

        try:
            resp = client.session.get(
                f"/scenarios/{scenario.id}/inputs",
                params=params
            )

            if resp.ok:
                return ServiceResult(
                    success=True,
                    data=resp.json(),
                    status_code=resp.status_code
                )

            return ServiceResult(
                success=False,
                errors=[f"{resp.status_code}: {resp.text}"],
                status_code=resp.status_code
            )
        except GenericError as error:
            msg = str(error)
            try:
                code = int(msg.split()[1].rstrip(':'))
            except Exception:
                code = None
            return ServiceResult(
                success=False,
                errors=[msg],
                status_code=code
            )
        except Exception as e:
            #TODO: catch more exceptions
            return ServiceResult(success=False, errors=[str(e)])
