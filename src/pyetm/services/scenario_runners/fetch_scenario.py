from typing import Any, Dict
from ..service_result import ServiceResult, GenericError
from pyetm.clients.base_client import BaseClient


class FetchScenarioRunner:
    """
    Runner for reading the full scenario JSON, including balanced_values.
    GET /api/v3/scenarios/{scenario_id}
    """

    @staticmethod
    def run(client: BaseClient, scenario) -> ServiceResult[Dict[str, Any]]:
        try:
            resp = client.session.get(f"/scenarios/{scenario.id}")
            if resp.ok:
                return ServiceResult(
                    success=True,
                    data=resp.json(),
                    status_code=resp.status_code,
                )
            else:
                return ServiceResult(
                    success=False,
                    errors=[f"{resp.status_code}: {resp.text}"],
                    status_code=resp.status_code,
                )
        except GenericError as e:
            return ServiceResult(success=False, errors=[str(e)])
        except Exception as e:
            return ServiceResult(success=False, errors=[str(e)])
