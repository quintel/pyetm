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
            # Success: return JSON + 200
            if resp.ok:
                return ServiceResult(
                    success=True,
                    data=resp.json(),
                    status_code=resp.status_code,
                )
            # HTTP error returned as a Response
            return ServiceResult(
                success=False,
                errors=[f"{resp.status_code}: {resp.text}"],
                status_code=resp.status_code,
            )

        except GenericError as error:
            msg = str(error)
            if msg.startswith("HTTP "):
                msg = msg[len("HTTP ") :]
            prefix = msg.split(":", 1)[0]
            token = prefix.split()[-1]
            try:
                code = int(token)
            except ValueError:
                code = None

            return ServiceResult(
                success=False,
                errors=[msg],
                status_code=code,
            )

        except Exception as e:
            # network failures, JSON parse errors, etc
            return ServiceResult(success=False, errors=[str(e)])
