from typing import Any, Dict, Optional
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchInputsRunner:
    """
    Runner for reading *all* inputs on a scenario.

    GET /api/v3/scenarios/{scenario_id}/inputs

    Returns:
        ServiceResult.ok(data) where `data` is a dict like:
            {
                "input_key_1": {
                    "min": float,
                    "max": float,
                    "default": float,
                    "unit": str,
                    …other fields…
                },
                "input_key_2": { … },
                …
            }
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        defaults: Optional[str] = None,
    ) -> ServiceResult[Dict[str, Any]]:
        params = {"defaults": defaults} if defaults else None

        try:
            resp = client.session.get(
                f"/scenarios/{scenario.id}/inputs",
                params=params,
            )

            if resp.ok:
                return ServiceResult.ok(data=resp.json())

            # HTTP-level failure is breaking
            return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

        except Exception as e:
            # any unexpected exception is treated as breaking
            return ServiceResult.fail([str(e)])
