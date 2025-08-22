from typing import Any, Dict, List, Union

from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateCouplingsRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for updating coupling groups in a scenario.

    POST /api/v3/scenarios/{scenario_id}/couple
    POST /api/v3/scenarios/{scenario_id}/uncouple
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        coupling_groups: List[str],
        action: str = "couple",
        force: bool = False,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update coupling groups for a scenario.

        Args:
            client: The API client
            scenario: The scenario object with an id attribute
            coupling_groups: List of coupling group names to couple/uncouple
            action: Either "couple" or "uncouple"
            force: If True and action is "uncouple", force uncouple all groups
        """
        if action not in ["couple", "uncouple"]:
            return ServiceResult.error(
                errors=[f"Invalid action: {action}. Must be 'couple' or 'uncouple'"]
            )

        # Prepare request data
        data: Dict[str, Union[List[str], bool]] = {"groups": coupling_groups}

        if action == "uncouple" and force:
            data["force"] = True

        result = UpdateCouplingsRunner._make_request(
            client=client,
            method="post",
            path=f"/scenarios/{scenario.id}/{action}",
            json=data,
        )

        if not result.success:
            return result

        # The response should be the updated scenario data
        body = result.data
        coupling_data: Dict[str, Any] = {}
        warnings: list[str] = []

        # Extract relevant coupling information from the response
        coupling_keys = [
            "active_couplings",
            "inactive_couplings",
        ]

        for key in coupling_keys:
            if key in body:
                coupling_data[key] = body[key]
            else:
                coupling_data[key] = None
                warnings.append(f"Missing coupling field in response: {key!r}")

        return ServiceResult.ok(data=coupling_data, errors=warnings)
