from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class ScenarioUsersDestroyRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for removing users from a Scenario.

    DELETE /api/v3/scenarios/:scenario_id/users
    """

    @staticmethod
    def run(
        client: BaseClient, scenario_id: int, users: List[Dict[str, Any]], **kwargs
    ) -> ServiceResult[List[Dict[str, Any]]]:
        if not users:
            return ServiceResult.fail(["No users provided"])

        errors = []
        for i, user in enumerate(users):
            if "user_id" not in user and "user_email" not in user:
                errors.append(
                    f"User {i}: Must provide either 'user_id' or 'user_email'"
                )

        if errors:
            return ServiceResult.fail(errors)

        payload = {"scenario_users": users}

        return ScenarioUsersDestroyRunner._make_request(
            client=client,
            method="delete",
            path=f"/scenarios/{scenario_id}/users",
            payload=payload,
            **kwargs,
        )
