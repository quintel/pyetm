from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class ScenarioUsersUpdateRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for updating user roles on a Scenario.

    PUT /api/v3/scenarios/:scenario_id/users
    """

    REQUIRED_KEYS = ["role"]

    @staticmethod
    def run(
        client: BaseClient, scenario_id: int, users: List[Dict[str, Any]], **kwargs
    ) -> ServiceResult[List[Dict[str, Any]]]:
        if not users:
            return ServiceResult.fail(["No users provided"])

        errors = []
        for i, user in enumerate(users):
            missing = ScenarioUsersUpdateRunner._validate_required_fields(
                user, ScenarioUsersUpdateRunner.REQUIRED_KEYS
            )
            if missing:
                errors.extend([f"User {i}: {err}" for err in missing])

            if "user_id" not in user and "user_email" not in user:
                errors.append(
                    f"User {i}: Must provide either 'user_id' or 'user_email'"
                )

        if errors:
            return ServiceResult.fail(errors)

        payload = {"scenario_users": users}

        return ScenarioUsersUpdateRunner._make_request(
            client=client,
            method="put",
            path=f"/scenarios/{scenario_id}/users",
            payload=payload,
            **kwargs,
        )
