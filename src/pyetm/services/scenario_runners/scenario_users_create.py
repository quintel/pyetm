from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class ScenarioUsersCreateRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for adding users to a Scenario.

    POST /api/v3/scenarios/:scenario_id/users
    """

    REQUIRED_KEYS = ["user_email", "role"]

    @staticmethod
    def run(
        client: BaseClient, scenario_id: int, users: List[Dict[str, Any]], **kwargs
    ) -> ServiceResult[List[Dict[str, Any]]]:
        if not users:
            return ServiceResult.fail(["No users provided"])

        errors = []
        for i, user in enumerate(users):
            missing = ScenarioUsersCreateRunner._validate_required_fields(
                user, ScenarioUsersCreateRunner.REQUIRED_KEYS
            )
            if missing:
                errors.extend([f"User {i}: {err}" for err in missing])

        if errors:
            return ServiceResult.fail(errors)

        payload = {"scenario_users": users}

        return ScenarioUsersCreateRunner._make_request(
            client=client,
            method="post",
            path=f"/scenarios/{scenario_id}/users",
            payload=payload,
            **kwargs,
        )
