"""Service for permanently deleting an ETEngine scenario/session (hard delete)."""

from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DestroySessionRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for permanently deleting an ETEngine scenario/session (hard delete).

    DELETE /api/v3/scenarios/:id

    WARNING: This is a permanent deletion and cannot be undone.
    """

    @staticmethod
    def build_request(scenario_id: int) -> Dict[str, Any]:
        """
        Build destroy request for concurrent batching.

        Args:
            scenario_id: ID of the ETEngine scenario to permanently delete

        Returns:
            Request dict ready for AsyncBatchRunner
        """
        return {
            "method": "delete",
            "path": f"/scenarios/{scenario_id}",
            "payload": None,
            "kwargs": {},
        }

    @staticmethod
    def run(
        client: BaseClient,
        scenario_id: int,
        **kwargs: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Permanently delete an ETEngine scenario/session (hard delete).

        WARNING: This is a permanent deletion and cannot be undone.

        Args:
            client: The HTTP client to use
            scenario_id: ID of the ETEngine scenario to permanently delete
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with deletion confirmation data

        Example usage:
            result = DestroySessionRunner.run(
                client=client,
                scenario_id=123
            )
            if result.success:
                print("Session permanently deleted")
        """
        if not isinstance(scenario_id, int) or scenario_id <= 0:
            return ServiceResult.fail([f"Invalid scenario_id: {scenario_id}. Must be a positive integer."])

        result = DestroySessionRunner._make_request(
            client=client,
            method="delete",
            path=f"/scenarios/{scenario_id}",
            **kwargs,
        )

        return result
