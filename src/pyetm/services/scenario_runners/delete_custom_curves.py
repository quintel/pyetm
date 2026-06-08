"""Service for delete custom curves operations."""

from typing import Any, Dict, List, Set, Union
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DeleteCustomCurvesRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for deleting custom curves from a scenario.

    DELETE /api/v3/scenarios/{scenario_id}/custom_curves/{curve_key}
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_keys: Union[List[str], Set[str]],
        **kwargs: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Execute deletion of custom curves.

        Args:
            client: HTTP client for API calls
            scenario: Scenario object with id attribute
            curve_keys: List or set of curve keys to delete
            **kwargs: Additional arguments for request

        Returns:
            ServiceResult with deleted curve keys and any errors
        """
        curve_keys_list = list(curve_keys) if isinstance(curve_keys, set) else curve_keys

        if not curve_keys_list:
            return ServiceResult.ok(
                data={
                    "deleted_curves": [],
                    "total_curves": 0,
                    "successful_deletions": 0,
                }
            )

        # Build batch delete requests
        requests = [
            {
                "method": "delete",
                "path": f"/scenarios/{scenario.id}/custom_curves/{key}",
                "payload": None,
            }
            for key in curve_keys_list
        ]

        # Execute batch requests
        results = DeleteCustomCurvesRunner._make_batch_requests(client, requests)

        # Process results
        successful_deletions = []
        all_errors = []

        for curve_key, result in zip(curve_keys_list, results):
            if result.success:
                successful_deletions.append(curve_key)
            else:
                for err in result.errors:
                    all_errors.append(f"{curve_key}: {err}")

        return ServiceResult(
            success=len(all_errors) == 0,
            data={
                "deleted_curves": successful_deletions,
                "total_curves": len(curve_keys_list),
                "successful_deletions": len(successful_deletions),
            },
            errors=all_errors,
        )
