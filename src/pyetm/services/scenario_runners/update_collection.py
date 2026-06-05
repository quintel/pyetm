"""Service for updating collections."""

from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateCollectionRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for updating a Collection in MyETM.

    PUT /api/v3/collections/:id
    """

    ALLOWED_KEYS = [
        "title",
        "area_code",
        "end_year",
        "interpolation",
        "discarded",
        "scenario_ids",
        "saved_scenario_ids"
    ]

    @staticmethod
    def run(
        client: BaseClient,
        collection_id: int,
        update_data: Dict[str, Any],
        **kwargs: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update an existing Collection in MyETM.

        Args:
            client: The HTTP client to use
            collection_id: ID of the Collection to update
            update_data: Dictionary with fields to update
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with updated collection data

        Example usage:
            result = UpdateCollectionRunner.run(
                client=client,
                collection_id=123,
                update_data={
                    "title": "Updated Title",
                    "saved_scenario_ids": [1, 2, 3, 4]
                }
            )
        """
        if not isinstance(collection_id, int) or collection_id <= 0:
            return ServiceResult.fail([f"Invalid collection_id: {collection_id}. Must be a positive integer."])

        if not update_data:
            return ServiceResult.fail(["No fields provided for update"])

        filtered_data, warnings = UpdateCollectionRunner._filter_allowed_fields(
            update_data,
            UpdateCollectionRunner.ALLOWED_KEYS,
            "update collection",
        )

        if not filtered_data:
            return ServiceResult.fail(
                ["No valid fields provided for update"] + warnings
            )

        payload = {"collection": filtered_data}

        result = UpdateCollectionRunner._make_request(
            client=client,
            method="put",
            path=f"/collections/{collection_id}",
            payload=payload,
            **kwargs,
        )

        if result.success and warnings:
            combined_errors = list(result.errors) + warnings
            assert result.data is not None, "Success result must have data"
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
