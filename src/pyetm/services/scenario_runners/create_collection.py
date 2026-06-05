"""Service for creating collections."""

from typing import Any, Dict, List, Optional
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class CreateCollectionRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for creating a Collection in MyETM.

    POST /api/v3/collections
    """

    REQUIRED_KEYS = ["title"]
    OPTIONAL_KEYS = ["area_code", "end_year", "interpolation", "scenario_ids", "saved_scenario_ids"]

    @staticmethod
    def run(
        client: BaseClient,
        collection_data: Dict[str, Any],
        **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Create a new Collection in MyETM.

        Args:
            client: The HTTP client to use
            collection_data: Dictionary with collection attributes
                - title (required): Collection title
                - scenario_ids (optional): List of ETEngine scenario IDs
                - saved_scenario_ids (optional): List of MyETM saved scenario IDs
                - area_code (optional): Area code for interpolated collections
                - end_year (optional): End year for interpolated collections
                - interpolation (optional): Whether this is an interpolated collection
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with created collection data

        Example usage:
            result = CreateCollectionRunner.run(
                client=client,
                collection_data={
                    "title": "My Collection",
                    "saved_scenario_ids": [1, 2, 3],
                    "interpolation": False
                }
            )
        """
        errors = CreateCollectionRunner._validate_required_fields(
            collection_data, CreateCollectionRunner.REQUIRED_KEYS
        )

        if errors:
            return ServiceResult.fail(errors)

        # Validate that at least one of scenario_ids or saved_scenario_ids is provided
        has_scenarios = bool(collection_data.get("scenario_ids"))
        has_saved_scenarios = bool(collection_data.get("saved_scenario_ids"))

        if not has_scenarios and not has_saved_scenarios:
            return ServiceResult.fail([
                "Collection must include at least one 'scenario_ids' or 'saved_scenario_ids'"
            ])

        all_allowed = (
            CreateCollectionRunner.REQUIRED_KEYS
            + CreateCollectionRunner.OPTIONAL_KEYS
        )
        filtered_data, warnings = CreateCollectionRunner._filter_allowed_fields(
            collection_data,
            all_allowed,
            "create collection",
        )

        payload = {"collection": filtered_data}

        result = CreateCollectionRunner._make_request(
            client=client,
            method="post",
            path="/collections",
            payload=payload,
            **kwargs,
        )

        if result.success and warnings:
            combined_errors = list(result.errors) + warnings
            assert result.data is not None, "Success result must have data"
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
