"""Service for fetching a single collection."""

from typing import Dict, Any
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchCollectionRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for fetching a Collection from MyETM by its ID.

    GET /api/v3/collections/{collection_id}
    """

    REQUIRED_KEYS = [
        "id",
        "title",
        "created_at",
        "updated_at",
    ]

    @staticmethod
    def run(
        client: BaseClient,
        collection_id: int,
        **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Fetch a single Collection by ID.

        Args:
            client: HTTP client
            collection_id: ID of the collection to fetch
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with Collection data

        Example usage:
            result = FetchCollectionRunner.run(
                client=client,
                collection_id=123
            )
            if result.success:
                print(f"Collection: {result.data['title']}")
        """
        if not isinstance(collection_id, int) or collection_id <= 0:
            return ServiceResult.fail([f"Invalid collection_id: {collection_id}. Must be a positive integer."])

        result = FetchCollectionRunner._make_request(
            client=client,
            method="get",
            path=f"/collections/{collection_id}",
            **kwargs,
        )

        if not result.success:
            for error in result.errors:
                if "404" in error or "not found" in error.lower():
                    return ServiceResult.fail(
                        [f"Collection {collection_id} not found on this environment"]
                    )
            return result

        if result.data is None:
            return ServiceResult.fail(["No data returned from API"])

        # Check if the API returned an error payload in a successful response
        if isinstance(result.data, dict) and "errors" in result.data:
            errors = result.data.get("errors", [])
            if errors:
                # Check for "not found" errors
                for error in errors:
                    if isinstance(error, str) and "not found" in error.lower():
                        return ServiceResult.fail(
                            [f"Collection {collection_id} not found on this environment"]
                        )
                # Return generic failure with the error messages
                return ServiceResult.fail(errors if isinstance(errors, list) else [str(errors)])

        _, warnings = FetchCollectionRunner._validate_response_keys(
            result.data,
            FetchCollectionRunner.REQUIRED_KEYS,
            fill_missing=False,
        )

        return ServiceResult.ok(data=result.data, errors=warnings)
