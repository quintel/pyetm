"""Service for deleting a collection."""

from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DeleteCollectionRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for deleting a Collection from MyETM.

    DELETE /api/v3/collections/:id
    """

    @staticmethod
    def run(
        client: BaseClient,
        collection_id: int,
        **kwargs: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Delete a Collection from MyETM.

        Args:
            client: The HTTP client to use
            collection_id: ID of the Collection to delete
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with deletion confirmation data

        Example usage:
            result = DeleteCollectionRunner.run(
                client=client,
                collection_id=123
            )
            if result.success:
                print("Collection deleted successfully")
        """
        if not isinstance(collection_id, int) or collection_id <= 0:
            return ServiceResult.fail([f"Invalid collection_id: {collection_id}. Must be a positive integer."])

        result = DeleteCollectionRunner._make_request(
            client=client,
            method="delete",
            path=f"/collections/{collection_id}",
            **kwargs,
        )

        return result
