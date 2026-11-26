"""Runner for deleting a collection in MyETM."""

from typing import Any

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class DeleteCollectionRunner(BaseRunner[Any]):
    """
    Deletes a collection. Its saved scenarios are untouched.

    DELETE /api/v4/collections/{id}
    """

    @staticmethod
    def run(client: BaseClient, collection_id: int, **kwargs: Any) -> ServiceResult[Any]:
        return DeleteCollectionRunner._make_request(client, "delete", f"/collections/{collection_id}", **kwargs)
