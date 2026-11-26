"""Runner for fetching a collection from MyETM."""

from typing import Any, Dict

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class FetchCollectionRunner(BaseRunner[Dict[str, Any]]):
    """
    Fetches a collection.

    GET /api/v4/collections/{id}
    """

    @staticmethod
    def run(client: BaseClient, collection_id: int, **kwargs: Any) -> ServiceResult[Dict[str, Any]]:
        return FetchCollectionRunner._make_myetm_request(client, "get", f"/collections/{collection_id}", **kwargs)
