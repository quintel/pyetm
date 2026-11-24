"""Runner for updating a collection in MyETM."""

from typing import Any, Dict

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class UpdateCollectionRunner(BaseRunner[Dict[str, Any]]):
    """
    Updates a collection's title or saved scenarios.

    PUT /api/v4/collections/{id}
    """

    ALLOWED_KEYS = ["title", "saved_scenario_ids"]

    @staticmethod
    def run(
        client: BaseClient, collection_id: int, update_data: Dict[str, Any], **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        filtered_data, warnings = UpdateCollectionRunner._filter_allowed_fields(
            update_data, UpdateCollectionRunner.ALLOWED_KEYS, "update collection"
        )
        if not filtered_data:
            return ServiceResult.fail(["No valid fields provided for update"] + warnings)

        result = UpdateCollectionRunner._make_myetm_request(
            client, "put", f"/collections/{collection_id}", payload={"collection": filtered_data}, **kwargs
        )
        return result.with_warnings(warnings)
