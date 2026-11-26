"""Runner for creating a collection in MyETM."""

from typing import Any, Dict

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class CreateCollectionRunner(BaseRunner[Dict[str, Any]]):
    """
    Creates a collection of saved scenarios on one Version.

    POST /api/v4/collections
    """

    REQUIRED_KEYS = ["title", "version", "saved_scenario_ids"]

    @staticmethod
    def run(client: BaseClient, collection_data: Dict[str, Any], **kwargs: Any) -> ServiceResult[Dict[str, Any]]:
        errors = CreateCollectionRunner._validate_required_fields(
            collection_data, CreateCollectionRunner.REQUIRED_KEYS
        )
        if errors:
            return ServiceResult.fail(errors)

        filtered_data, warnings = CreateCollectionRunner._filter_allowed_fields(
            collection_data, CreateCollectionRunner.REQUIRED_KEYS, "create collection"
        )
        result = CreateCollectionRunner._make_myetm_request(
            client, "post", "/collections", payload={"collection": filtered_data}, **kwargs
        )
        return result.with_warnings(warnings)
