"""Service for fetch metadata operations."""

from typing import Any, Dict

from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchMetadataRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for reading just the metadata fields of a scenario.

    GET /api/v3/scenarios/{scenario_id}
    """

    META_KEYS = [
        "id",
        "created_at",
        "updated_at",
        "end_year",
        "keep_compatible",
        "private",
        "area_code",
        "source",
        "metadata",
        "title",
        "start_year",
        "scaling",
        "preset_scenario_id",  # aliased to template_id in Session
        "url",
    ]

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        result = FetchMetadataRunner._make_request(
            client=client, method="get", path=f"/scenarios/{scenario.id}"
        )

        if not result.success:
            return result

        if result.data is None:
            return ServiceResult.fail(["No data returned from API"])

        validated_data, warnings = FetchMetadataRunner._validate_response_keys(
            result.data,
            FetchMetadataRunner.META_KEYS,
            fill_missing=True,
        )

        return ServiceResult.ok(data=validated_data, errors=warnings)
