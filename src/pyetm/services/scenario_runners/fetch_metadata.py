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
        "template",
        "url",
    ]

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        result = FetchMetadataRunner._make_request(
            client=client, method="get", path=f"/scenarios/{scenario.id}"
        )

        if not result.success:
            return result

        # Custom post-processing for metadata
        body = result.data
        meta: Dict[str, Any] = {}
        warnings: list[str] = []

        for key in FetchMetadataRunner.META_KEYS:
            if key in body:
                meta[key] = body[key]
            else:
                # non-breaking: warning
                meta[key] = None
                warnings.append(f"Missing field in response: {key!r}")

        return ServiceResult.ok(data=meta, errors=warnings)
