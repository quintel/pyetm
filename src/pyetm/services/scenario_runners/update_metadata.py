from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateMetadataRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for updating metadata fields on a scenario through the main scenario endpoint.

    PUT /api/v3/scenarios/{scenario_id}

    Args:
        client: The HTTP client to use
        scenario: The scenario object (must have an 'id' attribute)
        metadata: Dictionary of metadata updates to apply
        **kwargs: Additional arguments passed to the request
    """

    # Valid metadata keys that can be updated
    META_KEYS = [
        "end_year",
        "keep_compatible",
        "private",
        "area_code",
        "source",
        "metadata",
        "start_year",
        "scaling",
        "template",
        "url",
    ]

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, metadata: Dict[str, Any], **kwargs
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update metadata for a scenario.

        Example usage:
            result = UpdateMetadataRunner.run(
                client=client,
                scenario=scenario,
                metadata={
                    "end_year": 2050,
                    "private": True,
                    "area_code": "nl",
                }
            )
        """
        # Filter out any keys that can't be updated
        filtered_metadata = {
            key: value
            for key, value in metadata.items()
            if key in UpdateMetadataRunner.META_KEYS
        }

        # Create warnings for any filtered keys
        warnings = []
        filtered_keys = set(metadata.keys()) - set(filtered_metadata.keys())
        for key in filtered_keys:
            warnings.append(f"Ignoring non-updatable metadata field: {key!r}")

        # Transform metadata to the API format
        payload = {"scenario": filtered_metadata}

        result = UpdateMetadataRunner._make_request(
            client=client,
            method="put",
            path=f"/scenarios/{scenario.id}",
            payload=payload,
        )

        if result.success and warnings:
            # Merge our warnings with any from the API call
            combined_errors = list(result.errors) + warnings
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
