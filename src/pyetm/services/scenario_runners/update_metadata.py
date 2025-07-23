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

    META_KEYS = ["keep_compatible", "private", "source", "metadata"]

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, metadata: Dict[str, Any], **kwargs
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update metadata for a scenario.

        Fields in META_KEYS are set directly on the scenario.
        Other fields are automatically nested under the 'metadata' field.

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
        direct_fields = {}
        nested_metadata = {}

        for key, value in metadata.items():
            if key in UpdateMetadataRunner.META_KEYS:
                direct_fields[key] = value
            else:
                nested_metadata[key] = value

        if nested_metadata:
            # If user also provided a direct "metadata" field, merge with nested fields
            if "metadata" in direct_fields:
                if isinstance(direct_fields["metadata"], dict):
                    direct_fields["metadata"].update(nested_metadata)
                else:
                    direct_fields["metadata"] = nested_metadata
            else:
                direct_fields["metadata"] = nested_metadata

        # Transform metadata to the API format
        payload = {"scenario": direct_fields}

        result = UpdateMetadataRunner._make_request(
            client=client,
            method="put",
            path=f"/scenarios/{scenario.id}",
            payload=payload,
        )

        return result
