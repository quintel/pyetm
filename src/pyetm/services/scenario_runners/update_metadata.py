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

    META_KEYS = [
        "keep_compatible",
        "private",
        "source",
        "metadata",
        "end_year",
        "title",
        "template",
    ]
    UNSETTABLE_META_KEYS = [
        "id",
        "created_at",
        "updated_at",
        "area_code",
        "start_year",
        "scaling",
        "url",
    ]

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, metadata: Dict[str, Any], **kwargs
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update metadata for a scenario.

        Fields in META_KEYS are set directly on the scenario.
        Other fields are automatically merged and nested under the 'metadata' field.

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
        warnings = []

        for key, value in metadata.items():
            if key in UpdateMetadataRunner.META_KEYS:
                direct_fields[key] = value
            elif key in UpdateMetadataRunner.UNSETTABLE_META_KEYS:
                # Field exists on scenario but cannot be updated - add to nested metadata with warning
                nested_metadata[key] = value
                warnings.append(
                    f"Field '{key}' cannot be updated directly and has been added to nested metadata instead"
                )
            else:
                nested_metadata[key] = value

        existing_metadata = {}
        if hasattr(scenario, "metadata") and isinstance(scenario.metadata, dict):
            existing_metadata = scenario.metadata.copy()

        legacy_title = None
        if isinstance(existing_metadata, dict):
            legacy_title = existing_metadata.pop("title", None)
        if "metadata" in direct_fields and isinstance(direct_fields["metadata"], dict):
            direct_fields["metadata"].pop("title", None)
        nested_metadata.pop("title", None)
        if legacy_title is not None and "title" not in direct_fields:
            direct_fields["title"] = legacy_title

        final_metadata = existing_metadata.copy()
        if nested_metadata:
            final_metadata.update(nested_metadata)

        # If user provided a direct "metadata" field, merge it in last (highest priority)
        if "metadata" in direct_fields:
            if isinstance(direct_fields["metadata"], dict):
                final_metadata.update(direct_fields["metadata"])

        if final_metadata or nested_metadata or "metadata" in direct_fields:
            direct_fields["metadata"] = final_metadata

        if "template" in direct_fields:
            direct_fields["preset_scenario_id"] = direct_fields.pop("template")

        # Transform metadata to the API format
        payload = {"scenario": direct_fields}

        result = UpdateMetadataRunner._make_request(
            client=client,
            method="put",
            path=f"/scenarios/{scenario.id}",
            payload=payload,
        )

        return result
