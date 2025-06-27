from typing import Any, Dict
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchMetadataRunner:
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
        """
        :param client:   API client
        :param scenario: domain object with an `id` attribute
        :returns:
          - ServiceResult.ok(data, warnings) if we got JSON back;
          - ServiceResult.fail(errors) on any breaking error.
        """
        try:
            resp = client.session.get(f"/scenarios/{scenario.id}")

            if resp.ok:
                body = resp.json()
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

            # HTTP-level failure is breaking
            return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

        except Exception as e:
            # any unexpected exception is a breaking error
            return ServiceResult.fail([str(e)])
