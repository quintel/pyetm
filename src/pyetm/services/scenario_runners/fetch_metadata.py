from typing import Any, Dict, Optional
from ..service_result import ServiceResult, GenericError
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
        scenario,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        :param client:   API client
        :param scenario: domain object with an `id` attribute
        :returns:        ServiceResult.success=True with `.data` a dict of
                         only the META_KEYS (values may be None if absent),
                         otherwise success=False with errors.
        """
        try:
            resp = client.session.get(f"/scenarios/{scenario.id}")

            if resp.ok:
                body = resp.json()
                # extract only the meta fields
                meta = {k: body.get(k) for k in FetchMetadataRunner.META_KEYS}
                return ServiceResult(
                    success=True, data=meta, status_code=resp.status_code
                )

            return ServiceResult(
                success=False,
                errors=[f"{resp.status_code}: {resp.text}"],
                status_code=resp.status_code,
            )

        except GenericError as error:
            msg = str(error)
            try:
                code = int(msg.split()[1].rstrip(":"))
            except Exception:
                code = None
            return ServiceResult(success=False, errors=[msg], status_code=code)

        except Exception as e:
            return ServiceResult(success=False, errors=[str(e)])
