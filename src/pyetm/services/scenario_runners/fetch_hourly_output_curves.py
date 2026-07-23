"""Service for fetch hourly output curves operations."""

from __future__ import annotations

import io
from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.scenario_runners.fetch_curves_generic import (
    GenericCurveBulkRunner,
    GenericCurveDownloadRunner,
)
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient
from pyetm.services.curve_metadata_service import CurveMetadataService


class DownloadHourlyOutputCurveRunner(BaseRunner[io.StringIO]):
    """Download a specific hourly output curve."""

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, curve_name: str, **kwargs: Any
    ) -> ServiceResult[Any]:
        return GenericCurveDownloadRunner.run(client, scenario, curve_name, curve_type="output")


class FetchAllHourlyOutputCurvesRunner(BaseRunner[Dict[str, io.StringIO]]):
    """Download all known hourly output curves."""

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, batch_size: int | None = None, **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        # Fetch curve types dynamically from metadata service
        curve_types = CurveMetadataService.get_curve_names()

        return GenericCurveBulkRunner.run(
            client,
            scenario,
            curve_types,
            curve_type="output",
            batch_size=batch_size,
        )
