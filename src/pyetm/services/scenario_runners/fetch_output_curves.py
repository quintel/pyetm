import io
from typing import Any, Dict, List, Optional
import pandas as pd
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.scenario_runners.fetch_curves_generic import (
    GenericCurveDownloadRunner,
)
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DownloadOutputCurveRunner(BaseRunner[io.StringIO]):
    """Download a specific output curve."""

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_name: str,
    ) -> ServiceResult[io.StringIO]:
        return GenericCurveDownloadRunner.run(
            client, scenario, curve_name, curve_type="output"
        )


class FetchAllOutputCurvesRunner(BaseRunner[Dict[str, io.StringIO]]):
    """Download all known output curves."""

    CURVE_TYPES = [
        "merit_order",
        "electricity_price",
        "heat_network",
        "agriculture_heat",
        "household_heat",
        "buildings_heat",
        "hydrogen",
        "network_gas",
        "residual_load",
        "hydrogen_integral_cost",
    ]

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_types: Optional[List[str]] = None,
    ) -> ServiceResult[Dict[str, io.StringIO]]:
        """
        Uses the bulk endpoint to fetch output curves as a single CSV,
        then splits them into their carrier groups

        Args:
            client: The API client
            scenario: The scenario object
            curve_types: Optional list of curve types to fetch. If None, fetches all default curves.
        """
        try:
            types_to_fetch = curve_types or FetchAllOutputCurvesRunner.CURVE_TYPES

            path = f"/scenarios/{scenario.id}/bulk_output_curves"
            params = {"curve_types": ",".join(types_to_fetch)}
            resp = client.session.get(
                path, params=params, headers={"Accept": "text/csv"}
            )

            if not resp.ok:
                return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

            try:
                csv_text = resp.content.decode("utf-8")
                df = pd.read_csv(io.StringIO(csv_text), index_col=0)
            except Exception as e:
                return ServiceResult.fail([f"Failed to parse bulk CSV: {e}"])

            results: Dict[str, io.StringIO] = {}
            warnings: list[str] = []

            groups: Dict[str, list[str]] = {}
            for col in df.columns:
                base = str(col)
                for sep in (":", "/"):
                    if sep in base:
                        base = base.split(sep, 1)[0]
                        break
                groups.setdefault(base, []).append(col)

            for base, cols in groups.items():
                try:
                    sub = df[cols].dropna(how="all")
                    buf = io.StringIO()
                    sub.to_csv(buf, index=True)
                    buf.seek(0)
                    results[base] = buf
                except Exception as e:
                    warnings.append(f"{base}: Failed to prepare CSV: {e}")

            if results:
                return ServiceResult.ok(data=results, errors=warnings or None)
            else:
                return ServiceResult.fail(warnings or ["No curves present in CSV"])

        except (PermissionError, ValueError, ConnectionError) as e:
            return ServiceResult.fail([str(e)])
        except Exception as e:
            return ServiceResult.fail([str(e)])
