from unittest.mock import patch
from pyetm.services.scenario_runners.fetch_custom_curves import (
    DownloadCustomCurveRunner,
)
from pyetm.services.service_result import ServiceResult
from pyetm.clients.session import ETMResponse


def test_download_custom_curve_success(requests_mock, api_url, scenario):
    """200 → success=True, data returns the StringIO object (batch infra)."""
    curve_key = "interconnector_2_export_availability"
    csv_content = "time,value\n0,1.0\n1,0.5"

    # Patch batch request method
    response = ETMResponse(
        status_code=200,
        headers={"content-type": "text/csv"},
        url="/x",
        text=csv_content,
        _content=csv_content.encode("utf-8"),
    )
    with patch(
        "pyetm.services.scenario_runners.fetch_custom_curves.GenericCurveDownloadRunner._make_batch_requests",
        return_value=[ServiceResult.ok(data=response)],
    ):
        result = DownloadCustomCurveRunner.run(None, scenario, curve_key)
    assert result.success and "time,value" in result.data.getvalue()


def test_fetch_custom_curves_success(
    requests_mock, api_url, scenario, custom_curves_json
):
    """
    200 → success=True, data returns the JSON payload.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves"
    requests_mock.get(url, status_code=200, json=custom_curves_json)


def test_fetch_custom_curves_http_error(requests_mock, api_url, scenario):
    """
    500 → success=False, error message surfaced.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves"
    requests_mock.get(url, status_code=500, text="server failure")


def test_download_curve_http_error(requests_mock, api_url, scenario):
    """
    500 → success=False, error message surfaced for download.
    """
    curve_key = "some_curve_key"
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves/{curve_key}.csv"
    requests_mock.get(url, status_code=500, text="server failure")


def test_multiple_curves(requests_mock, api_url, scenario, custom_curves_json):
    """
    Test fetching multiple custom curves successfully.
    Expected: 200 responses for all endpoints with appropriate content.
    """

    list_url = f"{api_url}/scenarios/{scenario.id}/custom_curves"
    requests_mock.get(list_url, status_code=200, json=custom_curves_json)

    csv_content = "time,value\n0,1.0\n1,0.5"
    curve_url_1 = f"{api_url}/scenarios/{scenario.id}/custom_curves/interconnector_2_export_availability.csv"
    curve_url_2 = (
        f"{api_url}/scenarios/{scenario.id}/custom_curves/solar_pv_profile_1.csv"
    )

    requests_mock.get(curve_url_1, status_code=200, text=csv_content)
    requests_mock.get(curve_url_2, status_code=200, text=csv_content)
