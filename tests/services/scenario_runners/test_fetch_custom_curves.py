from unittest.mock import patch
from pyetm.services.scenario_runners.fetch_custom_curves import (
    DownloadCustomCurveRunner,
    FetchAllCustomCurveDataRunner,
)
from pyetm.services.service_result import ServiceResult
from pyetm.clients.etm_client import ETMResponse


def test_download_custom_curve_success(api_url, scenario):
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


def test_fetch_custom_curves_http_error(dummy_client, fake_response, dummy_scenario):
    client = dummy_client(fake_response(ok=False, status_code=500, text="server failure"))

    result = FetchAllCustomCurveDataRunner.run(client, dummy_scenario(5))

    assert result.errors == ["500: server failure"]


def test_download_curve_http_error(api_url, scenario):
    with patch(
        "pyetm.services.scenario_runners.fetch_custom_curves.GenericCurveDownloadRunner._make_batch_requests",
        return_value=[ServiceResult.fail(["500: server failure"])],
    ):
        result = DownloadCustomCurveRunner.run(None, scenario, "some_curve_key")

    assert result.errors == ["500: server failure"]


def test_fetch_all_curves_includes_internal_parameter(dummy_client, fake_response, dummy_scenario):
    """
    Test that FetchAllCustomCurveDataRunner includes include_internal=true parameter.
    This ensures internal curves (like weather-insulation curves) are accessible.
    """
    # Create a mock response with internal curves
    mock_response_data = [
        {"attached": False, "key": "solar_pv_profile_1", "type": "profile"},
        {
            "attached": False,
            "key": "weather-insulation_terraced_houses_low",
            "type": "weather_curve",
        },
        {
            "attached": False,
            "key": "weather-insulation_apartments_medium",
            "type": "weather_curve",
        },
    ]

    response = fake_response(ok=True, status_code=200, json_data=mock_response_data)
    client = dummy_client(response)
    scenario = dummy_scenario(scenario_id=12345)

    result = FetchAllCustomCurveDataRunner.run(client, scenario)

    # Verify the request was successful
    assert result.success
    assert len(result.data) == 3

    # Verify that include_internal parameter was passed
    assert len(client.calls) == 1
    url, params = client.calls[0]
    assert url == "/saved_scenarios/12345/custom_curves"
    assert params is not None
    assert "params" in params
    assert params["params"]["include_internal"] == "true"

    # Verify internal curves are in the response
    curve_keys = [curve["key"] for curve in result.data]
    assert "weather-insulation_terraced_houses_low" in curve_keys
    assert "weather-insulation_apartments_medium" in curve_keys
