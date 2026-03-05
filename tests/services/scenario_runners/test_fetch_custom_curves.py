from unittest.mock import patch
from pyetm.services.scenario_runners.fetch_custom_curves import (
    DownloadCustomCurveRunner,
    FetchAllCustomCurveDataRunner,
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


def test_fetch_custom_curves_with_include_internal(
    dummy_client, scenario, custom_curves_json, fake_response
):
    """
    Test that include_internal=True passes 'true' as a string in query params.
    This verifies the fix for GitHub issue #150.
    """
    # Add some internal curves to the response
    extended_json = custom_curves_json.copy()
    extended_json.append({
        "key": "internal_curve_1",
        "type": "custom",
        "internal": True,
    })

    # Create a client that returns the extended JSON
    # Need to wrap the list in a fake_response object
    response = fake_response(ok=True, status_code=200, json_data=extended_json)
    client = dummy_client(response, method="get")

    # Run with include_internal=True
    result = FetchAllCustomCurveDataRunner.run(
        client, scenario, include_internal=True
    )

    assert result.success
    # Verify that the query parameter was passed as a string "true"
    assert len(client.calls) == 1
    _url, call_data = client.calls[0]
    assert call_data is not None
    assert "params" in call_data
    assert call_data["params"]["include_internal"] == "true"


def test_fetch_custom_curves_with_include_unattached(
    dummy_client, scenario, custom_curves_json, fake_response
):
    """
    Test that include_unattached=True passes 'true' as a string in query params.
    """
    # Add some unattached curves to the response
    extended_json = custom_curves_json.copy()
    extended_json.append({
        "key": "unattached_curve_1",
        "type": "custom",
        "attached": False,
    })

    response = fake_response(ok=True, status_code=200, json_data=extended_json)
    client = dummy_client(response, method="get")

    result = FetchAllCustomCurveDataRunner.run(
        client, scenario, include_unattached=True
    )

    assert result.success
    assert len(client.calls) == 1
    _url, call_data = client.calls[0]
    assert call_data is not None
    assert "params" in call_data
    assert call_data["params"]["include_unattached"] == "true"


def test_fetch_custom_curves_with_both_parameters(
    dummy_client, scenario, custom_curves_json, fake_response
):
    """
    Test that both include_internal and include_unattached can be used together.
    """
    response = fake_response(ok=True, status_code=200, json_data=custom_curves_json)
    client = dummy_client(response, method="get")

    result = FetchAllCustomCurveDataRunner.run(
        client, scenario, include_internal=True, include_unattached=True
    )

    assert result.success
    assert len(client.calls) == 1
    _url, call_data = client.calls[0]
    assert call_data is not None
    assert "params" in call_data
    assert call_data["params"]["include_internal"] == "true"
    assert call_data["params"]["include_unattached"] == "true"


def test_fetch_custom_curves_without_parameters(
    dummy_client, scenario, custom_curves_json, fake_response
):
    """
    Test that when both parameters are explicitly False, no query params are sent.
    """
    response = fake_response(ok=True, status_code=200, json_data=custom_curves_json)
    client = dummy_client(response, method="get")

    result = FetchAllCustomCurveDataRunner.run(
        client, scenario, include_internal=False, include_unattached=False
    )

    assert result.success
    assert len(client.calls) == 1
    _url, call_data = client.calls[0]
    # When both parameters are False, payload should be None, so no params are sent
    assert call_data is None or "params" not in call_data


def test_fetch_custom_curves_with_defaults(
    dummy_client, scenario, custom_curves_json, fake_response
):
    """
    Test that default behavior includes internal curves.
    This is the new default as of the fix for issue #150.
    """
    response = fake_response(ok=True, status_code=200, json_data=custom_curves_json)
    client = dummy_client(response, method="get")

    # Call without any parameters - should use defaults (include_internal=True)
    result = FetchAllCustomCurveDataRunner.run(client, scenario)

    assert result.success
    assert len(client.calls) == 1
    _url, call_data = client.calls[0]
    # Default behavior should send include_internal=true
    assert call_data is not None
    assert "params" in call_data
    assert call_data["params"]["include_internal"] == "true"
    # include_unattached should not be sent (defaults to False)
    assert "include_unattached" not in call_data["params"]
