import pytest
import io

from pyetm.services.custom_curves import fetch_all_curve_data, download_curve
from pyetm.services.service_result import GenericError

def test_download_custom_curve_success(requests_mock, api_url, client, scenario, custom_curves_json):
    """
    200 → success=True, data returns the JSON payload.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves/interconnector_2_export_availability.csv"

    # Should return a csv response, but lets not mock that
    requests_mock.get(url, status_code=200, json=custom_curves_json)

    result = download_curve(client, scenario, "interconnector_2_export_availability")

    assert result.success is True
    assert result.status_code == 200
    assert isinstance(result.data, io.StringIO)

def test_fetch_custom_curves_success(requests_mock, api_url, client, scenario, custom_curves_json):
    """
    200 → success=True, data returns the JSON payload.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves"

    requests_mock.get(url, status_code=200, json=custom_curves_json)

    result = fetch_all_curve_data(client, scenario)

    assert result.success is True
    assert result.status_code == 200
    assert result.data == custom_curves_json


def test_fetch_custom_curves_http_error(requests_mock, api_url, client, scenario):
    """
    500 → success=False, status_code set, error message surfaced.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves"

    requests_mock.get(url, status_code=500, text="server failure")

    result = fetch_all_curve_data(client, scenario)

    assert result.success is False
    assert result.status_code == 500
    assert "500" in result.errors[0]
    assert "server failure" in result.errors[0]

def test_fetch_custom_curves_network_exception(monkeypatch, client, scenario):
    """
    Any exception → success=False, no status_code, error captured.
    """
    def bad_get(*args, **kwargs):
        raise RuntimeError("network down")
    monkeypatch.setattr(client.session, "get", bad_get)
    result = fetch_all_curve_data(client, scenario)

    assert result.success is False
    assert result.status_code is None
    assert "network down" in result.errors[0]

def test_generic_error_with_parseable_code(monkeypatch, client, scenario):
    # Simulate raising a GenericError with “Error 404: Scenario Not Found”
    def bad_get(*args, **kwargs):
        raise GenericError("Error 404: Scenario Not Found")
    monkeypatch.setattr(client.session, "get", bad_get)

    result = fetch_all_curve_data(client, scenario)
    assert result.success is False
    # The “404” should be parsed out as the status_code
    assert result.status_code == 404
    assert "404" in result.errors[0]

def test_generic_error_with_unparseable_code(monkeypatch, client, scenario):
    # Simulate raising a GenericError with non‐numeric
    def bad_get(*args, **kwargs):
        raise GenericError("Non numeric error")
    monkeypatch.setattr(client.session, "get", bad_get)

    result = fetch_all_curve_data(client, scenario)
    assert result.success is False
    # Parsing fails, so status_code stays None
    assert result.status_code is None
    assert "Non numeric error" in result.errors[0]

def test_fetch_custom_curves_http_error_via_fake_response(monkeypatch, client, scenario):
    """
    Simulate an HTTP error response (resp.ok == False) and verify
    we exercise the `errors=[f"{status_code}: {text}"]` path.
    """
    class FakeResponse:
        ok = False
        status_code = 418
        text = "Text with a difficult apostrophe's"
        def json(self):
            # should never be called in this branch
            pytest.fail("json() must not be called on a failed response")

    # stub out session.get to return our fake error
    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: FakeResponse())

    result = fetch_all_curve_data(client, scenario)

    assert result.success is False
    assert result.status_code == 418
    assert result.errors == ["418: Text with a difficult apostrophe's"]
