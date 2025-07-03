import pytest
import io

from pyetm.services.scenario_runners.fetch_custom_curves import (
    DownloadCurveRunner,
    FetchAllCurveDataRunner,
)


def test_download_custom_curve_success(
    requests_mock, api_url, client, scenario, custom_curves_json
):
    """
    200 → success=True, data returns the StringIO object.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves/interconnector_2_export_availability.csv"

    # Should return a csv response, but lets not mock that
    requests_mock.get(url, status_code=200, json=custom_curves_json)

    result = DownloadCurveRunner.run(
        client, scenario, "interconnector_2_export_availability"
    )

    assert result.success is True
    assert isinstance(result.data, io.StringIO)


def test_fetch_custom_curves_success(
    requests_mock, api_url, client, scenario, custom_curves_json
):
    """
    200 → success=True, data returns the JSON payload.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves"

    requests_mock.get(url, status_code=200, json=custom_curves_json)

    result = FetchAllCurveDataRunner.run(client, scenario)

    assert result.success is True
    assert result.data == custom_curves_json


def test_fetch_custom_curves_http_error(requests_mock, api_url, client, scenario):
    """
    500 → success=False, error message surfaced.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves"

    requests_mock.get(url, status_code=500, text="server failure")

    result = FetchAllCurveDataRunner.run(client, scenario)

    assert result.success is False
    assert "500" in result.errors[0]
    assert "server failure" in result.errors[0]


def test_fetch_custom_curves_network_exception(monkeypatch, client, scenario):
    """
    Any exception → success=False, error captured.
    """

    def bad_get(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(client.session, "get", bad_get)
    result = FetchAllCurveDataRunner.run(client, scenario)

    assert result.success is False
    assert "network down" in result.errors[0]


def test_download_curve_http_error(requests_mock, api_url, client, scenario):
    """
    500 → success=False, error message surfaced for download.
    """
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves/test_curve.csv"

    requests_mock.get(url, status_code=500, text="server failure")

    result = DownloadCurveRunner.run(client, scenario, "test_curve")

    assert result.success is False
    assert "500" in result.errors[0]
    assert "server failure" in result.errors[0]


def test_download_curve_network_exception(monkeypatch, client, scenario):
    """
    Any exception → success=False, error captured for download.
    """

    def bad_get(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(client.session, "get", bad_get)
    result = DownloadCurveRunner.run(client, scenario, "test_curve")

    assert result.success is False
    assert "network down" in result.errors[0]


def test_fetch_custom_curves_http_error_via_fake_response(
    monkeypatch, client, scenario
):
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

    result = FetchAllCurveDataRunner.run(client, scenario)

    assert result.success is False
    assert result.errors == ["418: Text with a difficult apostrophe's"]


def test_download_curve_http_error_via_fake_response(monkeypatch, client, scenario):
    """
    Simulate an HTTP error response (resp.ok == False) for download curve.
    """

    class FakeResponse:
        ok = False
        status_code = 404
        text = "Curve not found"

    # stub out session.get to return our fake error
    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: FakeResponse())

    result = DownloadCurveRunner.run(client, scenario, "missing_curve")

    assert result.success is False
    assert result.errors == ["404: Curve not found"]
