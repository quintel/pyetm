import pytest

from pyetm.services.scenario_runners.fetch_scenario import FetchScenarioRunner
from pyetm.services.service_result import GenericError, ServiceResult


def test_fetch_scenario_success(requests_mock, api_url, client, scenario):
    """
    200 → success=True, data returns the JSON payload.
    """
    payload = {
        "id": scenario.id,
        "balanced_values": {"one": 1.0, "two": 2.0},
        "user_values": {"one": 0.5},
    }
    url = f"{api_url}/scenarios/{scenario.id}"

    requests_mock.get(url, status_code=200, json=payload)

    result = FetchScenarioRunner.run(client, scenario)

    assert result.success is True
    assert result.status_code == 200
    assert result.data == payload


def test_fetch_scenario_http_error(requests_mock, api_url, client, scenario):
    """
    500 → success=False, status_code set, error message surfaced.
    """
    url = f"{api_url}/scenarios/{scenario.id}"
    requests_mock.get(url, status_code=500, text="server explosion")

    result = FetchScenarioRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 500
    # Should include "500: server explosion"
    assert result.errors == ["500: server explosion"]


def test_fetch_scenario_network_exception(monkeypatch, client, scenario):
    """
    Any exception → success=False, no status_code, error captured.
    """

    def bad_get(*args, **kwargs):
        raise RuntimeError("connection lost")

    monkeypatch.setattr(client.session, "get", bad_get)

    result = FetchScenarioRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code is None
    assert "connection lost" in result.errors[0]


def test_generic_error_with_parseable_code(monkeypatch, client, scenario):
    """
    GenericError with "Error 404: Not Found" → code parsed out as status_code
    """

    def raise_generic(*args, **kwargs):
        raise GenericError("Error 404: Not Found")

    monkeypatch.setattr(client.session, "get", raise_generic)

    result = FetchScenarioRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 404
    assert "404" in result.errors[0]


def test_generic_error_with_unparseable_code(monkeypatch, client, scenario):
    """
    GenericError with non‐numeric message → status_code stays None
    """

    def raise_generic(*args, **kwargs):
        raise GenericError("Something bad happened")

    monkeypatch.setattr(client.session, "get", raise_generic)

    result = FetchScenarioRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code is None
    assert "Something bad happened" in result.errors[0]


def test_fetch_scenario_http_error_via_fake_response(monkeypatch, client, scenario):
    """
    Simulate resp.ok == False path and verify error formatting.
    """

    class FakeResponse:
        ok = False
        status_code = 418
        text = "I'm a teapot"

        def json(self):
            pytest.fail("json() should not be called on a failed response")

    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: FakeResponse())

    result = FetchScenarioRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 418
    assert result.errors == ["418: I'm a teapot"]
