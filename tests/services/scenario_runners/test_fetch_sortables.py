import pytest
from pyetm.services.scenario_runners import FetchSortablesRunner
from pyetm.services.service_result import GenericError


def test_fetch_sortables_success(requests_mock, api_url, client, scenario):
    """
    200 → success=True, data returns the JSON payload.
    """
    payload = {
        "forecast_storage": ["fs1", "fs2"],
        "heat_network": {"lt": ["a1"], "mt": ["b1", "b2"], "ht": []},
        "hydrogen_supply": ["hs1"],
    }
    url = f"{api_url}/scenarios/{scenario.id}/user_sortables"

    requests_mock.get(url, status_code=200, json=payload)

    result = FetchSortablesRunner.run(client, scenario)

    assert result.success is True
    assert result.status_code == 200
    assert result.data == payload


def test_fetch_sortables_http_error(requests_mock, api_url, client, scenario):
    """
    500 → success=False, status_code set, error message surfaced.
    """
    url = f"{api_url}/scenarios/{scenario.id}/user_sortables"
    requests_mock.get(url, status_code=500, text="server failure")

    result = FetchSortablesRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 500
    assert "500" in result.errors[0]
    assert "server failure" in result.errors[0]


def test_fetch_sortables_network_exception(monkeypatch, client, scenario):
    """
    Any exception → success=False, no status_code, error captured.
    """

    def bad_get(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(client.session, "get", bad_get)

    result = FetchSortablesRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code is None
    assert "network down" in result.errors[0]


def test_generic_error_with_parseable_code(monkeypatch, client, scenario):
    """
    GenericError with “Error 404: …” → parse 404 into status_code.
    """

    def bad_get(*args, **kwargs):
        raise GenericError("Error 404: Scenario Not Found")

    monkeypatch.setattr(client.session, "get", bad_get)

    result = FetchSortablesRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 404
    assert "404" in result.errors[0]


def test_generic_error_with_unparseable_code(monkeypatch, client, scenario):
    """
    GenericError with non-numeric → status_code stays None.
    """

    def bad_get(*args, **kwargs):
        raise GenericError("Non numeric error")

    monkeypatch.setattr(client.session, "get", bad_get)

    result = FetchSortablesRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code is None
    assert "Non numeric error" in result.errors[0]


def test_fetch_sortables_http_error_via_fake_response(monkeypatch, client, scenario):
    """
    Simulate resp.ok == False with a fake response.
    """

    class FakeResponse:
        ok = False
        status_code = 418
        text = "Text with a difficult apostrophe's"

        def json(self):
            pytest.fail("json() must not be called on a failed response")

    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: FakeResponse())

    result = FetchSortablesRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 418
    assert result.errors == ["418: Text with a difficult apostrophe's"]
