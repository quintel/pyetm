import pytest
from pyetm.services.scenario_runners                import FetchInputsRunner
from pyetm.models                                   import Scenario
from pyetm.services.service_result import GenericError

def test_fetch_inputs_success_without_defaults(requests_mock, api_url, client, scenario):
    """
    200 → success=True, data returns the JSON payload.
    """
    payload = {"one": {"min": 1.0, "max": 2.0, "default": 1.0, "unit": "%"}}
    url     = f"{api_url}/scenarios/{scenario.id}/inputs"

    requests_mock.get(url, status_code=200, json=payload)

    result = FetchInputsRunner.run(client, scenario)

    assert result.success is True
    assert result.status_code == 200
    assert result.data == payload

def test_fetch_inputs_success_with_defaults(requests_mock, api_url, client, scenario):
    """
    200 with ?defaults=original → success=True + correct payload.
    """
    payload = {"two": {"min": 0.0, "max": 1.0, "default": 0.0, "unit": "%"}}
    url     = f"{api_url}/scenarios/{scenario.id}/inputs?defaults=original"

    requests_mock.get(url, status_code=200, json=payload, complete_qs=True)

    result = FetchInputsRunner.run(client, scenario, defaults="original")

    assert result.success is True
    assert result.status_code == 200
    assert result.data == payload

def test_fetch_inputs_http_error(requests_mock, api_url, client, scenario):
    """
    500 → success=False, status_code set, error message surfaced.
    """
    url = f"{api_url}/scenarios/{scenario.id}/inputs"

    requests_mock.get(url, status_code=500, text="server failure")

    result = FetchInputsRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 500
    assert "500" in result.errors[0]
    assert "server failure" in result.errors[0]

def test_fetch_inputs_network_exception(monkeypatch, client, scenario):
    """
    Any exception → success=False, no status_code, error captured.
    """
    def bad_get(*args, **kwargs):
        raise RuntimeError("network down")
    monkeypatch.setattr(client.session, "get", bad_get)
    result = FetchInputsRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code is None
    assert "network down" in result.errors[0]

def test_generic_error_with_parseable_code(monkeypatch, client, scenario):
    # Simulate raising a GenericError with “Error 404: Scenario Not Found”
    def bad_get(*args, **kwargs):
        raise GenericError("Error 404: Scenario Not Found")
    monkeypatch.setattr(client.session, "get", bad_get)

    result = FetchInputsRunner.run(client, scenario)
    assert result.success is False
    # The “404” should be parsed out as the status_code
    assert result.status_code == 404
    assert "404" in result.errors[0]

def test_generic_error_with_unparseable_code(monkeypatch, client, scenario):
    # Simulate raising a GenericError with non‐numeric
    def bad_get(*args, **kwargs):
        raise GenericError("Non numeric error")
    monkeypatch.setattr(client.session, "get", bad_get)

    result = FetchInputsRunner.run(client, scenario)
    assert result.success is False
    # Parsing fails, so status_code stays None
    assert result.status_code is None
    assert "Non numeric error" in result.errors[0]

def test_fetch_inputs_http_error_via_fake_response(monkeypatch, client, scenario):
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

    result = FetchInputsRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 418
    assert result.errors == ["418: Text with a difficult apostrophe's"]
