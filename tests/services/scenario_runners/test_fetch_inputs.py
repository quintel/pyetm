from pyetm.clients.base_client                      import BaseClient
from pyetm.services.scenario_runners                import FetchInputsRunner
from pyetm.models                                   import Scenario

BASE_URL    = "https://example.com/api"
TOKEN       = "fake-token"
SCENARIO_ID = 999

def test_fetch_inputs_success_without_defaults(requests_mock):
    """
    200 → success=True, data returns the JSON payload.
    """
    scenario = Scenario(id=SCENARIO_ID)
    url      = f"{BASE_URL}/scenarios/{SCENARIO_ID}/inputs"
    payload  = {"one": {"min": 1.0, "max": 2.0, "default": 1.0, "unit": "%"}}

    # stub the exact URL (no query string)
    requests_mock.get(url, status_code=200, json=payload)

    client = BaseClient()
    result = FetchInputsRunner.run(client, scenario)

    assert result.success is True
    assert result.status_code == 200
    assert result.data == payload

def test_fetch_inputs_success_with_defaults(requests_mock):
    """
    200 with ?defaults=original → success=True + correct payload.
    """
    scenario = Scenario(id=SCENARIO_ID)
    url      = f"{BASE_URL}/scenarios/{SCENARIO_ID}/inputs?defaults=original"
    payload  = {"two": {"min": 0.0, "max": 1.0, "default": 0.0, "unit": "%"}}

    # complete_qs=True ensures the query string is part of the match
    requests_mock.get(url, status_code=200, json=payload, complete_qs=True)

    client = BaseClient()
    result = FetchInputsRunner.run(client, scenario, defaults="original")

    assert result.success is True
    assert result.status_code == 200
    assert result.data == payload

def test_fetch_inputs_http_error(requests_mock):
    """
    500 → success=False, status_code set, error message surfaced.
    """
    scenario = Scenario(id=SCENARIO_ID)
    url      = f"{BASE_URL}/scenarios/{SCENARIO_ID}/inputs"

    requests_mock.get(url, status_code=500, text="server failure")

    client = BaseClient()
    result = FetchInputsRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code == 500
    assert "500" in result.errors[0]
    assert "server failure" in result.errors[0]

def test_fetch_inputs_network_exception(monkeypatch):
    """
    Any exception → success=False, no status_code, error captured.
    """
    scenario = Scenario(id=SCENARIO_ID)
    client   = BaseClient()

    def bad_get(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(client.session, "get", bad_get)
    result = FetchInputsRunner.run(client, scenario)

    assert result.success is False
    assert result.status_code is None
    assert "network down" in result.errors[0]
