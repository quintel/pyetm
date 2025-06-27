from types import SimpleNamespace
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner

# TODO: Move all stubs to fixtures


class FakeResponse:
    def __init__(self, ok, status_code, json_data=None, text=""):
        self.ok = ok
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text

    def json(self):
        return self._json_data


class DummyClient:
    def __init__(self, response):
        self._response = response
        self.calls = []

    @property
    def session(self):
        return SimpleNamespace(get=self._mock_get)

    def _mock_get(self, url, params=None):
        self.calls.append((url, params))
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


class DummyScenario:
    def __init__(self, scenario_id):
        self.id = scenario_id


def test_fetch_inputs_success_no_defaults():
    body = {"i1": {"min": 0.0}}
    response = FakeResponse(ok=True, status_code=200, json_data=body)
    client = DummyClient(response)
    scenario = DummyScenario(1)

    result = FetchInputsRunner.run(client, scenario)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/1/inputs", None)]


def test_fetch_inputs_success_with_defaults():
    body = {"i2": {"default": 42}}
    response = FakeResponse(ok=True, status_code=200, json_data=body)
    client = DummyClient(response)
    scenario = DummyScenario(2)

    result = FetchInputsRunner.run(client, scenario, defaults="original")
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/2/inputs", {"defaults": "original"})]


def test_fetch_inputs_http_failure():
    response = FakeResponse(ok=False, status_code=500, text="Server Error")
    client = DummyClient(response)
    scenario = DummyScenario(3)

    result = FetchInputsRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["500: Server Error"]


def test_fetch_inputs_exception():
    client = DummyClient(RuntimeError("network down"))
    scenario = DummyScenario(4)

    result = FetchInputsRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert any("network down" in err for err in result.errors)
