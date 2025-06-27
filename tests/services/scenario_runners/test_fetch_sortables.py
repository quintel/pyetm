from types import SimpleNamespace
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner


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


def test_fetch_sortables_success():
    body = {
        "forecast_storage": [1, 2],
        "heat_network": {"lt": [], "mt": [0], "ht": []},
    }
    response = FakeResponse(ok=True, status_code=200, json_data=body)
    client = DummyClient(response)
    scenario = DummyScenario(10)

    result = FetchSortablesRunner.run(client, scenario)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/10/user_sortables", None)]


def test_fetch_sortables_http_failure():
    response = FakeResponse(ok=False, status_code=403, text="Forbidden")
    client = DummyClient(response)
    scenario = DummyScenario(11)

    result = FetchSortablesRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["403: Forbidden"]


def test_fetch_sortables_exception():
    client = DummyClient(Exception("unexpected failure"))
    scenario = DummyScenario(12)

    result = FetchSortablesRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert any("unexpected failure" in err for err in result.errors)
