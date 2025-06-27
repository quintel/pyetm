from types import SimpleNamespace
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner

# TODO: Move all stubs to fixtures


# Fake response to mimic HTTP behavior
class FakeResponse:
    def __init__(self, ok, status_code, json_data=None, text=""):
        self.ok = ok
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text

    def json(self):
        return self._json_data


# Dummy client stub
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


def test_fetch_metadata_success_full():
    body = {k: f"value_{k}" for k in FetchMetadataRunner.META_KEYS}
    response = FakeResponse(ok=True, status_code=200, json_data=body)
    client = DummyClient(response)
    scenario = DummyScenario(42)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/42", None)]


def test_fetch_metadata_missing_keys_warns():
    response = FakeResponse(ok=True, status_code=200, json_data={})
    client = DummyClient(response)
    scenario = DummyScenario(7)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is True
    assert all(v is None for v in result.data.values())
    assert len(result.errors) == len(FetchMetadataRunner.META_KEYS)
    assert all("Missing field in response" in w for w in result.errors)


def test_fetch_metadata_http_failure():
    response = FakeResponse(ok=False, status_code=404, text="Not Found")
    client = DummyClient(response)
    scenario = DummyScenario(9)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Not Found"]


def test_fetch_metadata_exception():
    client = DummyClient(ValueError("boom! something went wrong"))
    scenario = DummyScenario(5)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert any("boom! something" in err for err in result.errors)
