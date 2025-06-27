import pytest
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.service_result import GenericError


@pytest.fixture
def metadata_json():
    """
    Simulated full‐scenario response, from which FetchMetadataRunner
    will pick only its META_KEYS.
    """
    return {
        "id": 100,
        "created_at": "2025-06-26T07:00:00Z",
        "updated_at": "2025-06-27T08:15:30Z",
        "end_year": 2050,
        "keep_compatible": True,
        "private": False,
        "area_code": "NL",
        "source": "source",
        "metadata": {"foo": "bar"},
        "start_year": 2019,
        "scaling": None,
        "template": 42,
        "url": "http://example.com/api/v3/scenarios/100",
        # extra keys that should be ignored
        "user_values": {"a": 1},
        "balanced_values": {},
    }


def test_fetch_metadata_success(
    requests_mock, api_url, client, scenario, metadata_json
):
    """
    200 → success=True, data returns ONLY the META_KEYS.
    """
    url = f"{api_url}/scenarios/{scenario.id}"
    requests_mock.get(url, status_code=200, json=metadata_json)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is True
    assert result.status_code == 200

    expected = {k: metadata_json[k] for k in FetchMetadataRunner.META_KEYS}
    assert result.data == expected


def test_fetch_metadata_http_error(requests_mock, api_url, client, scenario):
    """
    500 → success=False, status_code set, error message surfaced.
    """
    url = f"{api_url}/scenarios/{scenario.id}"
    requests_mock.get(url, status_code=500, text="server failure")

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is False
    assert result.status_code == 500
    assert "500" in result.errors[0]
    assert "server failure" in result.errors[0]


def test_fetch_metadata_network_exception(monkeypatch, client, scenario):
    """
    Any exception → success=False, no status_code, error captured.
    """

    def bad_get(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(client.session, "get", bad_get)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is False
    assert result.status_code is None
    assert "network down" in result.errors[0]


def test_generic_error_with_parseable_code(monkeypatch, client, scenario):
    """
    GenericError with “Error 404: …” → parse 404 into status_code.
    """

    def bad_get(*args, **kwargs):
        raise GenericError("Error 404: Not Found")

    monkeypatch.setattr(client.session, "get", bad_get)

    result = FetchMetadataRunner.run(client, scenario)
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

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is False
    assert result.status_code is None
    assert "Non numeric error" in result.errors[0]


def test_fetch_metadata_http_error_via_fake_response(monkeypatch, client, scenario):
    """
    Simulate resp.ok == False with a fake response.
    """

    class FakeResponse:
        ok = False
        status_code = 418
        text = "Text with a difficult apostrophe's"

        def json(self):
            pytest.fail("json() should not be called on a failed response")

    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: FakeResponse())

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is False
    assert result.status_code == 418
    assert result.errors == ["418: Text with a difficult apostrophe's"]
