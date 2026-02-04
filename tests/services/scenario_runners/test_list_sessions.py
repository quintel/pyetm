import pytest
from pyetm.services.scenario_runners.list_sessions import ListSessionsRunner


SAMPLE_SESSIONS = [
    {"id": 1, "title": "Base case", "area_code": "nl", "end_year": 2050},
    {"id": 2, "title": "High wind", "area_code": "nl", "end_year": 2040},
]


def test_list_sessions_success(dummy_client, fake_response):
    """Successful paginated response is unwrapped correctly."""
    envelope = {"data": SAMPLE_SESSIONS, "links": {"self": "/scenarios?page=1"}}
    client = dummy_client(fake_response(ok=True, status_code=200, json_data=envelope))

    result = ListSessionsRunner.run(client, page=1, limit=10)

    assert result.success is True
    assert result.data == SAMPLE_SESSIONS
    assert client.calls == [("/scenarios", {"params": {"page": 1, "limit": 10}})]


def test_list_sessions_bare_array(dummy_client, fake_response):
    """Some proxies may return a bare array without the envelope."""
    client = dummy_client(
        fake_response(ok=True, status_code=200, json_data=SAMPLE_SESSIONS)
    )

    result = ListSessionsRunner.run(client)

    assert result.success is True
    assert result.data == SAMPLE_SESSIONS


def test_list_sessions_default_pagination(dummy_client, fake_response):
    """Default page=1, limit=25 are sent when not specified."""
    envelope = {"data": [], "links": {}}
    client = dummy_client(fake_response(ok=True, status_code=200, json_data=envelope))

    ListSessionsRunner.run(client)

    assert client.calls[0] == ("/scenarios", {"params": {"page": 1, "limit": 25}})


def test_list_sessions_limit_clamped(dummy_client, fake_response):
    """Limit is clamped to [1, 100]."""
    envelope = {"data": [], "links": {}}
    client = dummy_client(fake_response(ok=True, status_code=200, json_data=envelope))

    ListSessionsRunner.run(client, limit=999)
    assert client.calls[0][1]["params"]["limit"] == 100

    client.calls.clear()
    ListSessionsRunner.run(client, limit=0)
    assert client.calls[0][1]["params"]["limit"] == 1


def test_list_sessions_http_error(dummy_client, fake_response):
    """HTTP-level failure propagates as a failed ServiceResult."""
    client = dummy_client(
        fake_response(ok=False, status_code=403, text="Forbidden")
    )

    result = ListSessionsRunner.run(client)

    assert result.success is False
    assert "403" in result.errors[0]


def test_list_sessions_unexpected_format(dummy_client, fake_response):
    """Non-list, non-envelope response returns a failure."""
    client = dummy_client(
        fake_response(ok=True, status_code=200, json_data="not a list")
    )

    result = ListSessionsRunner.run(client)

    assert result.success is False
    assert "Unexpected response format" in result.errors[0]


def test_list_sessions_exception(dummy_client):
    """Network-level exceptions are caught and returned as failures."""
    client = dummy_client(ConnectionError("timeout"))

    result = ListSessionsRunner.run(client)

    assert result.success is False
    assert "timeout" in result.errors[0]
