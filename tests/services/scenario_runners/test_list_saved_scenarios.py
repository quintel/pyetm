import pytest
from pyetm.services.scenario_runners.list_saved_scenarios import (
    ListSavedScenariosRunner,
)


SAMPLE_SAVED = [
    {
        "id": 10,
        "scenario_id": 1,
        "title": "Saved base",
        "scenario": {"id": 1, "area_code": "nl", "end_year": 2050},
    },
    {
        "id": 11,
        "scenario_id": 2,
        "title": "Saved wind",
        "scenario": {"id": 2, "area_code": "nl", "end_year": 2040},
    },
]


def test_list_saved_scenarios_success(dummy_client, fake_response):
    """Successful paginated envelope is unwrapped."""
    envelope = {"data": SAMPLE_SAVED, "links": {}}
    client = dummy_client(fake_response(ok=True, status_code=200, json_data=envelope))

    result = ListSavedScenariosRunner.run(client, page=2, limit=50)

    assert result.success is True
    assert result.data == SAMPLE_SAVED
    assert client.calls == [
        ("/saved_scenarios", {"params": {"page": 2, "limit": 50}})
    ]


def test_list_saved_scenarios_bare_array(dummy_client, fake_response):
    """Bare array (no envelope) is accepted."""
    client = dummy_client(
        fake_response(ok=True, status_code=200, json_data=SAMPLE_SAVED)
    )

    result = ListSavedScenariosRunner.run(client)

    assert result.success is True
    assert result.data == SAMPLE_SAVED


def test_list_saved_scenarios_default_pagination(dummy_client, fake_response):
    """Defaults page=1, limit=25."""
    envelope = {"data": [], "links": {}}
    client = dummy_client(fake_response(ok=True, status_code=200, json_data=envelope))

    ListSavedScenariosRunner.run(client)

    assert client.calls[0] == (
        "/saved_scenarios",
        {"params": {"page": 1, "limit": 25}},
    )


def test_list_saved_scenarios_limit_clamped(dummy_client, fake_response):
    """Limit is clamped to [1, 100]."""
    envelope = {"data": [], "links": {}}
    client = dummy_client(fake_response(ok=True, status_code=200, json_data=envelope))

    ListSavedScenariosRunner.run(client, limit=-5)
    assert client.calls[0][1]["params"]["limit"] == 1

    client.calls.clear()
    ListSavedScenariosRunner.run(client, limit=999)
    assert client.calls[0][1]["params"]["limit"] == 100


def test_list_saved_scenarios_http_error(dummy_client, fake_response):
    """HTTP failure propagates."""
    client = dummy_client(
        fake_response(ok=False, status_code=401, text="Unauthorized")
    )

    result = ListSavedScenariosRunner.run(client)

    assert result.success is False
    assert "401" in result.errors[0]


def test_list_saved_scenarios_unexpected_format(dummy_client, fake_response):
    """Non-list response returns failure."""
    client = dummy_client(
        fake_response(ok=True, status_code=200, json_data={"wrong": "shape"})
    )

    result = ListSavedScenariosRunner.run(client)

    assert result.success is False
    assert "Unexpected response format" in result.errors[0]
