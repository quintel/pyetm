"""Tests for FetchUserSavedScenariosRunner."""

from pyetm.services.scenario_runners.fetch_user_saved_scenarios import (
    FetchUserSavedScenariosRunner,
)


def test_fetch_user_saved_scenarios_success_paginated(dummy_client, fake_response):
    """Test successful fetch with paginated response format."""
    body = {
        "data": [
            {
                "id": 1,
                "scenario_id": 100,
                "title": "My Saved Scenario 1",
                "private": True,
                "created_at": "2025-01-01T00:00:00Z",
                "updated_at": "2025-01-01T00:00:00Z",
            },
            {
                "id": 2,
                "scenario_id": 200,
                "title": "My Saved Scenario 2",
                "private": False,
                "created_at": "2025-01-02T00:00:00Z",
                "updated_at": "2025-01-02T00:00:00Z",
            },
        ],
        "links": {},
        "meta": {},
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is True
    assert len(result.data) == 2
    assert result.data[0]["id"] == 1
    assert result.data[0]["scenario_id"] == 100
    assert result.data[1]["id"] == 2
    assert result.data[1]["scenario_id"] == 200
    assert client.calls == [("/saved_scenarios", None)]


def test_fetch_user_saved_scenarios_success_direct_list(dummy_client):
    """Test backwards compatibility with direct list response."""
    # Create a response that returns a direct list
    class DirectListResponse:
        ok = True
        status_code = 200

        def json(self):
            return [
                {
                    "id": 1,
                    "scenario_id": 100,
                    "title": "My Saved Scenario 1",
                    "private": True,
                },
            ]

    client = dummy_client(DirectListResponse(), method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is True
    assert len(result.data) == 1
    assert result.data[0]["id"] == 1


def test_fetch_user_saved_scenarios_empty_paginated(dummy_client, fake_response):
    """Test fetch when user has no saved scenarios (paginated format)."""
    body = {"data": [], "links": {}, "meta": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is True
    assert result.data == []


def test_fetch_user_saved_scenarios_empty_direct_list(dummy_client):
    """Test fetch when user has no saved scenarios (direct list)."""

    # Create a response that returns an empty list
    class EmptyListResponse:
        ok = True
        status_code = 200

        def json(self):
            return []

    client = dummy_client(EmptyListResponse(), method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is True
    assert result.data == []


def test_fetch_user_saved_scenarios_unauthenticated(dummy_client, fake_response):
    """Test fetch without authentication token."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is False
    assert any("401" in err or "Authentication failed" in err for err in result.errors)


def test_fetch_user_saved_scenarios_invalid_response_type(dummy_client, fake_response):
    """Test handling of invalid response type (dict without 'data' key)."""
    body = {"error": "something went wrong"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is False
    assert any("Expected list or paginated response" in err for err in result.errors)


def test_fetch_user_saved_scenarios_invalid_data_type(dummy_client, fake_response):
    """Test handling when 'data' key contains non-list."""
    body = {"data": "invalid", "links": {}, "meta": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is False
    assert any("Expected 'data' key to contain list" in err for err in result.errors)
