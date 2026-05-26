"""Tests for FetchUserScenariosRunner."""

from pyetm.services.scenario_runners.fetch_user_scenarios import (
    FetchUserScenariosRunner,
)


def test_fetch_user_scenarios_success_paginated(dummy_client):
    """Test successful fetch with paginated response format (auto-pagination)."""

    # Create a response that returns data on page 1, empty on page 2
    class PaginatedResponse:
        def __init__(self):
            self.call_count = 0

        ok = True
        status_code = 200

        def json(self):
            self.call_count += 1
            if self.call_count == 1:
                return {
                    "data": [
                        {
                            "id": 1,
                            "area_code": "nl",
                            "end_year": 2050,
                            "title": "Scenario 1",
                            "created_at": "2025-01-01T00:00:00Z",
                        },
                        {
                            "id": 2,
                            "area_code": "de",
                            "end_year": 2040,
                            "title": "Scenario 2",
                            "created_at": "2025-01-02T00:00:00Z",
                        },
                    ],
                    "links": {},
                    "meta": {},
                }
            else:
                # Empty page stops pagination
                return {"data": [], "links": {}, "meta": {}}

    response_obj = PaginatedResponse()
    client = dummy_client(response_obj, method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is True
    assert len(result.data) == 2
    assert result.data[0]["id"] == 1
    assert result.data[1]["id"] == 2
    # Verify it fetched page 1 then page 2 (empty, stops)
    assert len(client.calls) == 2
    assert client.calls[0] == ("/scenarios", {"params": {"page": 1}})
    assert client.calls[1] == ("/scenarios", {"params": {"page": 2}})


def test_fetch_user_scenarios_success_direct_list(dummy_client):
    """Test backwards compatibility with direct list response."""
    # Create a response that returns a direct list
    class DirectListResponse:
        ok = True
        status_code = 200

        def json(self):
            return [
                {
                    "id": 1,
                    "area_code": "nl",
                    "end_year": 2050,
                    "title": "Scenario 1",
                },
            ]

    client = dummy_client(DirectListResponse(), method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is True
    assert len(result.data) == 1
    assert result.data[0]["id"] == 1


def test_fetch_user_scenarios_empty_paginated(dummy_client):
    """Test fetch when user has no scenarios (paginated format)."""

    class EmptyPaginatedResponse:
        ok = True
        status_code = 200

        def json(self):
            return {"data": [], "links": {}, "meta": {}}

    response_obj = EmptyPaginatedResponse()
    client = dummy_client(response_obj, method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is True
    assert result.data == []
    # Should fetch page 1, get empty, and stop
    assert len(client.calls) == 1
    assert client.calls[0] == ("/scenarios", {"params": {"page": 1}})


def test_fetch_user_scenarios_empty_direct_list(dummy_client):
    """Test fetch when user has no scenarios (direct list)."""

    # Create a response that returns an empty list
    class EmptyListResponse:
        ok = True
        status_code = 200

        def json(self):
            return []

    client = dummy_client(EmptyListResponse(), method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is True
    assert result.data == []


def test_fetch_user_scenarios_with_pagination(dummy_client, fake_response):
    """Test fetch with pagination parameters."""
    body = {"data": [{"id": 1, "title": "Scenario 1"}], "links": {}, "meta": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client, page=2, per_page=10)
    assert result.success is True
    assert len(result.data) == 1
    # Check that pagination params were passed correctly
    assert client.calls == [("/scenarios", {"params": {"page": 2, "per_page": 10}})]


def test_fetch_user_scenarios_unauthenticated(dummy_client, fake_response):
    """Test fetch without authentication token."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is False
    assert any("401" in err or "Authentication failed" in err for err in result.errors)


def test_fetch_user_scenarios_invalid_response_type(dummy_client, fake_response):
    """Test handling of invalid response type (dict without 'data' key)."""
    body = {"error": "something went wrong"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is False
    assert any("Expected list or paginated response" in err for err in result.errors)


def test_fetch_user_scenarios_invalid_data_type(dummy_client, fake_response):
    """Test handling when 'data' key contains non-list."""
    body = {"data": "invalid", "links": {}, "meta": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is False
    assert any("Expected 'data' key to contain list" in err for err in result.errors)


def test_fetch_user_scenarios_multi_page_automatic(dummy_client):
    """Test automatic fetching of multiple pages when no page param provided."""

    # Simulate API that returns different responses for different pages
    class MultiPageResponse:
        def __init__(self):
            self.call_count = 0

        ok = True
        status_code = 200

        def json(self):
            self.call_count += 1
            if self.call_count == 1:
                # Page 1: return 2 scenarios
                return {
                    "data": [
                        {"id": 1, "title": "Scenario 1"},
                        {"id": 2, "title": "Scenario 2"},
                    ],
                    "links": {},
                    "meta": {},
                }
            elif self.call_count == 2:
                # Page 2: return 2 more scenarios
                return {
                    "data": [
                        {"id": 3, "title": "Scenario 3"},
                        {"id": 4, "title": "Scenario 4"},
                    ],
                    "links": {},
                    "meta": {},
                }
            else:
                # Page 3: empty page (stop condition)
                return {"data": [], "links": {}, "meta": {}}

    response_obj = MultiPageResponse()
    client = dummy_client(response_obj, method="get")

    result = FetchUserScenariosRunner.run(client)

    assert result.success is True
    assert len(result.data) == 4
    assert result.data[0]["id"] == 1
    assert result.data[1]["id"] == 2
    assert result.data[2]["id"] == 3
    assert result.data[3]["id"] == 4

    # Verify it made 3 calls (page 1, page 2, page 3 empty)
    assert len(client.calls) == 3
    assert client.calls[0] == ("/scenarios", {"params": {"page": 1}})
    assert client.calls[1] == ("/scenarios", {"params": {"page": 2}})
    assert client.calls[2] == ("/scenarios", {"params": {"page": 3}})


def test_fetch_user_scenarios_specific_page_only(dummy_client, fake_response):
    """Test that providing page parameter fetches only that page."""
    body = {"data": [{"id": 5, "title": "Page 2 Scenario"}], "links": {}, "meta": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client, page=2)

    assert result.success is True
    assert len(result.data) == 1
    assert result.data[0]["id"] == 5

    # Verify it only made 1 call with page=2
    assert len(client.calls) == 1
    assert client.calls[0] == ("/scenarios", {"params": {"page": 2}})


def test_fetch_user_scenarios_per_page_without_page(dummy_client):
    """Test per_page parameter when fetching all pages automatically."""

    class MultiPageResponse:
        def __init__(self):
            self.call_count = 0

        ok = True
        status_code = 200

        def json(self):
            self.call_count += 1
            if self.call_count == 1:
                return {"data": [{"id": 1}], "links": {}, "meta": {}}
            else:
                return {"data": [], "links": {}, "meta": {}}

    response_obj = MultiPageResponse()
    client = dummy_client(response_obj, method="get")

    result = FetchUserScenariosRunner.run(client, per_page=50)

    assert result.success is True
    assert len(result.data) == 1

    # Verify per_page was passed in both calls
    assert len(client.calls) == 2
    assert client.calls[0] == ("/scenarios", {"params": {"page": 1, "per_page": 50}})
    assert client.calls[1] == ("/scenarios", {"params": {"page": 2, "per_page": 50}})
