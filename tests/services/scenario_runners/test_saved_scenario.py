import pytest
from pyetm.services.scenario_runners.create_saved_scenario import (
    CreateSavedScenarioRunner,
)
from pyetm.services.scenario_runners.fetch_saved_scenario import (
    FetchSavedScenarioRunner,
)
from pyetm.services.scenario_runners.list_saved_scenarios import (
    ListSavedScenariosRunner,
)
from pyetm.services.scenario_runners.update_saved_scenario import (
    UpdateSavedScenarioRunner,
)
from pyetm.services.scenario_runners.delete_saved_scenario import (
    DeleteSavedScenarioRunner,
)


# --- CreateSavedScenarioRunner Tests ---


def test_create_saved_scenario_success_minimal(dummy_client, fake_response):
    body = {
        "id": 1,
        "scenario_id": 123,
        "title": "My Saved Scenario",
        "description": None,
        "private": False,
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    saved_scenario_data = {
        "scenario_id": 123,
        "title": "My Saved Scenario",
    }

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/saved_scenarios", {"json": saved_scenario_data})]


def test_create_saved_scenario_success_with_optional_fields(
    dummy_client, fake_response
):
    body = {
        "id": 2,
        "scenario_id": 456,
        "title": "Test Scenario",
        "description": "This is a test",
        "private": True,
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    saved_scenario_data = {
        "scenario_id": 456,
        "title": "Test Scenario",
        "description": "This is a test",
        "private": True,
    }

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/saved_scenarios", {"json": saved_scenario_data})]


def test_create_saved_scenario_missing_required_scenario_id(dummy_client):
    client = dummy_client({}, method="post")

    saved_scenario_data = {"title": "My Scenario"}  # Missing scenario_id

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert "Missing required fields: scenario_id" in result.errors[0]
    assert len(client.calls) == 0


def test_create_saved_scenario_missing_required_title(dummy_client):
    client = dummy_client({}, method="post")

    saved_scenario_data = {"scenario_id": 123}  # Missing title

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert "Missing required fields: title" in result.errors[0]
    assert len(client.calls) == 0


def test_create_saved_scenario_missing_both_required_fields(dummy_client):
    client = dummy_client({}, method="post")

    saved_scenario_data = {"private": True}  # Missing both

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    error_msg = result.errors[0]
    assert "Missing required fields:" in error_msg
    assert "scenario_id" in error_msg
    assert "title" in error_msg
    assert len(client.calls) == 0


def test_create_saved_scenario_filters_invalid_fields(dummy_client, fake_response):
    body = {"id": 3, "scenario_id": 789, "title": "Filtered Test"}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    saved_scenario_data = {
        "scenario_id": 789,
        "title": "Filtered Test",
        "description": "Valid field",  # Valid
        "id": 999,  # Invalid - should be filtered
        "created_at": "2025-01-01",  # Invalid - should be filtered
        "invalid_field": "value",  # Invalid - should be filtered
    }

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is True
    assert result.data == body

    # Check for warnings about filtered fields
    expected_warnings = [
        "Ignoring invalid field for create saved scenario: 'id'",
        "Ignoring invalid field for create saved scenario: 'created_at'",
        "Ignoring invalid field for create saved scenario: 'invalid_field'",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    # Only valid fields should be sent
    expected_payload = {
        "scenario_id": 789,
        "title": "Filtered Test",
        "description": "Valid field",
    }
    assert client.calls == [("/saved_scenarios", {"json": expected_payload})]


def test_create_saved_scenario_http_failure_401(dummy_client, fake_response):
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="post")

    saved_scenario_data = {"scenario_id": 123, "title": "Test"}

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


# --- FetchSavedScenarioRunner Tests ---


def test_fetch_saved_scenario_success(dummy_client, fake_response, dummy_scenario):
    body = {
        "id": 1,
        "scenario_id": 123,
        "title": "Fetched Scenario",
        "description": "Test description",
        "private": False,
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    saved_scenario = dummy_scenario(1)

    result = FetchSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is True
    assert result.data["id"] == 1
    assert result.data["scenario_id"] == 123
    assert result.data["title"] == "Fetched Scenario"
    assert client.calls == [("/saved_scenarios/1", None)]


def test_fetch_saved_scenario_missing_fields_warning(
    dummy_client, fake_response, dummy_scenario
):
    # Response missing some fields
    body = {
        "id": 2,
        "scenario_id": 456,
        "title": "Incomplete",
        # Missing: description, private, created_at, updated_at
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    saved_scenario = dummy_scenario(2)

    result = FetchSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is True
    assert result.data["id"] == 2
    assert "description" not in result.data  # Missing fields not filled
    # Should have warnings for missing fields
    assert any("Missing field in response" in err for err in result.errors)


def test_fetch_saved_scenario_not_found(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=404, text="Not Found")
    client = dummy_client(response, method="get")

    saved_scenario = dummy_scenario(999)

    result = FetchSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is False
    assert result.errors == ["404: Not Found"]


# --- ListSavedScenariosRunner Tests ---


def test_list_saved_scenarios_success(dummy_client, fake_response):
    body = {
        "data": [
            {"id": 1, "scenario_id": 123, "title": "Scenario 1"},
            {"id": 2, "scenario_id": 456, "title": "Scenario 2"},
        ],
        "links": {},
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = ListSavedScenariosRunner.run(client)
    assert result.success is True
    assert len(result.data) == 2
    assert result.data[0]["id"] == 1
    assert result.data[1]["id"] == 2
    assert client.calls == [("/saved_scenarios", {"params": {"page": 1, "limit": 25}})]


def test_list_saved_scenarios_with_pagination(dummy_client, fake_response):
    body = {"data": [{"id": 3, "scenario_id": 789, "title": "Scenario 3"}], "links": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = ListSavedScenariosRunner.run(client, page=2, limit=50)
    assert result.success is True
    assert len(result.data) == 1
    assert client.calls == [("/saved_scenarios", {"params": {"page": 2, "limit": 50}})]


def test_list_saved_scenarios_limit_max(dummy_client, fake_response):
    """Test that limit is capped at 100"""
    body = {"data": [{"id": 1, "scenario_id": 123, "title": "Test"}], "links": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = ListSavedScenariosRunner.run(client, page=1, limit=200)
    assert result.success is True
    # Limit should be capped at 100
    assert client.calls == [("/saved_scenarios", {"params": {"page": 1, "limit": 100}})]


def test_list_saved_scenarios_empty_list(dummy_client, fake_response):
    body = {"data": [], "links": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = ListSavedScenariosRunner.run(client)
    assert result.success is True
    assert len(result.data) == 0


def test_list_saved_scenarios_invalid_response_type(dummy_client, fake_response):
    """Test handling of non-wrapped response"""
    response = fake_response(ok=True, status_code=200, json_data={"not": "wrapped"})
    client = dummy_client(response, method="get")

    result = ListSavedScenariosRunner.run(client)
    assert result.success is False
    assert "Expected wrapped response with 'data' key" in result.errors[0]


# --- UpdateSavedScenarioRunner Tests ---


def test_update_saved_scenario_title(dummy_client, fake_response, dummy_scenario):
    body = {
        "id": 1,
        "scenario_id": 123,
        "title": "Updated Title",
        "description": "Original description",
        "private": False,
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    saved_scenario = dummy_scenario(1)
    update_data = {"title": "Updated Title"}

    result = UpdateSavedScenarioRunner.run(client, saved_scenario, update_data)
    assert result.success is True
    assert result.data["title"] == "Updated Title"
    assert client.calls == [("/saved_scenarios/1", {"json": update_data})]


def test_update_saved_scenario_multiple_fields(
    dummy_client, fake_response, dummy_scenario
):
    body = {
        "id": 2,
        "scenario_id": 456,
        "title": "New Title",
        "description": "New description",
        "private": True,
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    saved_scenario = dummy_scenario(2)
    update_data = {
        "title": "New Title",
        "description": "New description",
        "private": True,
    }

    result = UpdateSavedScenarioRunner.run(client, saved_scenario, update_data)
    assert result.success is True
    assert client.calls == [("/saved_scenarios/2", {"json": update_data})]


def test_update_saved_scenario_filters_invalid_fields(
    dummy_client, fake_response, dummy_scenario
):
    body = {"id": 3, "scenario_id": 789, "title": "Updated"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    saved_scenario = dummy_scenario(3)
    update_data = {
        "title": "Updated",  # Valid
        "scenario_id": 999,  # Invalid - should be filtered
        "id": 888,  # Invalid - should be filtered
        "created_at": "2025-01-01",  # Invalid - should be filtered
    }

    result = UpdateSavedScenarioRunner.run(client, saved_scenario, update_data)
    assert result.success is True

    # Check for warnings
    expected_warnings = [
        "Ignoring invalid field for update saved scenario: 'scenario_id'",
        "Ignoring invalid field for update saved scenario: 'id'",
        "Ignoring invalid field for update saved scenario: 'created_at'",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    # Only valid field should be sent
    assert client.calls == [("/saved_scenarios/3", {"json": {"title": "Updated"}})]


def test_update_saved_scenario_no_valid_fields(dummy_client, dummy_scenario):
    client = dummy_client({}, method="put")

    saved_scenario = dummy_scenario(1)
    update_data = {
        "id": 999,  # Invalid
        "scenario_id": 123,  # Invalid
    }

    result = UpdateSavedScenarioRunner.run(client, saved_scenario, update_data)
    assert result.success is False
    assert "No valid fields to update" in result.errors[0]
    assert len(client.calls) == 0


def test_update_saved_scenario_http_failure_403(
    dummy_client, fake_response, dummy_scenario
):
    response = fake_response(ok=False, status_code=403, text="Forbidden")
    client = dummy_client(response, method="put")

    saved_scenario = dummy_scenario(1)
    update_data = {"title": "Updated"}

    result = UpdateSavedScenarioRunner.run(client, saved_scenario, update_data)
    assert result.success is False
    assert result.errors == ["403: Forbidden"]


# --- DeleteSavedScenarioRunner Tests ---


def test_delete_saved_scenario_success(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=True, status_code=204, json_data={})
    client = dummy_client(response, method="delete")

    saved_scenario = dummy_scenario(1)

    result = DeleteSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is True
    assert client.calls == [("/saved_scenarios/1", None)]


def test_delete_saved_scenario_not_found(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=404, text="Not Found")
    client = dummy_client(response, method="delete")

    saved_scenario = dummy_scenario(999)

    result = DeleteSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is False
    assert result.errors == ["404: Not Found"]


def test_delete_saved_scenario_forbidden(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=403, text="Forbidden")
    client = dummy_client(response, method="delete")

    saved_scenario = dummy_scenario(1)

    result = DeleteSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is False
    assert result.errors == ["403: Forbidden"]
