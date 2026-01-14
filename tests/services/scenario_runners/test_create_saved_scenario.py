from pyetm.services.scenario_runners.create_saved_scenario import (
    CreateSavedScenarioRunner,
)


def test_create_saved_scenario_success_minimal(dummy_client, fake_response):
    """Test creating a SavedScenario with only required fields."""
    body = {
        "id": 456,
        "scenario_id": 123,
        "title": "My Saved Scenario",
        "description": None,
        "private": False,
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    saved_scenario_data = {"scenario_id": 123, "title": "My Saved Scenario"}

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/saved_scenarios", {"json": {"saved_scenario": saved_scenario_data}})
    ]


def test_create_saved_scenario_success_with_optional_fields(
    dummy_client, fake_response
):
    """Test creating a SavedScenario with all fields."""
    body = {
        "id": 457,
        "scenario_id": 123,
        "title": "My Saved Scenario",
        "private": True,
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    saved_scenario_data = {
        "scenario_id": 123,
        "title": "My Saved Scenario",
        "private": True,
    }

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/saved_scenarios", {"json": {"saved_scenario": saved_scenario_data}})
    ]


def test_create_saved_scenario_missing_required_field_scenario_id(
    dummy_client, fake_response
):
    """Test that missing scenario_id returns an error."""
    client = dummy_client({}, method="post")

    saved_scenario_data = {"title": "My Saved Scenario"}  # Missing scenario_id

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert "Missing required fields: scenario_id" in result.errors[0]
    assert len(client.calls) == 0  # Should not make API call


def test_create_saved_scenario_missing_required_field_title(
    dummy_client, fake_response
):
    """Test that missing title returns an error."""
    client = dummy_client({}, method="post")

    saved_scenario_data = {"scenario_id": 123}  # Missing title

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert "Missing required fields: title" in result.errors[0]
    assert len(client.calls) == 0  # Should not make API call


def test_create_saved_scenario_missing_both_required_fields(
    dummy_client, fake_response
):
    """Test that missing both required fields returns an error."""
    client = dummy_client({}, method="post")

    saved_scenario_data = {"private": True}  # Missing both required fields

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    error_msg = result.errors[0]
    assert "Missing required fields:" in error_msg
    assert "scenario_id" in error_msg
    assert "title" in error_msg
    assert len(client.calls) == 0  # Should not make API call


def test_create_saved_scenario_filters_invalid_fields(dummy_client, fake_response):
    """Test that invalid fields are filtered and warnings are returned."""
    body = {
        "id": 458,
        "scenario_id": 123,
        "title": "My Saved Scenario",
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    saved_scenario_data = {
        "scenario_id": 123,
        "title": "My Saved Scenario",
        "private": True,  # Valid
        "description": "Should be filtered",  # Invalid - should be filtered
        "id": 999,  # Invalid - should be filtered
        "created_at": "2019-01-01",  # Invalid - should be filtered
        "invalid_field": "value",  # Invalid - should be filtered
    }

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is True
    assert result.data == body

    # Should have warnings for filtered fields
    expected_warnings = [
        "Ignoring invalid field for create saved scenario: 'description'",
        "Ignoring invalid field for create saved scenario: 'id'",
        "Ignoring invalid field for create saved scenario: 'created_at'",
        "Ignoring invalid field for create saved scenario: 'invalid_field'",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    # Should only send valid fields
    expected_payload = {
        "saved_scenario": {
            "scenario_id": 123,
            "title": "My Saved Scenario",
            "private": True,
        }
    }
    assert client.calls == [("/saved_scenarios", {"json": expected_payload})]


def test_create_saved_scenario_http_failure_422(dummy_client, fake_response):
    """Test handling of 422 validation error."""
    response = fake_response(ok=False, status_code=422, text="Validation Error")
    client = dummy_client(response, method="post")

    saved_scenario_data = {"scenario_id": 123, "title": "My Saved Scenario"}

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["422: Validation Error"]


def test_create_saved_scenario_http_failure_401(dummy_client, fake_response):
    """Test handling of 401 unauthorized error."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="post")

    saved_scenario_data = {"scenario_id": 123, "title": "My Saved Scenario"}

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


def test_create_saved_scenario_http_failure_404(dummy_client, fake_response):
    """Test handling of 404 not found error (scenario doesn't exist)."""
    response = fake_response(ok=False, status_code=404, text="Scenario not found")
    client = dummy_client(response, method="post")

    saved_scenario_data = {"scenario_id": 99999, "title": "My Saved Scenario"}

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Scenario not found"]


def test_create_saved_scenario_connection_error(dummy_client):
    """Test handling of connection errors."""
    client = dummy_client(ConnectionError("Connection failed"), method="post")

    saved_scenario_data = {"scenario_id": 123, "title": "My Saved Scenario"}

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_create_saved_scenario_with_kwargs(dummy_client, fake_response):
    """Test that kwargs are passed through to the request."""
    body = {"id": 459, "scenario_id": 123, "title": "My Saved Scenario"}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    saved_scenario_data = {"scenario_id": 123, "title": "My Saved Scenario"}

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data, timeout=30)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify basic structure
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/saved_scenarios"
    assert client.calls[0][1]["json"] == {"saved_scenario": saved_scenario_data}


def test_create_saved_scenario_payload_structure(dummy_client, fake_response):
    """Test that the payload is correctly structured for the API."""
    body = {"id": 460, "scenario_id": 123, "title": "Test Scenario"}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    saved_scenario_data = {
        "scenario_id": 123,
        "title": "Test Scenario",
        "private": True,
    }

    CreateSavedScenarioRunner.run(client, saved_scenario_data)

    # Verify the exact payload structure
    expected_call = (
        "/saved_scenarios",
        {"json": {"saved_scenario": saved_scenario_data}},
    )
    assert client.calls == [expected_call]


def test_create_saved_scenario_empty_data(dummy_client, fake_response):
    """Test that empty data returns an error."""
    client = dummy_client({}, method="post")

    saved_scenario_data = {}

    result = CreateSavedScenarioRunner.run(client, saved_scenario_data)
    assert result.success is False
    assert result.data is None
    assert len(client.calls) == 0  # Should not make API call
