from pyetm.services.scenario_runners.update_saved_scenario import (
    UpdateSavedScenarioRunner,
)


def test_update_saved_scenario_success_single_field(dummy_client, fake_response):
    """Test updating a single field of a SavedScenario."""
    body = {
        "id": 456,
        "scenario_id": 123,
        "title": "Updated Title",
        "description": None,
        "private": False,
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {"title": "Updated Title"}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/saved_scenarios/456", {"json": {"saved_scenario": update_data}})
    ]


def test_update_saved_scenario_success_multiple_fields(dummy_client, fake_response):
    """Test updating multiple fields of a SavedScenario."""
    body = {
        "id": 456,
        "scenario_id": 123,
        "title": "New Title",
        "private": True,
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {
        "title": "New Title",
        "private": True,
    }

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/saved_scenarios/456", {"json": {"saved_scenario": update_data}})
    ]


def test_update_saved_scenario_success_all_allowed_fields(dummy_client, fake_response):
    """Test updating all allowed fields of a SavedScenario."""
    body = {
        "id": 456,
        "scenario_id": 123,
        "title": "Full Update",
        "private": True,
        "discarded": False,
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {
        "title": "Full Update",
        "private": True,
        "discarded": False,
    }

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/saved_scenarios/456", {"json": {"saved_scenario": update_data}})
    ]


def test_update_saved_scenario_empty_update_data(dummy_client, fake_response):
    """Test that empty update data returns an error."""
    client = dummy_client({}, method="put")

    update_data = {}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is False
    assert result.data is None
    assert "No fields provided for update" in result.errors
    assert len(client.calls) == 0  # Should not make API call


def test_update_saved_scenario_filters_invalid_fields(dummy_client, fake_response):
    """Test that invalid fields are filtered and warnings are returned."""
    body = {
        "id": 456,
        "title": "Updated Title",
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {
        "title": "Updated Title",  # Valid
        "id": 999,  # Invalid - should be filtered
        "scenario_id": 123,  # Valid
        "created_at": "2019-01-01",  # Invalid - should be filtered
        "invalid_field": "value",  # Invalid - should be filtered
    }

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is True
    assert result.data == body

    # Should have warnings for filtered fields
    expected_warnings = [
        "Ignoring invalid field for update saved scenario: 'id'",
        "Ignoring invalid field for update saved scenario: 'created_at'",
        "Ignoring invalid field for update saved scenario: 'invalid_field'",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    # Should only send valid fields
    expected_payload = {
        "saved_scenario": {"title": "Updated Title", "scenario_id": 123}
    }
    assert client.calls == [("/saved_scenarios/456", {"json": expected_payload})]


def test_update_saved_scenario_only_invalid_fields(dummy_client, fake_response):
    """Test that only invalid fields returns an error."""
    client = dummy_client({}, method="put")

    update_data = {
        "id": 999,  # Invalid
        "invalid_field": "value",  # Invalid
    }

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is False
    assert result.data is None
    assert "No valid fields provided for update" in result.errors
    assert len(client.calls) == 0  # Should not make API call


def test_update_saved_scenario_http_failure_422(dummy_client, fake_response):
    """Test handling of 422 validation error."""
    response = fake_response(ok=False, status_code=422, text="Validation Error")
    client = dummy_client(response, method="put")

    update_data = {"title": ""}  # Invalid empty title

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is False
    assert result.data is None
    assert result.errors == ["422: Validation Error"]


def test_update_saved_scenario_http_failure_401(dummy_client, fake_response):
    """Test handling of 401 unauthorized error."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="put")

    update_data = {"title": "New Title"}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


def test_update_saved_scenario_http_failure_404(dummy_client, fake_response):
    """Test handling of 404 not found error."""
    response = fake_response(ok=False, status_code=404, text="SavedScenario not found")
    client = dummy_client(response, method="put")

    update_data = {"title": "New Title"}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=99999, update_data=update_data
    )
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: SavedScenario not found"]


def test_update_saved_scenario_http_failure_403(dummy_client, fake_response):
    """Test handling of 403 forbidden error (not owner)."""
    response = fake_response(
        ok=False, status_code=403, text="Not authorized to update this scenario"
    )
    client = dummy_client(response, method="put")

    update_data = {"title": "New Title"}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is False
    assert result.data is None
    assert result.errors == ["403: Not authorized to update this scenario"]


def test_update_saved_scenario_connection_error(dummy_client):
    """Test handling of connection errors."""
    client = dummy_client(ConnectionError("Connection failed"), method="put")

    update_data = {"title": "New Title"}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_update_saved_scenario_with_kwargs(dummy_client, fake_response):
    """Test that kwargs are passed through to the request."""
    body = {"id": 456, "title": "Updated Title"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {"title": "Updated Title"}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data, timeout=30
    )
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify basic structure
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/saved_scenarios/456"
    assert client.calls[0][1]["json"] == {"saved_scenario": update_data}


def test_update_saved_scenario_payload_structure(dummy_client, fake_response):
    """Test that the payload is correctly structured for the API."""
    body = {"id": 456, "title": "Test Title"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {
        "title": "Test Title",
    }

    UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )

    # Verify the exact payload structure
    expected_call = (
        "/saved_scenarios/456",
        {"json": {"saved_scenario": update_data}},
    )
    assert client.calls == [expected_call]


def test_update_saved_scenario_discard(dummy_client, fake_response):
    """Test discarding a SavedScenario."""
    body = {
        "id": 456,
        "discarded": True,
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {"discarded": True}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/saved_scenarios/456", {"json": {"saved_scenario": update_data}})
    ]


def test_update_saved_scenario_change_privacy(dummy_client, fake_response):
    """Test changing privacy setting of a SavedScenario."""
    body = {
        "id": 456,
        "private": True,
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {"private": True}

    result = UpdateSavedScenarioRunner.run(
        client, saved_scenario_id=456, update_data=update_data
    )
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/saved_scenarios/456", {"json": {"saved_scenario": update_data}})
    ]
