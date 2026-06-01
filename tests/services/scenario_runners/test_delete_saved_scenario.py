from pyetm.services.scenario_runners.delete_saved_scenario import (
    DeleteSavedScenarioRunner,
)


def test_delete_saved_scenario_success(dummy_client, fake_response):
    """Test successful deletion of a SavedScenario."""
    body = {"message": "Saved scenario deleted successfully"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="delete")

    result = DeleteSavedScenarioRunner.run(client, saved_scenario_id=123)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/saved_scenarios/123", None)]


def test_delete_saved_scenario_http_failure_404(dummy_client, fake_response):
    """Test handling of 404 not found error."""
    response = fake_response(ok=False, status_code=404, text="SavedScenario not found")
    client = dummy_client(response, method="delete")

    result = DeleteSavedScenarioRunner.run(client, saved_scenario_id=99999)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: SavedScenario not found"]


def test_delete_saved_scenario_http_failure_401(dummy_client, fake_response):
    """Test handling of 401 unauthorized error."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="delete")

    result = DeleteSavedScenarioRunner.run(client, saved_scenario_id=123)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


def test_delete_saved_scenario_http_failure_403(dummy_client, fake_response):
    """Test handling of 403 forbidden error (not owner)."""
    response = fake_response(
        ok=False, status_code=403, text="Not authorized to delete this scenario"
    )
    client = dummy_client(response, method="delete")

    result = DeleteSavedScenarioRunner.run(client, saved_scenario_id=123)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["403: Not authorized to delete this scenario"]


def test_delete_saved_scenario_invalid_id_negative(dummy_client):
    """Test that negative IDs are rejected."""
    client = dummy_client({}, method="delete")

    result = DeleteSavedScenarioRunner.run(client, saved_scenario_id=-1)
    assert result.success is False
    assert result.data is None
    assert "Invalid saved_scenario_id" in result.errors[0]
    assert len(client.calls) == 0


def test_delete_saved_scenario_invalid_id_zero(dummy_client):
    """Test that zero ID is rejected."""
    client = dummy_client({}, method="delete")

    result = DeleteSavedScenarioRunner.run(client, saved_scenario_id=0)
    assert result.success is False
    assert result.data is None
    assert "Invalid saved_scenario_id" in result.errors[0]
    assert len(client.calls) == 0


def test_delete_saved_scenario_connection_error(dummy_client):
    """Test handling of connection errors."""
    client = dummy_client(ConnectionError("Connection failed"), method="delete")

    result = DeleteSavedScenarioRunner.run(client, saved_scenario_id=123)
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_delete_saved_scenario_with_kwargs(dummy_client, fake_response):
    """Test that kwargs are passed through to the request."""
    body = {"message": "Deleted"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="delete")

    result = DeleteSavedScenarioRunner.run(
        client, saved_scenario_id=123, timeout=30
    )
    assert result.success is True
    assert result.data == body
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/saved_scenarios/123"
