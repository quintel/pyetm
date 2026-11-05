from pyetm.services.scenario_runners.destroy_saved_scenario import (
    DestroySavedScenarioRunner,
)


def test_destroy_saved_scenario_success(dummy_client, fake_response):
    """MyETM's purge is the whole delete: the engine's Session delete is never called."""
    response = fake_response(ok=True, status_code=200, json_data={"message": "deleted"})
    client = dummy_client(response, method="delete")

    result = DestroySavedScenarioRunner.run(client, saved_scenario_id=123)

    assert result.success is True
    assert result.data == {"message": "SavedScenario permanently deleted", "saved_scenario_id": 123}
    assert [call[0] for call in client.calls] == ["/saved_scenarios/123"]


def test_destroy_saved_scenario_failure_first_call(dummy_client, fake_response):
    """Test handling when SavedScenario deletion fails."""
    saved_scenario_response = fake_response(
        ok=False, status_code=404, text="SavedScenario not found"
    )
    client = dummy_client(saved_scenario_response, method="delete")

    result = DestroySavedScenarioRunner.run(client, saved_scenario_id=123)

    assert result.success is False
    assert result.data is None
    assert "Failed to delete SavedScenario" in result.errors[0]
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/saved_scenarios/123"


def test_destroy_saved_scenario_invalid_saved_scenario_id(dummy_client):
    """Test that invalid SavedScenario IDs are rejected."""
    client = dummy_client({}, method="delete")

    result = DestroySavedScenarioRunner.run(client, saved_scenario_id=-1)

    assert result.success is False
    assert result.data is None
    assert "Invalid saved_scenario_id" in result.errors[0]
    assert len(client.calls) == 0


def test_destroy_saved_scenario_http_failure_401(dummy_client, fake_response):
    """Test handling of 401 unauthorized error."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="delete")

    result = DestroySavedScenarioRunner.run(client, saved_scenario_id=123)

    assert result.success is False
    assert result.data is None
    assert "Failed to delete SavedScenario" in result.errors[0]


def test_destroy_saved_scenario_http_failure_403(dummy_client, fake_response):
    """Test handling of 403 forbidden error (not owner)."""
    response = fake_response(
        ok=False, status_code=403, text="Not authorized to delete this scenario"
    )
    client = dummy_client(response, method="delete")

    result = DestroySavedScenarioRunner.run(client, saved_scenario_id=123)

    assert result.success is False
    assert result.data is None
    assert "Failed to delete SavedScenario" in result.errors[0]
