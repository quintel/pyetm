from pyetm.services.scenario_runners.destroy_saved_scenario import (
    DestroySavedScenarioRunner,
)
from types import SimpleNamespace


def test_destroy_saved_scenario_success(fake_response):
    """Test successful destruction of a SavedScenario and its Session."""
    # Create a custom client that returns different responses for each call
    call_count = [0]
    calls_made = []

    saved_scenario_response = fake_response(
        ok=True, status_code=200, json_data={"message": "SavedScenario deleted"}
    )
    session_response = fake_response(
        ok=True, status_code=200, json_data={"message": "Session deleted"}
    )

    def mock_delete(url, params=None, json=None, **kwargs):
        calls_made.append((url, None))
        call_count[0] += 1
        if call_count[0] == 1:
            return saved_scenario_response
        else:
            return session_response

    client = SimpleNamespace(
        session=SimpleNamespace(delete=mock_delete),
        calls=calls_made
    )

    result = DestroySavedScenarioRunner.run(
        client,
        saved_scenario_id=123,
        scenario_id=456
    )

    assert result.success is True
    assert "SavedScenario and underlying Session permanently deleted" in result.data["message"]
    assert result.data["saved_scenario_id"] == 123
    assert result.data["scenario_id"] == 456
    assert result.errors == []
    # Verify both API calls were made
    assert len(client.calls) == 2
    assert client.calls[0][0] == "/saved_scenarios/123"
    assert client.calls[1][0] == "/scenarios/456"


def test_destroy_saved_scenario_failure_first_call(dummy_client, fake_response):
    """Test handling when SavedScenario deletion fails."""
    saved_scenario_response = fake_response(
        ok=False, status_code=404, text="SavedScenario not found"
    )
    client = dummy_client(saved_scenario_response, method="delete")

    result = DestroySavedScenarioRunner.run(
        client,
        saved_scenario_id=123,
        scenario_id=456
    )

    assert result.success is False
    assert result.data is None
    assert "Failed to delete SavedScenario" in result.errors[0]
    # Should only have made one call (first one failed)
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/saved_scenarios/123"


def test_destroy_saved_scenario_failure_second_call(fake_response):
    """Test handling when Session deletion fails after SavedScenario deletion succeeds."""
    call_count = [0]
    calls_made = []

    saved_scenario_response = fake_response(
        ok=True, status_code=200, json_data={"message": "SavedScenario deleted"}
    )
    session_response = fake_response(
        ok=False, status_code=404, text="Session not found"
    )

    def mock_delete(url, params=None, json=None, **kwargs):
        calls_made.append((url, None))
        call_count[0] += 1
        if call_count[0] == 1:
            return saved_scenario_response
        else:
            return session_response

    client = SimpleNamespace(
        session=SimpleNamespace(delete=mock_delete),
        calls=calls_made
    )

    result = DestroySavedScenarioRunner.run(
        client,
        saved_scenario_id=123,
        scenario_id=456
    )

    assert result.success is False
    assert result.data is None
    assert "SavedScenario deleted but failed to delete Session" in result.errors[0]
    # Both calls should have been made
    assert len(client.calls) == 2


def test_destroy_saved_scenario_invalid_saved_scenario_id(dummy_client):
    """Test that invalid SavedScenario IDs are rejected."""
    client = dummy_client({}, method="delete")

    result = DestroySavedScenarioRunner.run(
        client,
        saved_scenario_id=-1,
        scenario_id=456
    )

    assert result.success is False
    assert result.data is None
    assert "Invalid saved_scenario_id" in result.errors[0]
    assert len(client.calls) == 0


def test_destroy_saved_scenario_invalid_scenario_id(dummy_client):
    """Test that invalid Session IDs are rejected."""
    client = dummy_client({}, method="delete")

    result = DestroySavedScenarioRunner.run(
        client,
        saved_scenario_id=123,
        scenario_id=0
    )

    assert result.success is False
    assert result.data is None
    assert "Invalid scenario_id" in result.errors[0]
    assert len(client.calls) == 0


def test_destroy_saved_scenario_http_failure_401(dummy_client, fake_response):
    """Test handling of 401 unauthorized error."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="delete")

    result = DestroySavedScenarioRunner.run(
        client,
        saved_scenario_id=123,
        scenario_id=456
    )

    assert result.success is False
    assert result.data is None
    assert "Failed to delete SavedScenario" in result.errors[0]


def test_destroy_saved_scenario_http_failure_403(dummy_client, fake_response):
    """Test handling of 403 forbidden error (not owner)."""
    response = fake_response(
        ok=False, status_code=403, text="Not authorized to delete this scenario"
    )
    client = dummy_client(response, method="delete")

    result = DestroySavedScenarioRunner.run(
        client,
        saved_scenario_id=123,
        scenario_id=456
    )

    assert result.success is False
    assert result.data is None
    assert "Failed to delete SavedScenario" in result.errors[0]
