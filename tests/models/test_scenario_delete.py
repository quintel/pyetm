"""Tests for Scenario deletion functionality."""

from unittest.mock import Mock
import pytest
from pyetm.models.scenario import Scenario, SavedScenarioError
from pyetm.services.scenario_runners.delete_saved_scenario import (
    DeleteSavedScenarioRunner,
)


def test_scenario_delete_success(monkeypatch, ok_service_result, saved_scenario):
    """Test successful deletion of a saved scenario."""
    delete_response = {"message": "Scenario deleted successfully"}

    monkeypatch.setattr(
        DeleteSavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: ok_service_result(delete_response),
    )

    # Should not raise an exception
    saved_scenario.delete()


def test_scenario_delete_failure(monkeypatch, fail_service_result, saved_scenario):
    """Test deletion failure raises SavedScenarioError."""
    monkeypatch.setattr(
        DeleteSavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: fail_service_result(
            ["404: SavedScenario not found"]
        ),
    )

    with pytest.raises(SavedScenarioError, match="Could not delete saved scenario"):
        saved_scenario.delete()


def test_scenario_delete_not_authorized(
    monkeypatch, fail_service_result, saved_scenario
):
    """Test deletion failure when not authorized."""
    monkeypatch.setattr(
        DeleteSavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: fail_service_result(
            ["403: Not authorized to delete this scenario"]
        ),
    )

    with pytest.raises(SavedScenarioError, match="Could not delete saved scenario"):
        saved_scenario.delete()


def test_scenario_delete_with_explicit_client(monkeypatch, ok_service_result, mock_client):
    """Test that delete can use an explicitly passed client."""
    delete_response = {"message": "Deleted"}

    # Track which client was used
    used_client = None

    def mock_delete(client, saved_scenario_id):
        nonlocal used_client
        used_client = client
        return ok_service_result(delete_response)

    monkeypatch.setattr(DeleteSavedScenarioRunner, "run", mock_delete)

    scenario = Scenario(id=123, scenario_id=456, title="Test")

    # Delete with explicit client
    scenario.delete(client=mock_client)

    # Verify the explicit client was used
    assert used_client is mock_client
