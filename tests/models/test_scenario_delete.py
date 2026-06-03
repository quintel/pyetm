"""Tests for Scenario deletion functionality (discard and delete)."""

from unittest.mock import Mock
import pytest
from pyetm.models.scenario import Scenario, SavedScenarioError
from pyetm.services.scenario_runners.delete_saved_scenario import (
    DeleteSavedScenarioRunner,
)
from pyetm.services.scenario_runners.destroy_saved_scenario import (
    DestroySavedScenarioRunner,
)


# Tests for discard() - soft-delete, recoverable
def test_scenario_discard_success(monkeypatch, ok_service_result, saved_scenario):
    """Test successful discard of a saved scenario (soft-delete)."""
    discard_response = {"message": "Scenario discarded successfully"}

    monkeypatch.setattr(
        DeleteSavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: ok_service_result(discard_response),
    )

    # Should not raise an exception
    saved_scenario.discard()


def test_scenario_discard_failure(monkeypatch, fail_service_result, saved_scenario):
    """Test discard failure raises SavedScenarioError."""
    monkeypatch.setattr(
        DeleteSavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: fail_service_result(
            ["404: SavedScenario not found"]
        ),
    )

    with pytest.raises(SavedScenarioError, match="Could not discard saved scenario"):
        saved_scenario.discard()


def test_scenario_discard_with_explicit_client(monkeypatch, ok_service_result, mock_client):
    """Test that discard can use an explicitly passed client."""
    discard_response = {"message": "Discarded"}

    # Track which client was used
    used_client = None

    def mock_discard(client, saved_scenario_id):
        nonlocal used_client
        used_client = client
        return ok_service_result(discard_response)

    monkeypatch.setattr(DeleteSavedScenarioRunner, "run", mock_discard)

    scenario = Scenario(id=123, scenario_id=456, title="Test")

    # Discard with explicit client
    scenario.discard(client=mock_client)

    # Verify the explicit client was used
    assert used_client is mock_client


# Tests for delete() - hard-delete with cascade, permanent
def test_scenario_delete_success(monkeypatch, ok_service_result, saved_scenario):
    """Test successful permanent deletion of a saved scenario (hard delete with cascade)."""
    delete_response = {
        "message": "SavedScenario and underlying Session permanently deleted",
        "saved_scenario_id": 123,
        "scenario_id": 456
    }

    monkeypatch.setattr(
        DestroySavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id, scenario_id: ok_service_result(delete_response),
    )

    # Should not raise an exception
    saved_scenario.delete()


def test_scenario_delete_failure(monkeypatch, fail_service_result, saved_scenario):
    """Test permanent deletion failure raises SavedScenarioError."""
    monkeypatch.setattr(
        DestroySavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id, scenario_id: fail_service_result(
            ["Failed to delete SavedScenario: ['404: SavedScenario not found']"]
        ),
    )

    with pytest.raises(SavedScenarioError, match="Could not permanently delete saved scenario"):
        saved_scenario.delete()


def test_scenario_delete_not_authorized(
    monkeypatch, fail_service_result, saved_scenario
):
    """Test permanent deletion failure when not authorized."""
    monkeypatch.setattr(
        DestroySavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id, scenario_id: fail_service_result(
            ["Failed to delete SavedScenario: ['403: Not authorized to delete this scenario']"]
        ),
    )

    with pytest.raises(SavedScenarioError, match="Could not permanently delete saved scenario"):
        saved_scenario.delete()


def test_scenario_delete_with_explicit_client(monkeypatch, ok_service_result, mock_client):
    """Test that permanent delete can use an explicitly passed client."""
    delete_response = {
        "message": "SavedScenario and underlying Session permanently deleted",
        "saved_scenario_id": 123,
        "scenario_id": 456
    }

    # Track which client was used
    used_client = None

    def mock_delete(client, saved_scenario_id, scenario_id):
        nonlocal used_client
        used_client = client
        return ok_service_result(delete_response)

    monkeypatch.setattr(DestroySavedScenarioRunner, "run", mock_delete)

    scenario = Scenario(id=123, scenario_id=456, title="Test")

    # Delete with explicit client
    scenario.delete(client=mock_client)

    # Verify the explicit client was used
    assert used_client is mock_client
