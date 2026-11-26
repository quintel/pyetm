"""Tests for Scenario deletion functionality (discard and delete)."""

from unittest.mock import Mock
import pytest
from pyetm.models.scenario import Scenario, SavedScenarioError
from pyetm.services.scenario_runners.discard_saved_scenario import (
    DiscardSavedScenarioRunner,
)
from pyetm.services.scenario_runners.destroy_saved_scenario import (
    DestroySavedScenarioRunner,
)


# Tests for discard() - soft-delete, recoverable
def test_scenario_discard_success(monkeypatch, ok_service_result, saved_scenario):
    """Test successful discard of a saved scenario (soft-delete)."""
    discard_response = {"message": "Scenario discarded successfully"}

    monkeypatch.setattr(
        DiscardSavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: ok_service_result(discard_response),
    )

    # Should not raise an exception
    saved_scenario.discard()


def test_scenario_discard_failure(monkeypatch, fail_service_result, saved_scenario):
    """Test discard failure raises SavedScenarioError."""
    monkeypatch.setattr(
        DiscardSavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: fail_service_result(
            ["404: SavedScenario not found"]
        ),
    )

    with pytest.raises(SavedScenarioError, match="Could not discard saved scenario"):
        saved_scenario.discard()


def test_scenario_discard_uses_its_own_client(monkeypatch, ok_service_result, mock_client):
    """Discard uses the client the scenario was loaded with."""
    discard_response = {"message": "Discarded"}

    # Track which client was used
    used_client = None

    def mock_discard(client, saved_scenario_id):
        nonlocal used_client
        used_client = client
        return ok_service_result(discard_response)

    monkeypatch.setattr(DiscardSavedScenarioRunner, "run", mock_discard)

    scenario = Scenario(id=123, scenario_id=456, title="Test")

    scenario._client = mock_client
    scenario.discard()

    assert used_client is mock_client


# Tests for delete() - hard-delete with cascade, permanent
def test_scenario_delete_success(monkeypatch, ok_service_result, saved_scenario):
    """Test successful permanent deletion of a saved scenario through MyETM's purge."""
    delete_response = {
        "message": "SavedScenario permanently deleted",
        "saved_scenario_id": 123,
    }

    monkeypatch.setattr(
        DestroySavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: ok_service_result(delete_response),
    )

    # Should not raise an exception
    saved_scenario.delete()


def test_scenario_delete_failure(monkeypatch, fail_service_result, saved_scenario):
    """Test permanent deletion failure raises SavedScenarioError."""
    monkeypatch.setattr(
        DestroySavedScenarioRunner,
        "run",
        lambda client, saved_scenario_id: fail_service_result(
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
        lambda client, saved_scenario_id: fail_service_result(
            ["Failed to delete SavedScenario: ['403: Not authorized to delete this scenario']"]
        ),
    )

    with pytest.raises(SavedScenarioError, match="Could not permanently delete saved scenario"):
        saved_scenario.delete()


def test_scenario_delete_uses_its_own_client(monkeypatch, ok_service_result, mock_client):
    """Permanent delete uses the client the scenario was loaded with."""
    delete_response = {
        "message": "SavedScenario permanently deleted",
        "saved_scenario_id": 123,
    }

    # Track which client was used
    used_client = None

    def mock_delete(client, saved_scenario_id):
        nonlocal used_client
        used_client = client
        return ok_service_result(delete_response)

    monkeypatch.setattr(DestroySavedScenarioRunner, "run", mock_delete)

    scenario = Scenario(id=123, scenario_id=456, title="Test")

    scenario._client = mock_client
    scenario.delete()

    assert used_client is mock_client
