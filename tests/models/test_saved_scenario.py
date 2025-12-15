from unittest.mock import Mock, patch
import pytest
from datetime import datetime
from pyetm.models.saved_scenario import SavedScenario, SavedScenarioError
from pyetm.models.scenario import Scenario
from pyetm.services.scenario_runners.create_saved_scenario import (
    CreateSavedScenarioRunner,
)
from pyetm.services.scenario_runners.update_saved_scenario import (
    UpdateSavedScenarioRunner,
)


# --- Model Validation Tests --- #


def test_saved_scenario_model_validation_minimal():
    """Test SavedScenario model validates with minimal required fields."""
    data = {
        "id": 1,
        "scenario_id": 100,
        "title": "Test Scenario",
    }
    saved_scenario = SavedScenario.model_validate(data)
    assert saved_scenario.id == 1
    assert saved_scenario.scenario_id == 100
    assert saved_scenario.title == "Test Scenario"
    assert saved_scenario.description is None
    assert saved_scenario.private is False


def test_saved_scenario_model_validation_full(saved_scenario_data):
    """Test SavedScenario model validates with all fields."""
    saved_scenario = SavedScenario.model_validate(saved_scenario_data)
    assert saved_scenario.id == 456
    assert saved_scenario.scenario_id == 123
    assert saved_scenario.title == "My Saved Scenario"
    assert saved_scenario.description == "A test description"
    assert saved_scenario.private is False
    assert saved_scenario.area_code == "nl"
    assert saved_scenario.end_year == 2050


def test_saved_scenario_model_with_nested_scenario():
    """Test SavedScenario model with nested scenario data."""
    data = {
        "id": 1,
        "scenario_id": 100,
        "title": "Test Scenario",
        "scenario": {
            "id": 100,
            "area_code": "nl",
            "end_year": 2050,
        },
    }
    saved_scenario = SavedScenario.model_validate(data)
    assert saved_scenario.scenario is not None
    assert saved_scenario.scenario["id"] == 100


# --- Create Tests --- #


def test_create_saved_scenario_success(monkeypatch, ok_service_result, mock_client):
    """Test successful SavedScenario creation."""
    created_data = {
        "id": 789,
        "scenario_id": 123,
        "title": "New Saved Scenario",
        "description": "Created via API",
        "private": True,
    }

    monkeypatch.setattr(
        CreateSavedScenarioRunner,
        "run",
        lambda client, params: ok_service_result(created_data),
    )

    params = {
        "scenario_id": 123,
        "title": "New Saved Scenario",
        "description": "Created via API",
        "private": True,
    }

    saved_scenario = SavedScenario.create(params, client=mock_client)
    assert saved_scenario.id == 789
    assert saved_scenario.scenario_id == 123
    assert saved_scenario.title == "New Saved Scenario"
    assert saved_scenario.private is True
    assert len(saved_scenario.warnings) == 0


def test_create_saved_scenario_with_warnings(
    monkeypatch, ok_service_result, mock_client
):
    """Test SavedScenario creation with warnings."""
    created_data = {
        "id": 790,
        "scenario_id": 123,
        "title": "Saved Scenario",
    }
    warnings = ["Ignoring invalid field for create saved scenario: 'invalid_field'"]

    monkeypatch.setattr(
        CreateSavedScenarioRunner,
        "run",
        lambda client, params: ok_service_result(created_data, warnings),
    )

    params = {
        "scenario_id": 123,
        "title": "Saved Scenario",
        "invalid_field": "should_be_ignored",
    }

    saved_scenario = SavedScenario.create(params, client=mock_client)
    assert saved_scenario.id == 790
    base_warnings = saved_scenario.warnings.get_by_field("base")
    assert len(base_warnings) == 1
    assert base_warnings[0].message == warnings[0]


def test_create_saved_scenario_failure(monkeypatch, fail_service_result, mock_client):
    """Test SavedScenario creation failure raises SavedScenarioError."""
    monkeypatch.setattr(
        CreateSavedScenarioRunner,
        "run",
        lambda client, params: fail_service_result(["Missing required field: title"]),
    )

    params = {"scenario_id": 123}  # Missing title

    with pytest.raises(SavedScenarioError, match="Could not create saved scenario"):
        SavedScenario.create(params, client=mock_client)


def test_create_saved_scenario_preserves_params_not_in_response(
    monkeypatch, ok_service_result, mock_client
):
    """Test that params not returned by API are still set on the instance."""
    created_data = {
        "id": 791,
        "scenario_id": 123,
        "title": "Saved Scenario",
        # description not in response
    }

    monkeypatch.setattr(
        CreateSavedScenarioRunner,
        "run",
        lambda client, params: ok_service_result(created_data),
    )

    params = {
        "scenario_id": 123,
        "title": "Saved Scenario",
        "description": "Local description",
    }

    saved_scenario = SavedScenario.create(params, client=mock_client)
    # description should be set from params since it wasn't in response
    assert saved_scenario.description == "Local description"


# --- from_scenario Tests --- #


def test_from_scenario_success(monkeypatch, ok_service_result, mock_client):
    """Test creating SavedScenario from a Scenario instance."""
    # Create a mock scenario
    scenario = Mock(spec=Scenario)
    scenario.id = 999

    created_data = {
        "id": 800,
        "scenario_id": 999,
        "title": "From Scenario",
        "description": "Created from scenario",
        "private": False,
    }

    monkeypatch.setattr(
        CreateSavedScenarioRunner,
        "run",
        lambda client, params: ok_service_result(created_data),
    )

    saved_scenario = SavedScenario.from_scenario(
        scenario,
        title="From Scenario",
        client=mock_client,
        description="Created from scenario",
    )

    assert saved_scenario.id == 800
    assert saved_scenario.scenario_id == 999
    assert saved_scenario.title == "From Scenario"
    assert saved_scenario.description == "Created from scenario"


def test_from_scenario_with_kwargs(monkeypatch, ok_service_result, mock_client):
    """Test from_scenario passes kwargs correctly."""
    scenario = Mock(spec=Scenario)
    scenario.id = 1000

    created_data = {
        "id": 801,
        "scenario_id": 1000,
        "title": "Private Scenario",
        "private": True,
    }

    captured_params = {}

    def capture_run(client, params):
        captured_params.update(params)
        return ok_service_result(created_data)

    monkeypatch.setattr(CreateSavedScenarioRunner, "run", capture_run)

    SavedScenario.from_scenario(
        scenario, title="Private Scenario", client=mock_client, private=True
    )

    assert captured_params["scenario_id"] == 1000
    assert captured_params["title"] == "Private Scenario"
    assert captured_params["private"] is True


# --- Update Tests --- #


def test_update_saved_scenario_success(
    monkeypatch, ok_service_result, saved_scenario, mock_client
):
    """Test successful SavedScenario update."""
    updated_data = {
        "id": 456,
        "title": "Updated Title",
        "description": "Updated description",
    }

    monkeypatch.setattr(
        UpdateSavedScenarioRunner,
        "run",
        lambda client, id, kwargs: ok_service_result(updated_data),
    )

    saved_scenario.update(
        mock_client, title="Updated Title", description="Updated description"
    )

    assert saved_scenario.title == "Updated Title"
    assert saved_scenario.description == "Updated description"
    assert len(saved_scenario.warnings) == 0


def test_update_saved_scenario_with_warnings(
    monkeypatch, ok_service_result, saved_scenario, mock_client
):
    """Test SavedScenario update with warnings."""
    updated_data = {"id": 456, "title": "New Title"}
    warnings = ["Ignoring invalid field for update saved scenario: 'invalid_field'"]

    monkeypatch.setattr(
        UpdateSavedScenarioRunner,
        "run",
        lambda client, id, kwargs: ok_service_result(updated_data, warnings),
    )

    saved_scenario.update(mock_client, title="New Title", invalid_field="ignored")

    assert saved_scenario.title == "New Title"
    update_warnings = saved_scenario.warnings.get_by_field("update")
    assert len(update_warnings) == 1
    assert update_warnings[0].message == warnings[0]


def test_update_saved_scenario_failure(
    monkeypatch, fail_service_result, saved_scenario, mock_client
):
    """Test SavedScenario update failure raises SavedScenarioError."""
    monkeypatch.setattr(
        UpdateSavedScenarioRunner,
        "run",
        lambda client, id, kwargs: fail_service_result(["403: Forbidden"]),
    )

    with pytest.raises(SavedScenarioError, match="Could not update saved scenario"):
        saved_scenario.update(mock_client, title="New Title")


def test_update_saved_scenario_applies_response_data(
    monkeypatch, ok_service_result, saved_scenario, mock_client
):
    """Test that update applies response data to the instance."""
    original_title = saved_scenario.title
    updated_data = {
        "id": 456,
        "title": "Server Updated Title",
        "private": True,
        "updated_at": "2025-12-15T10:00:00Z",
    }

    monkeypatch.setattr(
        UpdateSavedScenarioRunner,
        "run",
        lambda client, id, kwargs: ok_service_result(updated_data),
    )

    saved_scenario.update(mock_client, title="Requested Title")

    # Should use server response, not the requested value
    assert saved_scenario.title == "Server Updated Title"
    assert saved_scenario.private is True


def test_update_saved_scenario_applies_kwargs_if_not_in_response(
    monkeypatch, ok_service_result, saved_scenario, mock_client
):
    """Test that kwargs are applied if not in response data."""
    updated_data = {"id": 456}  # Response doesn't include title

    monkeypatch.setattr(
        UpdateSavedScenarioRunner,
        "run",
        lambda client, id, kwargs: ok_service_result(updated_data),
    )

    saved_scenario.update(mock_client, title="Local Title")

    # Should use local value since it wasn't in response
    assert saved_scenario.title == "Local Title"


def test_update_saved_scenario_discard(
    monkeypatch, ok_service_result, saved_scenario, mock_client
):
    """Test discarding a SavedScenario."""
    updated_data = {"id": 456, "discarded": True}

    monkeypatch.setattr(
        UpdateSavedScenarioRunner,
        "run",
        lambda client, id, kwargs: ok_service_result(updated_data),
    )

    saved_scenario.update(mock_client, discarded=True)
    # discarded is not a model field, but should not raise an error


def test_update_saved_scenario_change_privacy(
    monkeypatch, ok_service_result, saved_scenario, mock_client
):
    """Test changing privacy setting."""
    assert saved_scenario.private is False

    updated_data = {"id": 456, "private": True}

    monkeypatch.setattr(
        UpdateSavedScenarioRunner,
        "run",
        lambda client, id, kwargs: ok_service_result(updated_data),
    )

    saved_scenario.update(mock_client, private=True)
    assert saved_scenario.private is True


# --- get_scenario Tests --- #


def test_get_scenario_from_nested_data(saved_scenario, mock_client):
    """Test get_scenario creates Scenario from nested data."""
    saved_scenario.scenario = {
        "id": 123,
        "area_code": "nl",
        "end_year": 2050,
    }

    scenario = saved_scenario.get_scenario(mock_client)
    assert scenario.id == 123
    assert scenario.area_code == "nl"
    assert scenario.end_year == 2050


def test_get_scenario_caches_result(saved_scenario, mock_client):
    """Test get_scenario caches the Scenario instance."""
    saved_scenario.scenario = {
        "id": 123,
        "area_code": "nl",
        "end_year": 2050,
    }

    scenario1 = saved_scenario.get_scenario(mock_client)
    scenario2 = saved_scenario.get_scenario(mock_client)

    # Should return the same cached instance
    assert scenario1 is scenario2


def test_get_scenario_returns_cached_model(saved_scenario, mock_client):
    """Test get_scenario returns cached model if set."""
    cached_scenario = Mock(spec=Scenario)
    cached_scenario.id = 999
    saved_scenario._scenario_model = cached_scenario

    scenario = saved_scenario.get_scenario(mock_client)
    assert scenario is cached_scenario
    assert scenario.id == 999


def test_get_scenario_fetches_if_no_nested_data(
    monkeypatch, saved_scenario, mock_client
):
    """Test get_scenario fetches if no nested scenario data."""
    saved_scenario.scenario = None
    saved_scenario._scenario_model = None

    fetched_scenario = Mock(spec=Scenario)
    fetched_scenario.id = 123

    with patch.object(Scenario, "load", return_value=fetched_scenario) as mock_load:
        scenario = saved_scenario.get_scenario(mock_client)

        mock_load.assert_called_once_with(123)
        assert scenario is fetched_scenario
