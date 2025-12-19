from unittest.mock import Mock, patch
import pytest
from datetime import datetime
from pyetm.models.scenario import Scenario, SavedScenarioError
from pyetm.models.session import Session
from pyetm.services.scenario_runners.create_saved_scenario import (
    CreateSavedScenarioRunner,
)
from pyetm.services.scenario_runners.update_saved_scenario import (
    UpdateSavedScenarioRunner,
)
from pyetm.services.scenario_runners.fetch_saved_scenario import (
    FetchSavedScenarioRunner,
)


# --- Model Validation Tests --- #


def test_saved_scenario_session_validation_minimal():
    """Test SavedScenario model validates with minimal required fields."""
    data = {
        "id": 1,
        "scenario_id": 100,
        "title": "Test Scenario",
    }
    saved_scenario = Scenario.model_validate(data)
    assert saved_scenario.id == 1
    assert saved_scenario.scenario_id == 100
    assert saved_scenario.title == "Test Scenario"
    assert saved_scenario.description is None
    assert saved_scenario.private is False


def test_saved_scenario_session_validation_full(saved_scenario_data):
    """Test SavedScenario model validates with all fields."""
    saved_scenario = Scenario.model_validate(saved_scenario_data)
    assert saved_scenario.id == 456
    assert saved_scenario.scenario_id == 123
    assert saved_scenario.title == "My Saved Scenario"
    assert saved_scenario.description == "A test description"
    assert saved_scenario.private is False
    assert saved_scenario.area_code == "nl"
    assert saved_scenario.end_year == 2050


def test_saved_scenario_session_with_nested_scenario():
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
    saved_scenario = Scenario.model_validate(data)
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

    saved_scenario = Scenario.create(params, client=mock_client)
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

    saved_scenario = Scenario.create(params, client=mock_client)
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
        Scenario.create(params, client=mock_client)


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

    saved_scenario = Scenario.create(params, client=mock_client)
    # description should be set from params since it wasn't in response
    assert saved_scenario.description == "Local description"


# --- from_scenario Tests --- #


def test_from_scenario_success(monkeypatch, ok_service_result, mock_client):
    """Test creating SavedScenario from a Scenario instance."""
    # Create a mock scenario
    scenario = Mock(spec=Session)
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

    saved_scenario = Scenario.from_scenario(
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
    scenario = Mock(spec=Session)
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

    Scenario.from_scenario(
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


# --- session Property Tests --- #


def test_session_property_from_nested_data(saved_scenario):
    """Test session property creates Scenario from nested data."""
    saved_scenario.scenario = {
        "id": 123,
        "area_code": "nl",
        "end_year": 2050,
    }

    scenario = saved_scenario.session
    assert scenario.id == 123
    assert scenario.area_code == "nl"
    assert scenario.end_year == 2050


def test_session_property_caches_result(saved_scenario):
    """Test session property caches the Scenario instance."""
    saved_scenario.scenario = {
        "id": 123,
        "area_code": "nl",
        "end_year": 2050,
    }

    scenario1 = saved_scenario.session
    scenario2 = saved_scenario.session

    # Should return the same cached instance
    assert scenario1 is scenario2


def test_session_property_returns_cached_model(saved_scenario):
    """Test session property returns cached model if set."""
    cached_scenario = Mock(spec=Session)
    cached_scenario.id = 999
    saved_scenario._scenario_session = cached_scenario

    scenario = saved_scenario.session
    assert scenario is cached_scenario
    assert scenario.id == 999


def test_session_property_fetches_if_no_nested_data(monkeypatch, saved_scenario):
    """Test session property fetches if no nested scenario data."""
    saved_scenario.scenario = None
    saved_scenario._scenario_session = None

    fetched_scenario = Mock(spec=Session)
    fetched_scenario.id = 123

    with patch.object(Session, "load", return_value=fetched_scenario) as mock_load:
        scenario = saved_scenario.session

        mock_load.assert_called_once_with(123)
        assert scenario is fetched_scenario


# --- Delegation Tests --- #


def test_saved_scenario_delegates_property_access(saved_scenario):
    """Test SavedScenario delegates property access to underlying session."""
    # Mock the session
    mock_session = Mock(spec=Session)
    mock_session.inputs = Mock()
    mock_session.sortables = Mock()
    mock_session.custom_curves = Mock()
    mock_session.output_curves = Mock()
    mock_session.couplings = Mock()
    mock_session.version = "latest"
    mock_session.start_year = 2020
    mock_session.url = "http://test.com"

    saved_scenario._scenario_session = mock_session

    # Test property delegation
    assert saved_scenario.inputs is mock_session.inputs
    assert saved_scenario.sortables is mock_session.sortables
    assert saved_scenario.custom_curves is mock_session.custom_curves
    assert saved_scenario.output_curves is mock_session.output_curves
    assert saved_scenario.couplings is mock_session.couplings
    assert saved_scenario.version == "latest"
    assert saved_scenario.start_year == 2020
    assert saved_scenario.url == "http://test.com"


def test_saved_scenario_delegates_method_calls(saved_scenario):
    """Test SavedScenario delegates method calls to underlying session."""
    # Mock the session
    mock_session = Mock(spec=Session)
    mock_session.user_values.return_value = {"input1": 42}
    mock_session.update_user_values = Mock()
    mock_session.update_sortables = Mock()
    mock_session.results.return_value = Mock()

    saved_scenario._scenario_session = mock_session

    # Test method delegation
    values = saved_scenario.user_values()
    assert values == {"input1": 42}
    mock_session.user_values.assert_called_once()

    saved_scenario.update_user_values({"input1": 50})
    mock_session.update_user_values.assert_called_once_with(
        {"input1": 50}, skip_upload=False
    )

    saved_scenario.update_sortables({"demand": ["a", "b"]})
    mock_session.update_sortables.assert_called_once_with({"demand": ["a", "b"]})

    saved_scenario.results()
    mock_session.results.assert_called_once()


def test_saved_scenario_delegation_transparent_to_user(saved_scenario):
    """Test that SavedScenario and Scenario can be used interchangeably."""
    # Mock a scenario with some behavior
    mock_session = Mock(spec=Session)
    mock_session.inputs = Mock()
    mock_session.inputs.is_valid_update.return_value = {}
    mock_session.update_user_values = Mock()
    mock_session.identifier.return_value = "test_scenario"

    saved_scenario._scenario_session = mock_session

    # User code should work identically whether they have a Scenario or SavedScenario
    identifier = saved_scenario.identifier()
    assert identifier == "test_scenario"

    # Both should support the same operations
    saved_scenario.update_user_values({"test": 123})
    mock_session.update_user_values.assert_called_once_with(
        {"test": 123}, skip_upload=False
    )
