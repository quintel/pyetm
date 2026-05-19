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
    assert saved_scenario.private is False


def test_saved_scenario_session_validation_full(saved_scenario_data):
    """Test SavedScenario model validates with all fields."""
    saved_scenario = Scenario.model_validate(saved_scenario_data)
    assert saved_scenario.id == 456
    assert saved_scenario.scenario_id == 123
    assert saved_scenario.title == "My Saved Scenario"
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
        "private": True,
    }

    saved_scenario = Scenario.create(params, client=mock_client)
    assert saved_scenario.id == 789
    assert saved_scenario.scenario_id == 123
    assert saved_scenario.title == "New Saved Scenario"
    assert saved_scenario.private is True
    assert len(saved_scenario.warnings) == 0


def test_create_saved_scenario_with_warnings(monkeypatch, ok_service_result, mock_client):
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
        # private not in response
    }

    monkeypatch.setattr(
        CreateSavedScenarioRunner,
        "run",
        lambda client, params: ok_service_result(created_data),
    )

    params = {
        "scenario_id": 123,
        "title": "Saved Scenario",
        "private": True,
    }

    saved_scenario = Scenario.create(params, client=mock_client)
    # private should be set from params since it wasn't in response
    assert saved_scenario.private is True


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
        private=False,
    )

    assert saved_scenario.id == 800
    assert saved_scenario.scenario_id == 999
    assert saved_scenario.title == "From Scenario"
    assert saved_scenario.private is False


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

    Scenario.from_scenario(scenario, title="Private Scenario", client=mock_client, private=True)

    assert captured_params["scenario_id"] == 1000
    assert captured_params["title"] == "Private Scenario"
    assert captured_params["private"] is True


# --- Update Tests --- #


def test_update_saved_scenario_success(monkeypatch, ok_service_result, saved_scenario, mock_client):
    """Test successful SavedScenario update."""
    updated_data = {
        "id": 456,
        "title": "Updated Title",
        "private": True,
    }

    monkeypatch.setattr(
        UpdateSavedScenarioRunner,
        "run",
        lambda client, id, kwargs: ok_service_result(updated_data),
    )

    saved_scenario.update(mock_client, title="Updated Title", private=True)

    assert saved_scenario.title == "Updated Title"
    assert saved_scenario.private is True
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


def test_update_saved_scenario_discard(monkeypatch, ok_service_result, saved_scenario, mock_client):
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
    mock_session.hourly_output_curves = Mock()
    mock_session.couplings = Mock()
    mock_session.version = "latest"
    mock_session.start_year = 2020
    mock_session.url = "http://test.com"

    saved_scenario._scenario_session = mock_session

    # Test property delegation
    assert saved_scenario.inputs is mock_session.inputs
    assert saved_scenario.sortables is mock_session.sortables
    assert saved_scenario.custom_curves is mock_session.custom_curves
    assert saved_scenario.hourly_output_curves is mock_session.hourly_output_curves
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
    mock_session.update_user_values.assert_called_once_with({"input1": 50}, skip_upload=False)

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

    # SavedScenario has its own identifier logic that prioritizes saved title
    # This saved_scenario fixture has title="My Saved Scenario", so that takes priority
    identifier = saved_scenario.identifier()
    assert identifier == "My Saved Scenario"

    # Both should support the same operations
    saved_scenario.update_user_values({"test": 123})
    mock_session.update_user_values.assert_called_once_with({"test": 123}, skip_upload=False)


# ------ interpolate ------ #


def test_saved_scenario_interpolate_success(monkeypatch, ok_service_result):
    """Test successful batch interpolation of saved scenarios"""
    # Mock Session.interpolate to return interpolated sessions
    interpolated_sessions = [
        Session(id=77771, area_code="nl", end_year=2040, start_year=2023),
        Session(id=77772, area_code="nl", end_year=2060, start_year=2023),
    ]

    def mock_interpolate(sessions, end_years, client):
        return interpolated_sessions

    monkeypatch.setattr(Session, "interpolate", mock_interpolate)

    # Mock Session.save to return SavedScenario instances
    def mock_save(self, client, title, **kwargs):
        saved_data = {
            "id": self.id + 10000,
            "scenario_id": self.id,
            "title": title,
            **kwargs,
        }
        return Scenario.model_validate(saved_data)

    monkeypatch.setattr(Session, "save", mock_save)

    # Create saved scenarios
    saved_2030 = Scenario(id=1001, scenario_id=12345, title="Saved 2030", end_year=2030)
    saved_2050 = Scenario(id=1002, scenario_id=45678, title="Saved 2050", end_year=2050)
    saved_2070 = Scenario(id=1003, scenario_id=67890, title="Saved 2070", end_year=2070)

    # Mock session property
    for ss in [saved_2030, saved_2050, saved_2070]:
        ss._scenario_session = Session(id=ss.scenario_id, area_code="nl", end_year=ss.end_year)

    result = Scenario.interpolate([saved_2030, saved_2050, saved_2070], [2040, 2060])

    assert len(result) == 2
    assert all(isinstance(s, Scenario) for s in result)
    assert result[0].title == "Interpolated to 2040"
    assert result[1].title == "Interpolated to 2060"


def test_saved_scenario_interpolate_with_custom_titles(monkeypatch):
    """Test batch interpolation with custom titles"""
    interpolated_sessions = [
        Session(id=88881, area_code="nl", end_year=2040, start_year=2023),
    ]

    def mock_interpolate(sessions, end_years, client):
        return interpolated_sessions

    monkeypatch.setattr(Session, "interpolate", mock_interpolate)

    saved_titles = []

    def mock_save(self, client, title, **kwargs):
        saved_titles.append(title)
        saved_data = {
            "id": 99990,
            "scenario_id": self.id,
            "title": title,
            **kwargs,
        }
        return Scenario.model_validate(saved_data)

    monkeypatch.setattr(Session, "save", mock_save)

    saved_2030 = Scenario(id=1001, scenario_id=12345, title="Saved 2030", end_year=2030)
    saved_2050 = Scenario(id=1002, scenario_id=67890, title="Saved 2050", end_year=2050)

    for ss in [saved_2030, saved_2050]:
        ss._scenario_session = Session(id=ss.scenario_id, area_code="nl", end_year=ss.end_year)

    result = Scenario.interpolate([saved_2030, saved_2050], [2040], titles=["Custom Title 2040"])

    assert len(result) == 1
    assert saved_titles[0] == "Custom Title 2040"
    assert result[0].title == "Custom Title 2040"


def test_saved_scenario_interpolate_with_save_kwargs(monkeypatch):
    """Test batch interpolation passes kwargs to save()"""
    interpolated_sessions = [
        Session(id=88882, area_code="nl", end_year=2040, start_year=2023),
    ]

    def mock_interpolate(sessions, end_years, client):
        return interpolated_sessions

    monkeypatch.setattr(Session, "interpolate", mock_interpolate)

    save_calls = []

    def mock_save(self, client, title, **kwargs):
        save_calls.append(kwargs)
        saved_data = {
            "id": 99991,
            "scenario_id": self.id,
            "title": title,
            "private": kwargs.get("private", False),
        }
        return Scenario.model_validate(saved_data)

    monkeypatch.setattr(Session, "save", mock_save)

    saved_2030 = Scenario(id=1001, scenario_id=12345, title="Saved 2030", end_year=2030)
    saved_2050 = Scenario(id=1002, scenario_id=67890, title="Saved 2050", end_year=2050)

    for ss in [saved_2030, saved_2050]:
        ss._scenario_session = Session(id=ss.scenario_id, area_code="nl", end_year=ss.end_year)

    result = Scenario.interpolate(
        [saved_2030, saved_2050],
        [2040],
        private=True,
    )

    assert len(result) == 1
    assert save_calls[0]["private"] is True
    assert result[0].private is True


def test_saved_scenario_interpolate_titles_length_mismatch():
    """Test that mismatched titles length raises ValueError"""
    saved_2030 = Scenario(id=1001, scenario_id=12345, title="Saved 2030", end_year=2030)
    saved_2050 = Scenario(id=1002, scenario_id=67890, title="Saved 2050", end_year=2050)

    with pytest.raises(ValueError, match="Length of titles .* must match"):
        Scenario.interpolate(
            [saved_2030, saved_2050],
            2040,
            2060,  # 2 target years
            titles=["Only One Title"],  # But only 1 title
        )


def test_saved_scenario_interpolate_rejects_duplicate_end_years():
    """Test that interpolate raises ValueError when scenarios have duplicate end_years"""
    saved_2040_a = Scenario(id=1001, scenario_id=12345, title="Saved 2040 A", end_year=2040)
    saved_2040_b = Scenario(id=1002, scenario_id=67890, title="Saved 2040 B", end_year=2040)
    saved_2050 = Scenario(id=1003, scenario_id=11111, title="Saved 2050", end_year=2050)

    # Mock session property with matching end_years
    for ss in [saved_2040_a, saved_2040_b, saved_2050]:
        ss._scenario_session = Session(id=ss.scenario_id, area_code="nl", end_year=ss.end_year)

    with pytest.raises(ValueError, match="Sessions must have unique end_year values"):
        Scenario.interpolate([saved_2040_a, saved_2040_b, saved_2050], 2045)


def test_saved_scenario_interpolate_rejects_different_area_codes():
    """Test that interpolate raises ValueError when scenarios have different area_codes"""
    saved_nl = Scenario(id=1001, scenario_id=12345, title="Saved NL", end_year=2030)
    saved_de = Scenario(id=1002, scenario_id=67890, title="Saved DE", end_year=2050)

    # Mock session property with different area codes
    saved_nl._scenario_session = Session(
        id=saved_nl.scenario_id, area_code="nl", end_year=saved_nl.end_year
    )
    saved_de._scenario_session = Session(
        id=saved_de.scenario_id, area_code="de", end_year=saved_de.end_year
    )

    with pytest.raises(ValueError, match="All sessions must have the same area_code"):
        Scenario.interpolate([saved_nl, saved_de], 2040)


# --- Identifier Resolution Tests --- #


def test_identifier_prioritizes_saved_title():
    """Test identifier returns saved scenario title when available."""
    saved = Scenario(id=1001, scenario_id=12345, title="Saved Title")
    # Mock session with short_name, title, and id
    saved._scenario_session = Session(
        id=12345, area_code="nl", end_year=2050, title="Session Title", short_name="short"
    )

    assert saved.identifier() == "Saved Title"


def test_identifier_falls_back_to_short_name():
    """Test identifier returns short_name when saved title is not available."""
    saved = Scenario(id=1001, scenario_id=12345, title="")
    # Mock session with short_name
    saved._scenario_session = Session(
        id=12345, area_code="nl", end_year=2050, title="Session Title", short_name="short"
    )

    assert saved.identifier() == "short"


def test_identifier_falls_back_to_session_title():
    """Test identifier returns session title when saved title and short_name not available."""
    saved = Scenario(id=1001, scenario_id=12345, title="")
    # Mock session with only title
    saved._scenario_session = Session(
        id=12345, area_code="nl", end_year=2050, title="Session Title"
    )

    assert saved.identifier() == "Session Title"


def test_identifier_falls_back_to_saved_id():
    """Test identifier returns saved scenario id when saved title, short_name, and session title not available."""
    saved = Scenario(id=1001, scenario_id=12345, title="")
    # Mock session with no title or short_name
    saved._scenario_session = Session(id=12345, area_code="nl", end_year=2050)

    assert saved.identifier() == 1001


def test_identifier_falls_back_to_session_id():
    """Test identifier returns session id as final fallback."""
    saved = Scenario(id=None, scenario_id=12345, title="")
    # Mock session with no title or short_name
    saved._scenario_session = Session(id=12345, area_code="nl", end_year=2050)

    assert saved.identifier() == 12345


def test_identifier_resolution_order_complete():
    """Test complete identifier resolution order with all properties present."""
    # Test 1: When all properties are present, saved title should win
    saved1 = Scenario(id=1001, scenario_id=12345, title="Saved Title")
    saved1._scenario_session = Session(
        id=12345, area_code="nl", end_year=2050, title="Session Title", short_name="short"
    )
    assert saved1.identifier() == "Saved Title"

    # Test 2: Without saved title, should return short_name
    saved2 = Scenario(id=1001, scenario_id=12345, title="")
    saved2._scenario_session = Session(
        id=12345, area_code="nl", end_year=2050, title="Session Title", short_name="short"
    )
    assert saved2.identifier() == "short"

    # Test 3: Without saved title and short_name, should return session title
    saved3 = Scenario(id=1001, scenario_id=12345, title="")
    saved3._scenario_session = Session(
        id=12345, area_code="nl", end_year=2050, title="Session Title"
    )
    assert saved3.identifier() == "Session Title"

    # Test 4: Without saved title, short_name, and session title, should return saved id
    saved4 = Scenario(id=1001, scenario_id=12345, title="")
    saved4._scenario_session = Session(id=12345, area_code="nl", end_year=2050)
    assert saved4.identifier() == 1001

    # Test 5: Without any identifier except session id, should return session id
    saved5 = Scenario(id=None, scenario_id=12345, title="")
    saved5._scenario_session = Session(id=12345, area_code="nl", end_year=2050)
    assert saved5.identifier() == 12345
