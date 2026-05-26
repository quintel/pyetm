"""
Integration tests for Scenarios.create_many() with user_values, custom_curves, and sortables parameters.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, call, ANY
from pyetm.models.scenarios import Scenarios, ScenarioCreationParams
from pyetm.models.scenario import Scenario
from pyetm.models.session import Session
from pyetm.clients import BaseClient


class TestCreateManyWithUserValues:
    """Test create_many() with user_values parameter."""

    @patch("pyetm.models.scenarios.Scenarios._apply_data_concurrently")
    @patch("pyetm.models.scenario.Scenario.create")
    def test_create_many_with_inputs(self, mock_create, mock_apply_data):
        """Test that user_values are passed to concurrent data application."""
        # Mock Scenario.create to return mock saved scenarios
        mock_saved_1 = Mock(spec=Scenario)
        mock_saved_1.id = 10
        mock_saved_1.session = Mock(id=1)
        mock_saved_1.warnings = Mock(__len__=Mock(return_value=0))
        mock_saved_2 = Mock(spec=Scenario)
        mock_saved_2.id = 20
        mock_saved_2.session = Mock(id=2)
        mock_saved_2.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.side_effect = [mock_saved_1, mock_saved_2]

        # Mock data application to return no warnings
        mock_apply_data.return_value = []

        # Create scenarios collection
        scenarios = Scenarios(client=Mock())

        # Call create_many with user_values
        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario 1",
                "area_code": "nl",
                "end_year": 2050,
                "user_values": {"capacity_of_solar_pv": 1000},
            },
            {
                "title": "Scenario 2",
                "area_code": "nl",
                "end_year": 2050,
                "user_values": {"capacity_of_solar_pv": 2000},
            },
        ]

        result = scenarios.create_many(params)

        # Verify scenarios were created
        assert len(result.items) == 2

        # Verify concurrent data application was called
        mock_apply_data.assert_called_once()
        call_args = mock_apply_data.call_args[0]

        # Check that data_to_apply has the right structure
        data_to_apply = call_args[1]
        assert len(data_to_apply) == 2
        # data_to_apply is list of (index, data_dict) tuples
        assert data_to_apply[0][0] == 0  # index
        assert data_to_apply[0][1]["user_values"] == {"capacity_of_solar_pv": 1000}
        assert data_to_apply[1][0] == 1  # index
        assert data_to_apply[1][1]["user_values"] == {"capacity_of_solar_pv": 2000}

    @patch("pyetm.models.scenarios.Scenarios._apply_data_concurrently")
    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_with_mixed_inputs(
        self, mock_session_new, mock_create, mock_apply_data
    ):
        """Test create_many where only some scenarios have user_values."""
        mock_session_1 = Mock()
        mock_session_1.id = 1
        mock_session_2 = Mock()
        mock_session_2.id = 2
        mock_session_3 = Mock()
        mock_session_3.id = 3
        mock_session_new.side_effect = [mock_session_1, mock_session_2, mock_session_3]

        mock_saved_1 = Mock(spec=Scenario)
        mock_saved_1.session = mock_session_1
        mock_saved_1.warnings = Mock(__len__=Mock(return_value=0))
        mock_saved_2 = Mock(spec=Scenario)
        mock_saved_2.session = mock_session_2
        mock_saved_2.warnings = Mock(__len__=Mock(return_value=0))
        mock_saved_3 = Mock(spec=Scenario)
        mock_saved_3.session = mock_session_3
        mock_saved_3.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.side_effect = [mock_saved_1, mock_saved_2, mock_saved_3]

        mock_apply_data.return_value = []

        scenarios = Scenarios(client=Mock())

        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario 1",
                "area_code": "nl",
                "end_year": 2050,
                "user_values": {"capacity_of_solar_pv": 1000},
            },
            {
                "title": "Scenario 2",
                "area_code": "nl",
                "end_year": 2050,
                # No user_values for this one
            },
            {
                "title": "Scenario 3",
                "area_code": "nl",
                "end_year": 2050,
                "user_values": {"capacity_of_solar_pv": 3000},
            },
        ]

        result = scenarios.create_many(params)

        assert len(result.items) == 3

        # Verify data application includes only scenarios with user_values
        call_args = mock_apply_data.call_args[0]
        data_to_apply = call_args[1]
        # Only 2 scenarios have data to apply
        assert len(data_to_apply) == 2
        assert data_to_apply[0][0] == 0  # First scenario
        assert data_to_apply[0][1]["user_values"] == {"capacity_of_solar_pv": 1000}
        assert data_to_apply[1][0] == 2  # Third scenario (index 2)
        assert data_to_apply[1][1]["user_values"] == {"capacity_of_solar_pv": 3000}


class TestCreateManyWithCustomCurves:
    """Test create_many() with custom_curves parameter."""

    @patch("pyetm.models.scenarios.Scenarios._apply_data_concurrently")
    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_with_custom_curves(
        self, mock_session_new, mock_create, mock_apply_data
    ):
        """Test that custom curves are passed to concurrent data application."""
        mock_session_1 = Mock()
        mock_session_1.id = 1
        mock_session_2 = Mock()
        mock_session_2.id = 2
        mock_session_new.side_effect = [mock_session_1, mock_session_2]

        mock_saved_1 = Mock(spec=Scenario)
        mock_saved_1.session = mock_session_1
        mock_saved_1.warnings = Mock(__len__=Mock(return_value=0))
        mock_saved_2 = Mock(spec=Scenario)
        mock_saved_2.session = mock_session_2
        mock_saved_2.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.side_effect = [mock_saved_1, mock_saved_2]

        mock_apply_data.return_value = []

        scenarios = Scenarios(client=Mock())

        # Create sample curve data
        curve1 = pd.Series([1, 2, 3] * 2920, name="solar_profile")
        curve2 = pd.Series([4, 5, 6] * 2920, name="solar_profile")

        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario 1",
                "area_code": "nl",
                "end_year": 2050,
                "custom_curves": {"solar_profile": curve1},
            },
            {
                "title": "Scenario 2",
                "area_code": "nl",
                "end_year": 2050,
                "custom_curves": {"solar_profile": curve2},
            },
        ]

        result = scenarios.create_many(params)

        assert len(result.items) == 2

        # Verify concurrent data application was called with curves
        call_args = mock_apply_data.call_args[0]
        data_to_apply = call_args[1]
        assert len(data_to_apply) == 2
        assert "custom_curves" in data_to_apply[0][1]
        assert "custom_curves" in data_to_apply[1][1]

    @patch("pyetm.models.scenarios.Scenarios._apply_data_concurrently")
    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_with_multiple_curves_per_scenario(
        self, mock_session_new, mock_create, mock_apply_data
    ):
        """Test create_many with multiple custom curves per scenario."""
        mock_session = Mock()
        mock_session.id = 1
        mock_session_new.return_value = mock_session

        mock_saved = Mock(spec=Scenario)
        mock_saved.session = mock_session
        mock_saved.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.return_value = mock_saved

        mock_apply_data.return_value = []

        scenarios = Scenarios(client=Mock())

        curve1 = pd.Series([1] * 8760, name="solar_profile")
        curve2 = pd.Series([2] * 8760, name="wind_profile")

        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario 1",
                "area_code": "nl",
                "end_year": 2050,
                "custom_curves": {
                    "solar_profile": curve1,
                    "wind_profile": curve2,
                },
            },
        ]

        result = scenarios.create_many(params)

        assert len(result.items) == 1

        call_args = mock_apply_data.call_args[0]
        data_to_apply = call_args[1]
        assert "solar_profile" in data_to_apply[0][1]["custom_curves"]
        assert "wind_profile" in data_to_apply[0][1]["custom_curves"]


class TestCreateManyWithSortables:
    """Test create_many() with sortables parameter."""

    @patch("pyetm.models.scenarios.Scenarios._apply_data_concurrently")
    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_with_sortables(
        self, mock_session_new, mock_create, mock_apply_data
    ):
        """Test that sortables are passed to concurrent data application."""
        mock_session_1 = Mock()
        mock_session_1.id = 1
        mock_session_2 = Mock()
        mock_session_2.id = 2
        mock_session_new.side_effect = [mock_session_1, mock_session_2]

        mock_saved_1 = Mock(spec=Scenario)
        mock_saved_1.session = mock_session_1
        mock_saved_1.warnings = Mock(__len__=Mock(return_value=0))
        mock_saved_2 = Mock(spec=Scenario)
        mock_saved_2.session = mock_session_2
        mock_saved_2.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.side_effect = [mock_saved_1, mock_saved_2]

        mock_apply_data.return_value = []

        scenarios = Scenarios(client=Mock())

        sortables1 = {
            "forecast_storage": ["item1", "item2"],
            "hydrogen_supply": ["h1", "h2"],
        }
        sortables2 = {
            "forecast_storage": ["item2", "item1"],
            "hydrogen_supply": ["h2", "h1"],
        }

        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario 1",
                "area_code": "nl",
                "end_year": 2050,
                "sortables": sortables1,
            },
            {
                "title": "Scenario 2",
                "area_code": "nl",
                "end_year": 2050,
                "sortables": sortables2,
            },
        ]

        result = scenarios.create_many(params)

        assert len(result.items) == 2

        call_args = mock_apply_data.call_args[0]
        data_to_apply = call_args[1]
        assert data_to_apply[0][1]["sortables"] == sortables1
        assert data_to_apply[1][1]["sortables"] == sortables2


class TestCreateManyCombined:
    """Test create_many() with multiple parameter types combined."""

    @patch("pyetm.models.scenarios.Scenarios._apply_data_concurrently")
    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_with_all_parameters(
        self, mock_session_new, mock_create, mock_apply_data
    ):
        """Test create_many with user_values, curves, and sortables combined."""
        mock_session = Mock()
        mock_session.id = 1
        mock_session_new.return_value = mock_session

        mock_saved = Mock(spec=Scenario)
        mock_saved.session = mock_session
        mock_saved.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.return_value = mock_saved

        mock_apply_data.return_value = []

        scenarios = Scenarios(client=Mock())

        curve = pd.Series([1] * 8760, name="solar_profile")
        sortables = {"forecast_storage": ["item1"]}

        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario 1",
                "area_code": "nl",
                "end_year": 2050,
                "user_values": {"capacity_of_solar_pv": 1000},
                "custom_curves": {"solar_profile": curve},
                "sortables": sortables,
            },
        ]

        result = scenarios.create_many(params)

        assert len(result.items) == 1

        call_args = mock_apply_data.call_args[0]
        data_to_apply = call_args[1]
        data = data_to_apply[0][1]
        assert "user_values" in data
        assert "custom_curves" in data
        assert "sortables" in data


class TestCreateManyErrorHandling:
    """Test error handling in create_many()."""

    @patch("pyetm.models.scenarios.Scenarios._apply_data_concurrently")
    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_data_warnings_populated(
        self, mock_session_new, mock_create, mock_apply_data
    ):
        """Test that data_warnings attribute is populated when data application fails."""
        mock_session_1 = Mock()
        mock_session_1.id = 1
        mock_session_2 = Mock()
        mock_session_2.id = 2
        mock_session_new.side_effect = [mock_session_1, mock_session_2]

        mock_saved_1 = Mock(spec=Scenario)
        mock_saved_1.session = mock_session_1
        mock_saved_1.warnings = Mock(__len__=Mock(return_value=0))
        mock_saved_2 = Mock(spec=Scenario)
        mock_saved_2.session = mock_session_2
        mock_saved_2.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.side_effect = [mock_saved_1, mock_saved_2]

        # Mock data application to return warnings
        mock_apply_data.return_value = [
            "Failed to apply user_values to scenario 1: Invalid input",
            "Failed to upload curve to scenario 2: Invalid curve",
        ]

        scenarios = Scenarios(client=Mock())

        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario 1",
                "area_code": "nl",
                "end_year": 2050,
                "user_values": {"invalid_input": 999},
            },
            {
                "title": "Scenario 2",
                "area_code": "nl",
                "end_year": 2050,
                "custom_curves": {"invalid_curve": pd.Series([1, 2, 3])},
            },
        ]

        result = scenarios.create_many(params)

        # Scenarios should still be created
        assert len(result.items) == 2

        # Warnings should be stored
        assert len(result.data_warnings) == 2
        assert "Invalid input" in result.data_warnings[0]
        assert "Invalid curve" in result.data_warnings[1]

    @patch("pyetm.models.scenarios.Scenarios._apply_data_concurrently")
    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_raise_on_data_errors(
        self, mock_session_new, mock_create, mock_apply_data
    ):
        """Test that raise_on_data_errors=True raises exception on data failure."""
        mock_session = Mock()
        mock_session.id = 1
        mock_session_new.return_value = mock_session

        mock_saved = Mock(spec=Scenario)
        mock_saved.session = mock_session
        mock_saved.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.return_value = mock_saved

        # Mock data application to return errors
        mock_apply_data.return_value = ["Failed to apply data"]

        scenarios = Scenarios(client=Mock())

        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario 1",
                "area_code": "nl",
                "end_year": 2050,
                "user_values": {"invalid_input": 999},
            },
        ]

        # Should raise when raise_on_data_errors=True
        with pytest.raises(Exception) as exc_info:
            scenarios.create_many(params, raise_on_data_errors=True)

        # Check that the error message mentions the failure
        assert "failed to apply data" in str(exc_info.value).lower()

    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_partial_scenario_creation_failure(
        self, mock_session_new, mock_create
    ):
        """Test behavior when some scenario creations fail."""
        mock_session_1 = Mock()
        mock_session_1.id = 1
        mock_session_3 = Mock()
        mock_session_3.id = 3

        # First succeeds, second fails, third succeeds
        mock_session_new.side_effect = [
            mock_session_1,
            Exception("Creation failed"),
            mock_session_3,
        ]

        mock_saved_1 = Mock(spec=Scenario)
        mock_saved_1.session = mock_session_1
        mock_saved_1.warnings = Mock(__len__=Mock(return_value=0))
        mock_saved_3 = Mock(spec=Scenario)
        mock_saved_3.session = mock_session_3
        mock_saved_3.warnings = Mock(__len__=Mock(return_value=0))

        mock_create.side_effect = [mock_saved_1, mock_saved_3]

        scenarios = Scenarios(client=Mock())

        params: list[ScenarioCreationParams] = [
            {"title": "Scenario 1", "area_code": "nl", "end_year": 2050},
            {"title": "Scenario 2", "area_code": "nl", "end_year": 2050},
            {"title": "Scenario 3", "area_code": "nl", "end_year": 2050},
        ]

        # Should handle the exception gracefully
        result = scenarios.create_many(params)

        # Should only have the successful scenarios
        assert len(result.items) == 2


class TestCreateManyWithTemplateId:
    """Test create_many() with template_id parameter."""

    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_with_template_id_only(self, mock_session_new, mock_create):
        """Test that template_id allows creating scenarios without area_code and end_year."""
        # Mock Session.new to return mock session
        mock_session = Mock()
        mock_session.id = 1
        mock_session_new.return_value = mock_session

        # Mock Scenario.from_scenario to return mock saved scenario
        mock_saved = Mock(spec=Scenario)
        mock_saved.id = 10
        mock_saved.session = mock_session
        mock_saved.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.return_value = mock_saved

        scenarios = Scenarios(client=Mock())

        # Create scenario with only template_id (no area_code or end_year)
        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario from template",
                "template_id": 100000,
            },
        ]

        result = scenarios.create_many(params)

        # Verify scenario was created successfully
        assert len(result.items) == 1

        # Verify Session.new was called with None for area_code and end_year
        # and template_id parameter passed through
        mock_session_new.assert_called_once_with(None, None, client=ANY, template_id=100000)

    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_with_template_id_and_explicit_area_year(
        self, mock_session_new, mock_create
    ):
        """Test that template_id works with explicit area_code and end_year too."""
        mock_session = Mock()
        mock_session.id = 1
        mock_session_new.return_value = mock_session

        mock_saved = Mock(spec=Scenario)
        mock_saved.session = mock_session
        mock_saved.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.return_value = mock_saved

        scenarios = Scenarios(client=Mock())

        # Create scenario with template_id AND explicit area_code/end_year
        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario with template and overrides",
                "template_id": 100000,
                "area_code": "de",
                "end_year": 2040,
            },
        ]

        result = scenarios.create_many(params)

        assert len(result.items) == 1

        # Verify Session.new was called with explicit values
        mock_session_new.assert_called_once_with("de", 2040, client=ANY, template_id=100000)

    @patch("pyetm.models.scenario.Scenario.create")
    @patch("pyetm.models.session.Session.new")
    def test_create_many_with_default_area_year_and_template(
        self, mock_session_new, mock_create
    ):
        """Test that default area_code/end_year are used when template_id is present."""
        mock_session = Mock()
        mock_session.id = 1
        mock_session_new.return_value = mock_session

        mock_saved = Mock(spec=Scenario)
        mock_saved.session = mock_session
        mock_saved.warnings = Mock(__len__=Mock(return_value=0))
        mock_create.return_value = mock_saved

        scenarios = Scenarios(client=Mock())

        # Create scenario with template_id and default area_code/end_year
        params: list[ScenarioCreationParams] = [
            {
                "title": "Scenario with defaults and template",
                "template_id": 100000,
            },
        ]

        result = scenarios.create_many(params, area_code="nl", end_year=2050)

        assert len(result.items) == 1

        # Verify Session.new was called with defaults (not None)
        mock_session_new.assert_called_once_with("nl", 2050, client=ANY, template_id=100000)
