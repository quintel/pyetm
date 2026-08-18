from unittest.mock import Mock, patch

import pandas as pd

from pyetm.models.scenario import Scenario
from pyetm.models.scenario_packer import ScenarioPacker
from pyetm.models.scenarios import Scenarios
from pyetm.models.session import Session
from pyetm.models.sessions import Sessions


class TestScenariosFromExcel:
    """Test Scenarios.from_excel() filtering for Session instances only."""

    def test_from_excel_filters_sessions_only(self, monkeypatch):
        """Test that from_excel only returns Scenario instances (not SavedScenario)."""
        # Create mock scenarios
        session1 = Mock(spec=Session)
        session1.id = 100
        session2 = Mock(spec=Session)
        session2.id = 200
        saved1 = Mock(spec=Scenario)
        saved1.id = 300

        # Mock ScenarioPacker to return mixed scenarios
        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, session2, saved1}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Sessions.from_excel("test.xlsx")

        # Should only include Scenario instances, not SavedScenario
        assert len(result.items) == 2
        assert session1 in result.items
        assert session2 in result.items
        assert saved1 not in result.items

    def test_from_excel_empty_when_no_sessions(self, monkeypatch):
        """Test that from_excel returns empty collection when only SavedScenarios exist."""
        saved1 = Mock(spec=Scenario)
        saved1.id = 100
        saved2 = Mock(spec=Scenario)
        saved2.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {saved1, saved2}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Sessions.from_excel("test.xlsx")

        # Should be empty
        assert len(result.items) == 0

    def test_from_excel_all_sessions(self, monkeypatch):
        """Test that from_excel returns all sessions when no SavedScenarios exist."""
        session1 = Mock(spec=Session)
        session1.id = 100
        session2 = Mock(spec=Session)
        session2.id = 200
        session3 = Mock(spec=Session)
        session3.id = 300

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, session2, session3}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Sessions.from_excel("test.xlsx")

        # Should include all sessions
        assert len(result.items) == 3
        assert session1 in result.items
        assert session2 in result.items
        assert session3 in result.items

    def test_from_excel_sorts_by_id(self, monkeypatch):
        """Test that scenarios are sorted by ID."""
        session1 = Mock(spec=Session)
        session1.id = 300
        session2 = Mock(spec=Session)
        session2.id = 100
        session3 = Mock(spec=Session)
        session3.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, session2, session3}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Sessions.from_excel("test.xlsx")

        # Should be sorted by ID
        assert result.items[0].id == 100
        assert result.items[1].id == 200
        assert result.items[2].id == 300


class TestSavedScenariosFromExcel:
    """Test Scenarios.from_excel() support for mixed SavedScenario and Session collections."""

    def test_from_excel_loads_both_types(self, monkeypatch):
        """Test that from_excel returns both SavedScenario and Session instances."""
        session1 = Mock(spec=Session)
        session1.id = 100
        saved1 = Mock(spec=Scenario)
        saved1.id = 200
        saved2 = Mock(spec=Scenario)
        saved2.id = 300

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, saved1, saved2}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should include both SavedScenario and Session instances
        assert len(result.items) == 3
        assert saved1 in result.items
        assert saved2 in result.items
        assert session1 in result.items
        assert session1 in result.items

    def test_from_excel_loads_sessions_only(self, monkeypatch):
        """Test that from_excel returns Sessions when only Sessions exist."""
        session1 = Mock(spec=Session)
        session1.id = 100
        session2 = Mock(spec=Session)
        session2.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, session2}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should include all Sessions
        assert len(result.items) == 2
        assert session1 in result.items
        assert session2 in result.items

    def test_from_excel_all_saved_scenarios(self, monkeypatch):
        """Test that from_excel returns all SavedScenarios when no Sessions exist."""
        saved1 = Mock(spec=Scenario)
        saved1.id = 100
        saved2 = Mock(spec=Scenario)
        saved2.id = 200
        saved3 = Mock(spec=Scenario)
        saved3.id = 300

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {saved1, saved2, saved3}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should include all SavedScenarios
        assert len(result.items) == 3
        assert saved1 in result.items
        assert saved2 in result.items
        assert saved3 in result.items

    def test_from_excel_sorts_by_id(self, monkeypatch):
        """Test that scenarios are sorted by ID regardless of type."""
        saved1 = Mock(spec=Scenario)
        saved1.id = 300
        session1 = Mock(spec=Session)
        session1.id = 100
        session1 = Mock(spec=Session)
        session1.id = 100
        saved2 = Mock(spec=Scenario)
        saved2.id = 200
        saved2.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {saved1, session1, saved2}
        mock_packer._scenarios.return_value = {saved1, session1, saved2}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should be sorted by ID
        assert result.items[0].id == 100
        assert result.items[1].id == 200
        assert result.items[2].id == 300


class TestSavedScenariosSessionsProperty:
    """Test Scenarios.sessions property for accessing underlying Session objects."""

    def test_sessions_property_returns_list_of_scenarios(self):
        """Test that sessions property returns list of underlying Session objects from SavedScenarios."""
        # Create mock SavedScenarios with mock sessions
        session1 = Mock(spec=Session)
        session1.id = 100
        session2 = Mock(spec=Session)
        session2.id = 200

        saved1 = Mock(spec=Scenario)
        saved1.id = 1
        saved1.session = session1

        saved2 = Mock(spec=Scenario)
        saved2.id = 2
        saved2.session = session2

        collection = Scenarios(items=[saved1, saved2])

        # Access sessions property
        sessions = collection.sessions

        # Should return list of Session objects
        assert isinstance(sessions, list)
        assert len(sessions) == 2
        assert sessions[0] is session1
        assert sessions[1] is session2

    def test_sessions_property_with_mixed_collection(self):
        """Test that sessions property handles mixed SavedScenario and Session objects."""
        # Create a Session object (direct ETEngine scenario)
        session1 = Mock(spec=Session)
        session1.id = 100

        # Create a SavedScenario with underlying session
        scenario2 = Mock(spec=Session)
        scenario2.id = 200
        saved1 = Mock(spec=Scenario)
        saved1.id = 1
        saved1.session = scenario2

        # Create mixed collection
        collection = Scenarios(items=[session1, saved1])

        # Access sessions property
        sessions = collection.sessions

        # Should return list with Session directly and session from SavedScenario
        assert isinstance(sessions, list)
        assert len(sessions) == 2
        assert sessions[0] is session1  # Direct Session
        assert sessions[1] is scenario2  # Session from SavedScenario

    def test_sessions_property_empty_collection(self):
        """Test that sessions property returns empty list for empty collection."""
        collection = Scenarios(items=[])
        sessions = collection.sessions

        assert isinstance(sessions, list)
        assert len(sessions) == 0

    def test_sessions_property_single_saved_scenario(self):
        """Test sessions property with single SavedScenario."""
        session = Mock(spec=Session)
        session.id = 100

        saved = Mock(spec=Scenario)
        saved.id = 1
        saved.session = session

        collection = Scenarios(items=[saved])
        sessions = collection.sessions

        assert len(sessions) == 1
        assert sessions[0] is session

    def test_sessions_property_single_session(self):
        """Test sessions property with single Session object."""
        session = Mock(spec=Session)
        session.id = 100

        collection = Scenarios(items=[session])
        sessions = collection.sessions

        assert len(sessions) == 1
        assert sessions[0] is session


class TestMixedScenariosSeparation:
    """Test Excel loading behavior for Sessions vs Scenarios collections."""

    def test_mixed_excel_behavior(self, monkeypatch):
        """Test that Sessions.from_excel filters, while Scenarios.from_excel loads all."""
        # Create mixed scenarios
        session1 = Mock(spec=Session)
        session1.id = 100
        session2 = Mock(spec=Session)
        session2.id = 200
        saved1 = Mock(spec=Scenario)
        saved1.id = 300
        saved2 = Mock(spec=Scenario)
        saved2.id = 400

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, session2, saved1, saved2}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            # Load as Sessions (filters to Sessions only)
            sessions_result = Sessions.from_excel("test.xlsx")

            # Load as Scenarios (loads all types)
            scenarios_result = Scenarios.from_excel("test.xlsx")

        # Verify Sessions collection (filtered)
        assert len(sessions_result.items) == 2
        assert session1 in sessions_result.items
        assert session2 in sessions_result.items
        assert saved1 not in sessions_result.items
        assert saved2 not in sessions_result.items

        # Verify Scenarios collection (mixed - loads all)
        assert len(scenarios_result.items) == 4
        assert saved1 in scenarios_result.items
        assert saved2 in scenarios_result.items
        assert session1 in scenarios_result.items
        assert session2 in scenarios_result.items

    def test_sessions_from_excel_filters_only_sessions(self, monkeypatch):
        """Test that Sessions.from_excel only loads Session objects."""
        session1 = Mock(spec=Session)
        session1.id = 100
        saved1 = Mock(spec=Scenario)
        saved1.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, saved1}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            sessions_result = Sessions.from_excel("test.xlsx")

        # Sessions collection should only have Session objects
        assert len(sessions_result.items) == 1
        assert session1 in sessions_result.items
        assert saved1 not in sessions_result.items


class TestScenariosGetMethods:
    """Test Scenarios convenience methods for accessing packer data."""

    def test_get_hourly_output_curves_delegates_to_packer(self):
        """Test that get_hourly_output_curves delegates to the packer."""
        # Create mock scenarios
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1
        scenario1.identifier.return_value = "scenario_1"
        scenario1.get_hourly_curves.return_value = {}

        scenario2 = Mock(spec=Scenario)
        scenario2.id = 2
        scenario2.identifier.return_value = "scenario_2"
        scenario2.get_hourly_curves.return_value = {}

        collection = Scenarios(items=[scenario1, scenario2])

        # Mock the packer
        mock_packer = Mock(spec=ScenarioPacker)
        expected_result = {
            "merit_order": {
                "Scenario 1": pd.DataFrame(),
                "Scenario 2": pd.DataFrame(),
            }
        }
        mock_packer.hourly_output_curves.return_value = expected_result
        collection._packer = mock_packer

        # Call the method
        result = collection.get_hourly_output_curves("electricity")

        # Verify fetch was called on scenarios with new API (list of identifiers)
        scenario1.get_hourly_curves.assert_called_once_with(["electricity"])
        scenario2.get_hourly_curves.assert_called_once_with(["electricity"])

        # Verify delegation to packer
        mock_packer.hourly_output_curves.assert_called_once_with("electricity")
        assert result == expected_result

    def test_get_hourly_output_curves_with_filter(self):
        """Test that get_hourly_output_curves passes carrier type to packer."""
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1

        collection = Scenarios(items=[scenario1])

        # Mock the packer
        mock_packer = Mock(spec=ScenarioPacker)
        expected_result = {
            "merit_order": {
                "Scenario 1": pd.DataFrame(),
            },
            "electricity_price": {
                "Scenario 1": pd.DataFrame(),
            },
            "residual_load": {
                "Scenario 1": pd.DataFrame(),
            },
        }
        mock_packer.hourly_output_curves.return_value = expected_result
        collection._packer = mock_packer

        # Call with carrier type
        result = collection.get_hourly_output_curves("electricity")

        # Verify carrier type was passed
        mock_packer.hourly_output_curves.assert_called_once_with("electricity")
        assert result == expected_result

    def test_get_hourly_output_curves_triggers_fetch(self):
        """Test that get_hourly_output_curves triggers fetch on each scenario if not cached."""
        # Create mock scenarios
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1
        scenario1.identifier.return_value = "scenario_1"
        scenario1.get_hourly_curves.return_value = {
            "merit_order": pd.DataFrame({"hour": [0, 1], "value": [100, 200]}),
            "electricity_price": pd.DataFrame({"hour": [0, 1], "price": [0.12, 0.13]}),
            "residual_load": pd.DataFrame({"hour": [0, 1], "load": [500, 600]}),
        }

        scenario2 = Mock(spec=Scenario)
        scenario2.id = 2
        scenario2.identifier.return_value = "scenario_2"
        scenario2.get_hourly_curves.return_value = {
            "merit_order": pd.DataFrame({"hour": [0, 1], "value": [150, 250]}),
            "electricity_price": pd.DataFrame({"hour": [0, 1], "price": [0.14, 0.15]}),
            "residual_load": pd.DataFrame({"hour": [0, 1], "load": [550, 650]}),
        }

        collection = Scenarios(items=[scenario1, scenario2])

        # Mock the packer to return organized data
        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer.hourly_output_curves.return_value = {
            "merit_order": {
                "scenario_1": pd.DataFrame(),
                "scenario_2": pd.DataFrame(),
            }
        }
        collection._packer = mock_packer

        # Call get_hourly_output_curves
        result = collection.get_hourly_output_curves("electricity")

        # Verify it triggered fetch on each individual scenario with new API
        scenario1.get_hourly_curves.assert_called_once_with(["electricity"])
        scenario2.get_hourly_curves.assert_called_once_with(["electricity"])

        # Verify packer was called
        mock_packer.hourly_output_curves.assert_called_once_with("electricity")

        # Verify result structure
        assert isinstance(result, dict)

    def test_get_annual_exports_delegates_to_packer(self):
        """Test that get_annual_exports delegates to the packer."""
        # Create mock scenarios
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1
        scenario2 = Mock(spec=Scenario)
        scenario2.id = 2

        collection = Scenarios(items=[scenario1, scenario2])

        # Mock the packer
        mock_packer = Mock(spec=ScenarioPacker)
        expected_result = {
            "total_co2_emissions": {
                "Scenario 1": pd.DataFrame(),
                "Scenario 2": pd.DataFrame(),
            }
        }
        mock_packer.annual_exports.return_value = expected_result
        collection._packer = mock_packer

        # Call the method
        result = collection.get_annual_exports()

        # Verify delegation
        mock_packer.annual_exports.assert_called_once_with(None)
        assert result == expected_result

    def test_get_annual_exports_with_filter(self):
        """Test that get_annual_exports passes export filter to packer."""
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1

        collection = Scenarios(items=[scenario1])

        # Mock the packer
        mock_packer = Mock(spec=ScenarioPacker)
        expected_result = {
            "total_co2_emissions": {
                "Scenario 1": pd.DataFrame(),
            }
        }
        mock_packer.annual_exports.return_value = expected_result
        collection._packer = mock_packer

        # Call with specific export
        result = collection.get_annual_exports("total_co2_emissions")

        # Verify filter was passed
        mock_packer.annual_exports.assert_called_once_with("total_co2_emissions")
        assert result == expected_result

    def test_get_methods_work_through_combine_property(self):
        """Test that methods work even when packer is created via combine property."""
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1
        scenario1.identifier.return_value = "scenario_1"
        scenario1.get_hourly_curves.return_value = {}

        collection = Scenarios(items=[scenario1])

        # Don't pre-create packer - let combine property create it
        with patch.object(ScenarioPacker, "hourly_output_curves") as mock_curves:
            expected = {"merit_order": {"Scenario 1": pd.DataFrame()}}
            mock_curves.return_value = expected

            # This should work even though _packer is None
            with patch.object(ScenarioPacker, "add"):
                result = collection.get_hourly_output_curves("electricity")

            # Packer should have been created and method called
            assert collection._packer is not None
            # Verify fetch was triggered with new API
            scenario1.get_hourly_curves.assert_called_once_with(["electricity"])


class TestScenariosFromExcelWarnings:
    """Per-row failures recorded by the loaders must reach the caller."""

    def test_from_excel_lifts_warnings_off_the_scenarios(self):
        session = Session(id=100, area_code="nl2023", end_year=2050)
        session.add_warning("save", "Row 'SESSION_A' was not saved to MyETM: 422")

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = [session]

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        assert len(result.warnings) == 1
        assert "not saved to MyETM" in str(list(result.warnings)[0])

    def test_from_excel_stays_quiet_when_every_row_succeeded(self):
        session = Session(id=100, area_code="nl2023", end_year=2050)

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = [session]

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        assert len(result.warnings) == 0
