import pytest
import pandas as pd
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from pyetm.models.sessions import Sessions
from pyetm.models.scenarios import Scenarios
from pyetm.models.session import Session
from pyetm.models.scenario import Scenario
from pyetm.models.scenario_packer import ScenarioPacker


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


class TestScenariosFromExcelMixed:
    """Test Scenarios.from_excel() with mixed Scenario and Session instances."""

    def test_from_excel_includes_all_types(self, monkeypatch):
        """Test that from_excel includes both SavedScenario and Session instances."""
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

        # Should include all types
        assert len(result.items) == 3
        assert saved1 in result.items
        assert saved2 in result.items
        assert session1 in result.items

    def test_from_excel_all_sessions(self, monkeypatch):
        """Test that from_excel returns all Sessions when only Sessions exist."""
        session1 = Mock(spec=Session)
        session1.id = 100
        session2 = Mock(spec=Session)
        session2.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, session2}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should include all sessions
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
        """Test that mixed scenarios are sorted by ID."""
        saved1 = Mock(spec=Scenario)
        saved1.id = 300
        session1 = Mock(spec=Session)
        session1.id = 100
        saved2 = Mock(spec=Scenario)
        saved2.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {saved1, session1, saved2}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should be sorted by ID
        assert result.items[0].id == 100
        assert result.items[1].id == 200
        assert result.items[2].id == 300


class TestScenariosSessionsProperty:
    """Test Scenarios.sessions property for accessing underlying Session objects."""

    def test_sessions_property_returns_list_of_sessions(self):
        """Test that sessions property returns list of underlying Session objects."""
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

    def test_sessions_property_mixed_types(self):
        """Test that sessions property handles mixed Scenario and Session instances."""
        # Create mock instances
        session1 = Mock(spec=Session)
        session1.id = 100

        session2 = Mock(spec=Session)
        session2.id = 200

        saved_session = Mock(spec=Session)
        saved_session.id = 300

        saved = Mock(spec=Scenario)
        saved.id = 3
        saved.session = saved_session

        collection = Scenarios(items=[session1, saved, session2])

        # Access sessions property
        sessions = collection.sessions

        # Should return all Sessions, unwrapping Scenario instances
        assert isinstance(sessions, list)
        assert len(sessions) == 3
        assert sessions[0] is session1
        assert sessions[1] is saved_session  # Unwrapped from Scenario
        assert sessions[2] is session2

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

    def test_sessions_property_only_sessions(self):
        """Test sessions property with only Session instances."""
        session1 = Mock(spec=Session)
        session1.id = 100
        session2 = Mock(spec=Session)
        session2.id = 200

        collection = Scenarios(items=[session1, session2])
        sessions = collection.sessions

        assert len(sessions) == 2
        assert sessions[0] is session1
        assert sessions[1] is session2


class TestMixedScenariosHandling:
    """Test that Scenarios handles mixed types while Sessions filters."""

    def test_scenarios_includes_all_sessions_filters(self, monkeypatch):
        """Test that Scenarios includes all while Sessions filters to Sessions only."""
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
            # Load as Sessions (Sessions only)
            sessions_result = Sessions.from_excel("test.xlsx")

            # Load as Scenarios (all types)
            scenarios_result = Scenarios.from_excel("test.xlsx")

        # Verify Sessions collection - only Session instances
        assert len(sessions_result.items) == 2
        assert session1 in sessions_result.items
        assert session2 in sessions_result.items
        assert saved1 not in sessions_result.items
        assert saved2 not in sessions_result.items

        # Verify Scenarios collection - includes all types
        assert len(scenarios_result.items) == 4
        assert saved1 in scenarios_result.items
        assert saved2 in scenarios_result.items
        assert session1 in scenarios_result.items
        assert session2 in scenarios_result.items

    def test_scenarios_is_superset_of_sessions(self, monkeypatch):
        """Test that Scenarios contains everything Sessions contains (and more)."""
        session1 = Mock(spec=Session)
        session1.id = 100
        saved1 = Mock(spec=Scenario)
        saved1.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, saved1}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            sessions_result = Sessions.from_excel("test.xlsx")
            scenarios_result = Scenarios.from_excel("test.xlsx")

        # Get all items from both collections
        all_sessions = set(sessions_result.items)
        all_scenarios = set(scenarios_result.items)

        # Verify Sessions is a subset of Scenarios
        assert all_sessions.issubset(all_scenarios)
        assert len(all_scenarios) == 2
        assert len(all_sessions) == 1
