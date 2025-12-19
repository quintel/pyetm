"""Tests for Scenarios and SavedScenarios collection classes with from_excel filtering."""

import pytest
import pandas as pd
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from models.sessions import Sessions
from models.scenarios import Scenarios
from models.session import Session
from models.scenario import Scenario
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


class TestSavedScenariosFromExcel:
    """Test SavedScenarios.from_excel() filtering for SavedScenario instances only."""

    def test_from_excel_filters_saved_scenarios_only(self, monkeypatch):
        """Test that from_excel only returns SavedScenario instances."""
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

        # Should only include SavedScenario instances
        assert len(result.items) == 2
        assert saved1 in result.items
        assert saved2 in result.items
        assert session1 not in result.items

    def test_from_excel_empty_when_no_saved_scenarios(self, monkeypatch):
        """Test that from_excel returns empty collection when only Sessions exist."""
        session1 = Mock(spec=Session)
        session1.id = 100
        session2 = Mock(spec=Session)
        session2.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, session2}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should be empty
        assert len(result.items) == 0

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
        """Test that SavedScenarios are sorted by ID."""
        saved1 = Mock(spec=Scenario)
        saved1.id = 300
        saved2 = Mock(spec=Scenario)
        saved2.id = 100
        saved3 = Mock(spec=Scenario)
        saved3.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {saved1, saved2, saved3}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should be sorted by ID
        assert result.items[0].id == 100
        assert result.items[1].id == 200
        assert result.items[2].id == 300


class TestSavedScenariosSessionsProperty:
    """Test SavedScenarios.sessions property for accessing underlying Scenario objects."""

    def test_sessions_property_returns_list_of_scenarios(self):
        """Test that sessions property returns list of underlying Scenario objects."""
        # Create mock SavedScenarios with mock sessions
        scenario1 = Mock(spec=Session)
        scenario1.id = 100
        scenario2 = Mock(spec=Session)
        scenario2.id = 200

        saved1 = Mock(spec=Scenario)
        saved1.id = 1
        saved1.session = scenario1

        saved2 = Mock(spec=Scenario)
        saved2.id = 2
        saved2.session = scenario2

        collection = Scenarios(items=[saved1, saved2])

        # Access sessions property
        sessions = collection.sessions

        # Should return list of Scenario objects
        assert isinstance(sessions, list)
        assert len(sessions) == 2
        assert sessions[0] is scenario1
        assert sessions[1] is scenario2

    def test_sessions_property_empty_collection(self):
        """Test that sessions property returns empty list for empty collection."""
        collection = Scenarios(items=[])
        sessions = collection.sessions

        assert isinstance(sessions, list)
        assert len(sessions) == 0

    def test_sessions_property_single_saved_scenario(self):
        """Test sessions property with single SavedScenario."""
        scenario = Mock(spec=Session)
        scenario.id = 100

        saved = Mock(spec=Scenario)
        saved.id = 1
        saved.session = scenario

        collection = Scenarios(items=[saved])
        sessions = collection.sessions

        assert len(sessions) == 1
        assert sessions[0] is scenario


class TestMixedScenariosSeparation:
    """Test that mixed Excel files correctly separate Sessions and SavedScenarios."""

    def test_mixed_excel_separates_correctly(self, monkeypatch):
        """Test that same Excel file returns different results for Scenarios vs SavedScenarios."""
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
            # Load as Scenarios (Sessions only)
            sessions_result = Sessions.from_excel("test.xlsx")

            # Load as SavedScenarios (SavedScenarios only)
            saved_result = Scenarios.from_excel("test.xlsx")

        # Verify Sessions collection
        assert len(sessions_result.items) == 2
        assert session1 in sessions_result.items
        assert session2 in sessions_result.items
        assert saved1 not in sessions_result.items
        assert saved2 not in sessions_result.items

        # Verify SavedScenarios collection
        assert len(saved_result.items) == 2
        assert saved1 in saved_result.items
        assert saved2 in saved_result.items
        assert session1 not in saved_result.items
        assert session2 not in saved_result.items

    def test_no_overlap_between_collections(self, monkeypatch):
        """Test that there's no overlap between the two collections."""
        session1 = Mock(spec=Session)
        session1.id = 100
        saved1 = Mock(spec=Scenario)
        saved1.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {session1, saved1}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            sessions_result = Sessions.from_excel("test.xlsx")
            saved_result = Scenarios.from_excel("test.xlsx")

        # Get all items from both collections
        all_sessions = set(sessions_result.items)
        all_saved = set(saved_result.items)

        # Verify no overlap
        assert len(all_sessions & all_saved) == 0
        assert len(all_sessions) + len(all_saved) == 2
