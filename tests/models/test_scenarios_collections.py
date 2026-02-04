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


class TestSessionsList:
    """Integration tests for Sessions.list() DataFrame construction."""

    def test_list_returns_dataframe_from_runner(self, monkeypatch):
        """Runner data is turned into a DataFrame with one row per session."""
        from pyetm.services import scenario_runners as runners_pkg
        from pyetm.services.scenario_runners import list_sessions

        sample = [
            {"id": 1, "title": "Base", "area_code": "nl", "end_year": 2050},
            {"id": 2, "title": "Wind", "area_code": "nl", "end_year": 2040},
        ]

        mock_result = Mock()
        mock_result.success = True
        mock_result.data = sample

        monkeypatch.setattr(
            list_sessions.ListSessionsRunner, "run", staticmethod(lambda *a, **kw: mock_result)
        )

        df = Sessions.list(client=Mock())

        assert len(df) == 2
        assert list(df.columns) == ["id", "title", "area_code", "end_year"]
        assert df["id"].tolist() == [1, 2]
        assert df["title"].tolist() == ["Base", "Wind"]

    def test_list_returns_empty_dataframe_on_failure(self, monkeypatch):
        """Failed runner result yields an empty DataFrame."""
        from pyetm.services.scenario_runners import list_sessions

        mock_result = Mock()
        mock_result.success = False
        mock_result.errors = ["401 Unauthorized"]

        monkeypatch.setattr(
            list_sessions.ListSessionsRunner, "run", staticmethod(lambda *a, **kw: mock_result)
        )

        df = Sessions.list(client=Mock())

        assert df.empty

    def test_list_empty_data_yields_empty_dataframe(self, monkeypatch):
        """An empty data array from the API produces an empty DataFrame."""
        from pyetm.services.scenario_runners import list_sessions

        mock_result = Mock()
        mock_result.success = True
        mock_result.data = []

        monkeypatch.setattr(
            list_sessions.ListSessionsRunner, "run", staticmethod(lambda *a, **kw: mock_result)
        )

        df = Sessions.list(client=Mock())

        assert df.empty


class TestScenariosList:
    """Integration tests for Scenarios.list() DataFrame construction and flattening."""

    def test_list_flattens_nested_scenario_key(self, monkeypatch):
        """The nested 'scenario' dict is flattened into session_* columns."""
        from pyetm.services.scenario_runners import list_saved_scenarios

        sample = [
            {
                "id": 10,
                "scenario_id": 1,
                "title": "Saved base",
                "scenario": {"id": 1, "area_code": "nl", "end_year": 2050},
            },
            {
                "id": 11,
                "scenario_id": 2,
                "title": "Saved wind",
                "scenario": {"id": 2, "area_code": "nl", "end_year": 2040},
            },
        ]

        mock_result = Mock()
        mock_result.success = True
        mock_result.data = sample

        monkeypatch.setattr(
            list_saved_scenarios.ListSavedScenariosRunner,
            "run",
            staticmethod(lambda *a, **kw: mock_result),
        )

        df = Scenarios.list(client=Mock())

        # Top-level keys are present directly
        assert "id" in df.columns
        assert "title" in df.columns
        assert "scenario_id" in df.columns

        # Nested scenario key is removed; its contents are prefixed
        assert "scenario" not in df.columns
        assert "session_id" in df.columns
        assert "session_area_code" in df.columns
        assert "session_end_year" in df.columns

        # Values are correct
        assert df["session_id"].tolist() == [1, 2]
        assert df["session_area_code"].tolist() == ["nl", "nl"]
        assert df["session_end_year"].tolist() == [2050, 2040]
        assert df["title"].tolist() == ["Saved base", "Saved wind"]

    def test_list_handles_missing_nested_scenario(self, monkeypatch):
        """A missing or None 'scenario' key does not crash; session_* columns are absent."""
        from pyetm.services.scenario_runners import list_saved_scenarios

        sample = [
            {"id": 10, "scenario_id": 1, "title": "No nested"},
            {"id": 11, "scenario_id": 2, "title": "Null nested", "scenario": None},
        ]

        mock_result = Mock()
        mock_result.success = True
        mock_result.data = sample

        monkeypatch.setattr(
            list_saved_scenarios.ListSavedScenariosRunner,
            "run",
            staticmethod(lambda *a, **kw: mock_result),
        )

        df = Scenarios.list(client=Mock())

        assert len(df) == 2
        assert "scenario" not in df.columns
        # No session_* columns should appear since nested dicts are empty/missing
        session_cols = [c for c in df.columns if c.startswith("session_")]
        assert session_cols == []

    def test_list_returns_empty_dataframe_on_failure(self, monkeypatch):
        """Failed runner result yields an empty DataFrame."""
        from pyetm.services.scenario_runners import list_saved_scenarios

        mock_result = Mock()
        mock_result.success = False
        mock_result.errors = ["403 Forbidden"]

        monkeypatch.setattr(
            list_saved_scenarios.ListSavedScenariosRunner,
            "run",
            staticmethod(lambda *a, **kw: mock_result),
        )

        df = Scenarios.list(client=Mock())

        assert df.empty
