import pandas as pd
import numpy as np
from unittest.mock import Mock, MagicMock, patch
import pytest

from pyetm.models.packables.users_pack import UsersPack
from pyetm.models.scenario import Scenario
from pyetm.models.session import Session
from pyetm.services.service_result import ServiceResult


def make_scenario(id_val=1, identifier="S1"):
    """Create a mock saved Scenario."""
    s = Mock(spec=Scenario)
    s.id = id_val
    s.identifier = Mock(return_value=identifier)
    return s


def make_session(id_val=1, identifier="S1"):
    """Create a mock Session that is not saved to MyETM."""
    s = Mock(spec=Session)
    s.id = id_val
    s.identifier = Mock(return_value=identifier)
    return s


def make_user(email, role):
    """Create a user dictionary as returned by Scenario.list_users()."""
    return {"user_id": 1, "user_email": email, "role": role}


class TestUsersPack:
    def test_set_scenario_short_names(self):
        pack = UsersPack()
        short_names = {"1": "Base", "2": "Alternative"}

        pack.set_scenario_short_names(short_names)

        assert pack._scenario_short_names == short_names

    def test_set_scenario_short_names_with_none(self):
        pack = UsersPack()

        pack.set_scenario_short_names(None)

        assert pack._scenario_short_names == {}

    def test_get_scenario_display_key_uses_short_name(self):
        pack = UsersPack()
        pack.set_scenario_short_names({"1": "Base"})

        scenario = make_scenario(id_val=1)

        result = pack._get_scenario_display_key(scenario)

        assert result == "Base"

    def test_get_scenario_display_key_uses_identifier(self):
        pack = UsersPack()

        scenario = make_scenario(id_val=1, identifier="Scenario1")

        result = pack._get_scenario_display_key(scenario)

        assert result == "Scenario1"

    def test_get_scenario_display_key_falls_back_to_id(self):
        pack = UsersPack()

        scenario = make_scenario(id_val=1)
        scenario.identifier.side_effect = Exception("No identifier")

        result = pack._get_scenario_display_key(scenario)

        assert result == "1"

    def test_resolve_scenario_by_short_name(self):
        s1 = make_scenario(id_val=1, identifier="S1")
        s2 = make_scenario(id_val=2, identifier="S2")

        pack = UsersPack()
        pack.add(s1, s2)
        pack.set_scenario_short_names({"1": "Base", "2": "Alt"})

        result = pack.resolve_scenario("Base")

        assert result == s1

    def test_resolve_scenario_by_numeric_id(self):
        s1 = make_scenario(id_val=1, identifier="S1")
        s2 = make_scenario(id_val=2, identifier="S2")

        pack = UsersPack()
        pack.add(s1, s2)

        result = pack.resolve_scenario("2")

        assert result == s2

    def test_resolve_scenario_returns_none_for_invalid(self):
        pack = UsersPack()

        result = pack.resolve_scenario("nonexistent")

        assert result is None

    def test_to_dataframe_with_users(self):
        s1 = make_scenario(id_val=1, identifier="S1")
        s1.list_users.return_value = [
            make_user("user1@example.com", "owner"),
            make_user("user2@example.com", "viewer"),
        ]
        s2 = make_scenario(id_val=2, identifier="S2")
        s2.list_users.return_value = [
            make_user("user1@example.com", "collaborator"),
            make_user("user3@example.com", "viewer"),
        ]

        pack = UsersPack()
        pack.add(s1, s2)

        df = pack.to_dataframe()

        assert list(df.index) == ["user1@example.com", "user2@example.com", "user3@example.com"]
        assert df.loc["user1@example.com", ("S1", "role")] == "owner"
        assert df.loc["user1@example.com", ("S2", "role")] == "collaborator"
        assert pd.isna(df.loc["user3@example.com", ("S1", "role")])

    def test_to_dataframe_skips_members_without_email(self):
        s1 = make_scenario(id_val=1, identifier="S1")
        s1.list_users.return_value = [
            make_user(None, "owner"),
            make_user("user@example.com", "viewer"),
        ]

        pack = UsersPack()
        pack.add(s1)

        assert list(pack.to_dataframe().index) == ["user@example.com"]

    def test_to_dataframe_handles_api_failure(self, caplog):
        s1 = make_scenario(id_val=1, identifier="S1")
        s1.list_users.side_effect = Exception("API Error")

        pack = UsersPack()
        pack.add(s1)

        with caplog.at_level("WARNING"):
            df = pack.to_dataframe()

            assert df.empty
            assert "Failed to fetch users" in caplog.text

    def test_to_dataframe_warns_for_unsaved_session(self, caplog):
        session = make_session(id_val=1, identifier="Temp")
        saved = make_scenario(id_val=2, identifier="Saved")
        saved.list_users.return_value = [make_user("user@example.com", "owner")]

        pack = UsersPack()
        pack.add(session, saved)

        with caplog.at_level("WARNING"):
            df = pack.to_dataframe()

            assert "Session 'Temp' is not saved to MyETM" in caplog.text
        session.list_users.assert_not_called()
        assert df.loc["user@example.com", ("Saved", "role")] == "owner"
        assert pd.isna(df.loc["user@example.com", ("Temp", "role")])

    def test_to_dataframe_empty_scenarios(self):
        pack = UsersPack()

        df = pack.to_dataframe()

        assert df.empty

    def test_from_dataframe_applies_roles(self, caplog):
        s1 = make_scenario(id_val=1, identifier="S1")
        s1.update_users = Mock()

        pack = UsersPack()
        pack.add(s1)

        # Create proper grid format: header row + data rows
        df = pd.DataFrame(
            [[np.nan, "S1"], ["user1@example.com", "owner"], ["user2@example.com", "viewer"]]
        )

        update_set = {"users"}

        with caplog.at_level("INFO"):
            pack.from_dataframe(df, update_set)

            assert s1.update_users.call_count == 2
            # Verify skip_upload=False when users is in update_set
            # Note: roles are passed as-is; normalization happens in update_users()
            s1.update_users.assert_any_call("user1@example.com", "owner", skip_upload=False)
            s1.update_users.assert_any_call("user2@example.com", "viewer", skip_upload=False)
            assert "Updated user" in caplog.text

    def test_from_dataframe_removes_users(self, caplog):
        s1 = make_scenario(id_val=1, identifier="S1")
        s1.update_users = Mock()

        pack = UsersPack()
        pack.add(s1)

        # Create proper grid format: header row + data rows
        df = pd.DataFrame([[np.nan, "S1"], ["user@example.com", "remove"]])

        update_set = {"users"}

        with caplog.at_level("INFO"):
            pack.from_dataframe(df, update_set)

            s1.update_users.assert_called_once_with("user@example.com", "remove", skip_upload=False)
            assert "Updated user" in caplog.text

    def test_from_dataframe_skips_upload_when_not_in_update_set(self):
        s1 = make_scenario(id_val=1, identifier="S1")
        s1.update_users = Mock()

        pack = UsersPack()
        pack.add(s1)

        # Create proper grid format: header row + data rows
        df = pd.DataFrame([[np.nan, "S1"], ["user@example.com", "owner"]])

        pack.from_dataframe(df, update_set=set())

        # Should call update_users with skip_upload=True
        # Note: roles are passed as-is; normalization happens in update_users()
        s1.update_users.assert_called_once_with("user@example.com", "owner", skip_upload=True)

    def test_from_dataframe_handles_missing_scenario(self, caplog):
        s1 = make_scenario(id_val=1, identifier="S1")

        pack = UsersPack()
        pack.add(s1)

        df = pd.DataFrame(
            {
                "Email": ["", "user@example.com"],
                "S1": ["Scenario 1", "owner"],
                "Unknown": ["Unknown", "viewer"],
            }
        )

        with caplog.at_level("WARNING"):
            pack.from_dataframe(df, {"users"})

            assert "Could not find scenario" in caplog.text
            assert "Unknown" in caplog.text

    def test_from_dataframe_handles_invalid_roles(self):
        s1 = make_scenario(id_val=1, identifier="S1")
        s1.update_users = Mock()

        pack = UsersPack()
        pack.add(s1)

        # Create proper grid format: first row is header, following rows are data
        df = pd.DataFrame([[np.nan, "S1"], ["user@example.com", "invalid_role"]])

        pack.from_dataframe(df, {"users"})

        # Invalid roles are now passed to update_users which will handle validation
        s1.update_users.assert_called_once_with(
            "user@example.com", "invalid_role", skip_upload=False
        )

    def test_from_dataframe_skips_nan_values(self):
        s1 = make_scenario(id_val=1, identifier="S1")

        pack = UsersPack()
        pack.add(s1)

        df = pd.DataFrame(
            {
                "Email": ["", "user1@example.com", "user2@example.com"],
                "S1": ["Scenario 1", "owner", np.nan],
            }
        )

        # Should not raise any exceptions
        pack.from_dataframe(df, {"users"})

    def test_from_dataframe_early_returns(self):
        pack = UsersPack()

        # Should not raise exceptions
        pack.from_dataframe(None, {"users"})
        pack.from_dataframe(pd.DataFrame(), {"users"})

    def test_should_include_upload_with_users(self):
        pack = UsersPack()

        assert pack._should_include_upload({"users"}) is True

    def test_should_include_upload_without_users(self):
        pack = UsersPack()

        assert pack._should_include_upload({"user_values"}) is False
        assert pack._should_include_upload(set()) is False
        assert pack._should_include_upload(None) is False

    def test_pending_users_stored_when_skip_upload(self):
        """Test that user data is stored in _pending_users when skip_upload=True"""
        s1 = make_scenario(id_val=1, identifier="S1")
        s1.update_users = Mock()
        s1._pending_users = {}

        pack = UsersPack()
        pack.add(s1)

        # Create proper grid format: header row + data rows
        df = pd.DataFrame([[np.nan, "S1"], ["user@example.com", "owner"]])

        # Load with update=False (skip_upload=True)
        pack.from_dataframe(df, update_set=set())

        # Verify update_users was called with skip_upload=True
        # Note: roles are passed as-is; normalization happens in update_users()
        s1.update_users.assert_called_once_with("user@example.com", "owner", skip_upload=True)

    def test_from_dataframe_warns_for_unsaved_session(self, caplog):
        session = make_session(id_val=1, identifier="S1")

        pack = UsersPack()
        pack.add(session)

        df = pd.DataFrame([[np.nan, "S1"], ["user@example.com", "owner"]])

        with caplog.at_level("WARNING"):
            pack.from_dataframe(df, {"users"})

            assert "Session 'S1' is not saved to MyETM" in caplog.text
        session.update_users.assert_not_called()


class TestUsersPackThroughMyETM:
    """A saved Scenario's members round trip through the saved scenario users endpoints."""

    @pytest.fixture
    def saved(self):
        scenario = Scenario(id=7, scenario_id=70, title="Saved")
        scenario._scenario_session = Session(id=70, area_code="nl", end_year=2050)
        return scenario

    @pytest.fixture
    def client(self):
        with patch("pyetm.models.scenario.get_client") as get_client:
            yield get_client.return_value

    @patch("pyetm.models.scenario.SavedScenarioUsersIndexRunner")
    def test_export_writes_myetm_members(self, index_runner, saved, client):
        index_runner.run.return_value = ServiceResult.ok(
            [make_user("member@example.com", "scenario_collaborator")]
        )
        pack = UsersPack()
        pack.add(saved)

        df = pack.to_dataframe()

        index_runner.run.assert_called_once_with(client, 7)
        assert df.loc["member@example.com", ("Saved", "role")] == "collaborator"

    @patch("pyetm.models.scenario.SavedScenarioUsersCreateRunner")
    @patch("pyetm.models.scenario.SavedScenarioUsersIndexRunner")
    def test_import_sets_myetm_members(self, index_runner, create_runner, saved, client):
        index_runner.run.return_value = ServiceResult.ok([])
        create_runner.run.return_value = ServiceResult.ok([])
        pack = UsersPack()
        pack.add(saved)

        df = pd.DataFrame([[np.nan, "Saved"], ["new@example.com", "viewer"]])

        pack.from_dataframe(df, {"users"})

        create_runner.run.assert_called_once_with(
            client, 7, [{"user_email": "new@example.com", "role": "scenario_viewer"}]
        )
