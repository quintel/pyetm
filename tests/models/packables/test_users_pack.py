import pandas as pd
import numpy as np
from unittest.mock import Mock, MagicMock, patch
import pytest

from pyetm.models.packables.users_pack import UsersPack
from pyetm.services.service_result import ServiceResult


def make_scenario(id_val=1, identifier="S1", client=None):
    """Create a mock scenario."""
    s = Mock()
    s.id = id_val
    s.identifier = Mock(return_value=identifier)
    s.client = client or Mock()
    return s


def make_user(email, role):
    """Create a user dictionary as returned by the API."""
    return {"email": email, "role": role}


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

    def test_api_role_to_short(self):
        pack = UsersPack()

        assert pack._api_role_to_short("scenario_owner") == "owner"
        assert pack._api_role_to_short("scenario_viewer") == "viewer"
        assert pack._api_role_to_short("scenario_collaborator") == "collaborator"

    @patch("pyetm.models.packables.users_pack.ScenarioUsersIndexRunner")
    def test_to_dataframe_with_users(self, mock_runner):
        s1 = make_scenario(id_val=1, identifier="S1")
        s2 = make_scenario(id_val=2, identifier="S2")

        # Mock the API responses
        mock_runner.run.side_effect = [
            ServiceResult.ok(
                [
                    make_user("user1@example.com", "scenario_owner"),
                    make_user("user2@example.com", "scenario_viewer"),
                ]
            ),
            ServiceResult.ok(
                [
                    make_user("user1@example.com", "scenario_collaborator"),
                    make_user("user3@example.com", "scenario_viewer"),
                ]
            ),
        ]

        pack = UsersPack()
        pack.add(s1, s2)

        df = pack.to_dataframe()

        assert not df.empty
        assert "S1" in df.columns.get_level_values("scenario")
        assert "S2" in df.columns.get_level_values("scenario")
        assert "user1@example.com" in df.index
        assert "user2@example.com" in df.index
        assert "user3@example.com" in df.index

    @patch("pyetm.models.packables.users_pack.ScenarioUsersIndexRunner")
    def test_to_dataframe_converts_to_short_names(self, mock_runner):
        s1 = make_scenario(id_val=1, identifier="S1")

        mock_runner.run.return_value = ServiceResult.ok(
            [make_user("user@example.com", "scenario_owner")]
        )

        pack = UsersPack()
        pack.add(s1)

        df = pack.to_dataframe()

        assert df.loc["user@example.com", ("S1", "role")] == "owner"

    @patch("pyetm.models.packables.users_pack.ScenarioUsersIndexRunner")
    def test_to_dataframe_handles_api_failure(self, mock_runner, caplog):
        s1 = make_scenario(id_val=1, identifier="S1")

        mock_runner.run.return_value = ServiceResult.fail(["API Error"])

        pack = UsersPack()
        pack.add(s1)

        with caplog.at_level("WARNING"):
            df = pack.to_dataframe()

            assert df.empty
            assert "Failed to fetch users" in caplog.text

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

    def test_apply_pending_users_uploads_stored_data(self):
        """Test that apply_pending_users() uploads the stored user data"""
        from pyetm.models.session import Session

        # Create a real Session instance to test apply_pending_users
        s1 = Session(id=1, area_code="nl", end_year=2050)
        s1._pending_users = {
            "user1@example.com": "scenario_owner",
            "user2@example.com": "scenario_viewer",
        }

        # Mock the actual API-calling methods
        s1._user_exists = Mock(return_value=False)
        s1._add_user = Mock()
        s1._remove_user = Mock()
        s1._update_user_role = Mock()

        # Call apply_pending_users
        count = s1.apply_pending_users()

        # Should have called _add_user twice (for new users)
        assert s1._add_user.call_count == 2
        assert count == 2

        # Verify _pending_users is cleared
        assert len(s1._pending_users) == 0
