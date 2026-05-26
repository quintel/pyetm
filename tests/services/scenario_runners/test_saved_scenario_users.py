from unittest.mock import Mock
import pytest
from pyetm.services.scenario_runners.saved_scenario_users_index import (
    SavedScenarioUsersIndexRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_create import (
    SavedScenarioUsersCreateRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_update import (
    SavedScenarioUsersUpdateRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_destroy import (
    SavedScenarioUsersDestroyRunner,
)
from pyetm.clients.base_client import BaseClient


class TestSavedScenarioUsersIndexRunner:
    def test_successful_fetch(self):
        """Test successfully fetching saved scenario users"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = [
            {"user_id": 1, "user_email": None, "role": "scenario_owner"},
            {"user_id": 2, "user_email": "viewer@test.com", "role": "scenario_viewer"},
        ]
        mock_client.session.get.return_value = mock_response

        result = SavedScenarioUsersIndexRunner.run(mock_client, 123)

        assert result.success
        assert len(result.data) == 2
        assert result.data[0]["role"] == "scenario_owner"
        mock_client.session.get.assert_called_once_with("/saved_scenarios/123/users")

    def test_failed_fetch(self):
        """Test failed fetch returns error"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_client.session.get.return_value = mock_response

        result = SavedScenarioUsersIndexRunner.run(mock_client, 999)

        assert not result.success
        assert "404" in result.errors[0]


class TestSavedScenarioUsersCreateRunner:
    def test_successful_creation(self):
        """Test successfully creating saved scenario users"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = [
            {
                "user_id": None,
                "user_email": "viewer@test.com",
                "role": "scenario_viewer",
            },
            {
                "user_id": None,
                "user_email": "owner@test.com",
                "role": "scenario_owner",
            },
        ]
        mock_client.session.post.return_value = mock_response

        users = [
            {"user_email": "viewer@test.com", "role": "scenario_viewer"},
            {"user_email": "owner@test.com", "role": "scenario_owner"},
        ]
        result = SavedScenarioUsersCreateRunner.run(mock_client, 123, users)

        assert result.success
        assert len(result.data) == 2
        mock_client.session.post.assert_called_once_with(
            "/saved_scenarios/123/users", json={"saved_scenario_users": users}
        )

    def test_partial_success(self):
        """Test partial success when some users succeed and others fail"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = False  # 422 is not ok
        mock_response.status_code = 422
        mock_response.json.return_value = {
            "success": [
                {
                    "user_id": None,
                    "user_email": "valid@test.com",
                    "role": "scenario_viewer",
                }
            ],
            "errors": {
                "invalid@test": ["Email is invalid"],
                "duplicate@test.com": ["User already has access"],
            },
        }
        mock_client.session.post.return_value = mock_response

        users = [
            {"user_email": "valid@test.com", "role": "scenario_viewer"},
            {"user_email": "invalid@test", "role": "scenario_viewer"},
            {"user_email": "duplicate@test.com", "role": "scenario_owner"},
        ]
        result = SavedScenarioUsersCreateRunner.run(mock_client, 123, users)

        # Should be treated as failure but preserve success info
        assert not result.success
        # Check that successful operations are reported
        assert any("Partial success" in err for err in result.errors)
        assert any("valid@test.com" in str(err) for err in result.errors)
        # Check that errors are reported
        assert any("invalid@test" in err for err in result.errors)
        assert any("Email is invalid" in err for err in result.errors)
        assert any("duplicate@test.com" in err for err in result.errors)
        assert any("User already has access" in err for err in result.errors)

    def test_empty_users_list(self):
        """Test validation fails with empty users list"""
        mock_client = Mock(spec=BaseClient)

        result = SavedScenarioUsersCreateRunner.run(mock_client, 123, [])

        assert not result.success
        assert "No users provided" in result.errors[0]

    def test_missing_required_fields(self):
        """Test validation fails with missing required fields"""
        mock_client = Mock(spec=BaseClient)

        users = [{"user_email": "test@test.com"}]  # Missing role
        result = SavedScenarioUsersCreateRunner.run(mock_client, 123, users)

        assert not result.success
        assert "Missing required fields" in result.errors[0]


class TestSavedScenarioUsersUpdateRunner:
    def test_successful_update(self):
        """Test successfully updating saved scenario user roles"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = [
            {"user_id": 2, "user_email": None, "role": "scenario_owner"},
        ]
        mock_client.session.put.return_value = mock_response

        users = [{"user_id": 2, "role": "scenario_owner"}]
        result = SavedScenarioUsersUpdateRunner.run(mock_client, 123, users)

        assert result.success
        assert result.data[0]["role"] == "scenario_owner"
        mock_client.session.put.assert_called_once_with(
            "/saved_scenarios/123/users", json={"saved_scenario_users": users}
        )

    def test_update_by_email(self):
        """Test updating user by email address"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = [
            {
                "user_id": None,
                "user_email": "test@test.com",
                "role": "scenario_collaborator",
            }
        ]
        mock_client.session.put.return_value = mock_response

        users = [{"user_email": "test@test.com", "role": "scenario_collaborator"}]
        result = SavedScenarioUsersUpdateRunner.run(mock_client, 123, users)

        assert result.success
        assert result.data[0]["role"] == "scenario_collaborator"

    def test_partial_success_update(self):
        """Test partial success when updating multiple users"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = False  # 422 is not ok
        mock_response.status_code = 422
        mock_response.json.return_value = {
            "success": [{"user_id": 1, "user_email": None, "role": "scenario_owner"}],
            "errors": {
                "notfound@test.com": ["User not found"],
            },
        }
        mock_client.session.put.return_value = mock_response

        users = [
            {"user_id": 1, "role": "scenario_owner"},
            {"user_email": "notfound@test.com", "role": "scenario_viewer"},
        ]
        result = SavedScenarioUsersUpdateRunner.run(mock_client, 123, users)

        # Should be treated as failure but preserve success info
        assert not result.success
        assert any("Partial success" in err for err in result.errors)
        assert any("notfound@test.com" in err for err in result.errors)
        assert any("User not found" in err for err in result.errors)

    def test_missing_identifier(self):
        """Test validation fails without user_id or user_email"""
        mock_client = Mock(spec=BaseClient)

        users = [{"role": "scenario_viewer"}]  # Missing identifier
        result = SavedScenarioUsersUpdateRunner.run(mock_client, 123, users)

        assert not result.success
        assert "user_id" in result.errors[0] or "user_email" in result.errors[0]


class TestSavedScenarioUsersDestroyRunner:
    def test_successful_deletion(self):
        """Test successfully deleting saved scenario users"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = [
            {"user_id": 2, "user_email": None, "role": "scenario_viewer"}
        ]
        mock_client.session.delete.return_value = mock_response

        users = [{"user_id": 2}]
        result = SavedScenarioUsersDestroyRunner.run(mock_client, 123, users)

        assert result.success
        mock_client.session.delete.assert_called_once_with(
            "/saved_scenarios/123/users", json={"saved_scenario_users": users}
        )

    def test_delete_by_email(self):
        """Test deleting user by email address"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = [{"user_id": None, "user_email": "test@test.com"}]
        mock_client.session.delete.return_value = mock_response

        users = [{"user_email": "test@test.com"}]
        result = SavedScenarioUsersDestroyRunner.run(mock_client, 123, users)

        assert result.success

    def test_partial_success_deletion(self):
        """Test partial success when deleting multiple users"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = False  # 422 is not ok
        mock_response.status_code = 422
        mock_response.json.return_value = {
            "success": [{"user_id": 2, "user_email": None, "role": "scenario_viewer"}],
            "errors": {
                "lastowner@test.com": ["Cannot remove last owner"],
            },
        }
        mock_client.session.delete.return_value = mock_response

        users = [{"user_id": 2}, {"user_email": "lastowner@test.com"}]
        result = SavedScenarioUsersDestroyRunner.run(mock_client, 123, users)

        # Should be treated as failure but preserve success info
        assert not result.success
        assert any("Partial success" in err for err in result.errors)
        assert any("lastowner@test.com" in err for err in result.errors)
        assert any("Cannot remove last owner" in err for err in result.errors)

    def test_empty_users_list(self):
        """Test validation fails with empty users list"""
        mock_client = Mock(spec=BaseClient)

        result = SavedScenarioUsersDestroyRunner.run(mock_client, 123, [])

        assert not result.success
        assert "No users provided" in result.errors[0]

    def test_missing_identifier(self):
        """Test validation fails without user_id or user_email"""
        mock_client = Mock(spec=BaseClient)

        users = [{}]  # No identifier
        result = SavedScenarioUsersDestroyRunner.run(mock_client, 123, users)

        assert not result.success
        assert "user_id" in result.errors[0] or "user_email" in result.errors[0]


class TestEmptyErrorHandling:
    """Test that empty error messages from the server are handled gracefully"""

    def test_empty_error_list(self):
        """Test that empty strings in error list are filtered out"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 422
        mock_response.json.return_value = {
            "success": [],  # No successes
            "errors": [""],  # Empty error message
        }
        mock_client.session.post.return_value = mock_response

        users = [{"user_email": "test@test.com", "role": "scenario_viewer"}]
        result = SavedScenarioUsersCreateRunner.run(mock_client, 123, users)

        assert not result.success
        # Should have a helpful fallback message instead of empty string
        assert len(result.errors) > 0
        assert any("unknown error" in err.lower() for err in result.errors)

    def test_empty_error_in_dict(self):
        """Test that empty error messages in dict values are filtered out"""
        mock_client = Mock(spec=BaseClient)
        mock_client.session = Mock()
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 422
        mock_response.json.return_value = {
            "errors": {
                "test@test.com": [""]  # Empty error message
            }
        }
        mock_client.session.post.return_value = mock_response

        users = [{"user_email": "test@test.com", "role": "scenario_viewer"}]
        result = SavedScenarioUsersCreateRunner.run(mock_client, 123, users)

        assert not result.success
        # Empty errors should be filtered, but we should still have an error
        # Either from filtering fallback or from other sources
        assert len(result.errors) > 0
