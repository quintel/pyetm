import pytest
from unittest.mock import Mock, patch
from pyetm.services.service_result import ServiceResult
from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.update_metadata import UpdateMetadataRunner


def test_update_metadata_runner_direct_fields_only(
    dummy_client, fake_response, dummy_scenario
):
    """Test updating only fields in META_KEYS."""
    body = {"scenario": {"id": 123, "updated": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(123)
    scenario.metadata = {"existing": "value"}

    metadata = {"end_year": 2050, "private": True, "keep_compatible": False}

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data=body, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {
            "scenario": {
                "end_year": 2050,
                "private": True,
                "keep_compatible": False,
                "metadata": {"existing": "value"},
            }
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )
        assert result == mock_result


def test_update_metadata_runner_nested_metadata_only(
    dummy_client, fake_response, dummy_scenario
):
    """Test updating only custom fields (nested in metadata)."""
    body = {"scenario": {"id": 123, "updated": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(123)
    scenario.metadata = {"existing": "value"}

    metadata = {"custom_field": "custom_value", "another_field": 42}

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data=body, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {
            "scenario": {
                "metadata": {
                    "existing": "value",
                    "custom_field": "custom_value",
                    "another_field": 42,
                }
            }
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_mixed_fields(
    dummy_client, fake_response, dummy_scenario
):
    """Test updating both direct and nested fields."""
    body = {"scenario": {"id": 123, "updated": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(123)
    scenario.metadata = {"existing": "value"}

    metadata = {"end_year": 2050, "private": True, "custom_field": "custom_value"}

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data=body, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {
            "scenario": {
                "end_year": 2050,
                "private": True,
                "metadata": {"existing": "value", "custom_field": "custom_value"},
            }
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_unsettable_keys_generate_warnings():
    """Test that unsettable keys generate warnings and are nested."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    scenario.metadata = {"existing": "value"}

    metadata = {
        "id": 456,  # Unsettable
        "title": "New Title",  # Unsettable
        "end_year": 2050,  # Settable
    }

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {
            "scenario": {
                "end_year": 2050,
                "metadata": {"existing": "value", "id": 456, "title": "New Title"},
            }
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_direct_metadata_field_priority():
    """Test that direct 'metadata' field has highest priority."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    scenario.metadata = {"existing": "value"}

    metadata = {
        "custom_field": "will_be_overridden",
        "metadata": {"custom_field": "priority_value", "new_field": "new_value"},
    }

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {
            "scenario": {
                "metadata": {
                    "existing": "value",
                    "custom_field": "priority_value",  # Direct metadata wins
                    "new_field": "new_value",
                }
            }
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_scenario_without_existing_metadata():
    """Test updating scenario that has no existing metadata."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    scenario.metadata = None  # No existing metadata

    metadata = {"custom_field": "value", "end_year": 2050}

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {
            "scenario": {"end_year": 2050, "metadata": {"custom_field": "value"}}
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_scenario_with_non_dict_metadata():
    """Test updating scenario with non-dict metadata attribute."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    scenario.metadata = "not_a_dict"  # Invalid metadata type

    metadata = {"custom_field": "value"}

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {"scenario": {"metadata": {"custom_field": "value"}}}
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_scenario_without_metadata_attribute():
    """Test updating scenario that doesn't have metadata attribute."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    # Don't set metadata attribute at all

    metadata = {"custom_field": "value"}

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        # Mock hasattr to return False for metadata
        with patch("builtins.hasattr", return_value=False):
            result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {"scenario": {"metadata": {"custom_field": "value"}}}
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_empty_metadata():
    """Test running with empty metadata dictionary."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    scenario.metadata = {"existing": "value"}

    metadata = {}

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        # Should still send existing metadata
        expected_payload = {"scenario": {"metadata": {"existing": "value"}}}
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_all_meta_keys():
    """Test updating with all possible META_KEYS."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    scenario.metadata = {"existing": "value"}

    metadata = {
        "keep_compatible": True,
        "private": False,
        "source": "test_source",
        "metadata": {"nested": "value"},
        "end_year": 2060,
    }

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {
            "scenario": {
                "keep_compatible": True,
                "private": False,
                "source": "test_source",
                "end_year": 2060,
                "metadata": {"existing": "value", "nested": "value"},
            }
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_meta_keys_constants():
    """Test that META_KEYS and UNSETTABLE_META_KEYS are properly defined."""
    expected_meta_keys = [
        "keep_compatible",
        "private",
        "source",
        "metadata",
        "end_year",
    ]

    expected_unsettable_keys = [
        "id",
        "created_at",
        "updated_at",
        "area_code",
        "title",
        "start_year",
        "scaling",
        "template",
        "url",
    ]

    assert UpdateMetadataRunner.META_KEYS == expected_meta_keys
    assert UpdateMetadataRunner.UNSETTABLE_META_KEYS == expected_unsettable_keys


def test_update_metadata_runner_non_dict_direct_metadata_field():
    """Test handling when direct 'metadata' field is not a dict."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    scenario.metadata = {"existing": "value"}

    metadata = {
        "custom_field": "value",
        "metadata": "not_a_dict",  # Invalid metadata type
    }

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        # Should still include existing metadata and custom_field, but skip invalid direct metadata
        expected_payload = {
            "scenario": {"metadata": {"existing": "value", "custom_field": "value"}}
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )


def test_update_metadata_runner_preserves_existing_metadata_when_merging():
    """Test that existing metadata is preserved when adding new fields."""
    client = Mock(spec=BaseClient)
    scenario = Mock()
    scenario.id = 123
    scenario.metadata = {
        "author": "original_author",
        "description": "original_description",
        "tags": ["original"],
    }

    metadata = {
        "description": "updated_description",  # Should override
        "new_field": "new_value",  # Should be added
        "end_year": 2050,  # Direct field
    }

    with patch.object(UpdateMetadataRunner, "_make_request") as mock_request:
        mock_result = ServiceResult(success=True, data={"updated": True}, errors=[])
        mock_request.return_value = mock_result

        result = UpdateMetadataRunner.run(client, scenario, metadata)

        expected_payload = {
            "scenario": {
                "end_year": 2050,
                "metadata": {
                    "author": "original_author",  # Preserved
                    "description": "updated_description",  # Updated
                    "tags": ["original"],  # Preserved
                    "new_field": "new_value",  # Added
                },
            }
        }
        mock_request.assert_called_once_with(
            client=client, method="put", path="/scenarios/123", payload=expected_payload
        )
