"""Tests for Scenarios.delete_many() bulk deletion functionality."""

from unittest.mock import Mock, patch
from pyetm.models.scenarios import Scenarios
from pyetm.clients.base_client import AsyncBatchRunner
from pyetm.services.service_result import ServiceResult


def test_scenarios_delete_many_success(monkeypatch, mock_client):
    """Test successful bulk deletion of scenarios."""
    scenario_ids = [123, 456, 789]

    # Mock successful batch results
    mock_results = [
        ServiceResult.ok({"message": "Deleted"}),
        ServiceResult.ok({"message": "Deleted"}),
        ServiceResult.ok({"message": "Deleted"}),
    ]

    monkeypatch.setattr(
        AsyncBatchRunner,
        "batch_requests_sync",
        lambda session, requests, max_concurrent: mock_results,
    )

    result = Scenarios.delete_many(scenario_ids, client=mock_client)

    assert result["successful"] == scenario_ids
    assert result["failed"] == []


def test_scenarios_delete_many_partial_failure(monkeypatch, mock_client):
    """Test bulk deletion with some failures."""
    scenario_ids = [123, 456, 789]

    # Mock mixed results - middle one fails
    mock_results = [
        ServiceResult.ok({"message": "Deleted"}),
        ServiceResult.fail(["404: Not found"]),
        ServiceResult.ok({"message": "Deleted"}),
    ]

    monkeypatch.setattr(
        AsyncBatchRunner,
        "batch_requests_sync",
        lambda session, requests, max_concurrent: mock_results,
    )

    result = Scenarios.delete_many(scenario_ids, client=mock_client)

    assert result["successful"] == [123, 789]
    assert result["failed"] == [456]


def test_scenarios_delete_many_all_failures(monkeypatch, mock_client):
    """Test bulk deletion where all deletions fail."""
    scenario_ids = [123, 456]

    # Mock all failures
    mock_results = [
        ServiceResult.fail(["403: Forbidden"]),
        ServiceResult.fail(["404: Not found"]),
    ]

    monkeypatch.setattr(
        AsyncBatchRunner,
        "batch_requests_sync",
        lambda session, requests, max_concurrent: mock_results,
    )

    result = Scenarios.delete_many(scenario_ids, client=mock_client)

    assert result["successful"] == []
    assert result["failed"] == scenario_ids


def test_scenarios_delete_many_empty_list(mock_client):
    """Test bulk deletion with empty list."""
    result = Scenarios.delete_many([], client=mock_client)

    assert result["successful"] == []
    assert result["failed"] == []


def test_scenarios_delete_many_single_id(monkeypatch, mock_client):
    """Test bulk deletion with single ID."""
    mock_results = [ServiceResult.ok({"message": "Deleted"})]

    monkeypatch.setattr(
        AsyncBatchRunner,
        "batch_requests_sync",
        lambda session, requests, max_concurrent: mock_results,
    )

    result = Scenarios.delete_many([123], client=mock_client)

    assert result["successful"] == [123]
    assert result["failed"] == []
