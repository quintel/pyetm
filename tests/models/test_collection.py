"""Tests for Collection model."""

from unittest.mock import Mock
import pytest
from pyetm.models.collection import Collection, CollectionError
from pyetm.services.scenario_runners.create_collection import CreateCollectionRunner
from pyetm.services.scenario_runners.fetch_collection import FetchCollectionRunner
from pyetm.services.scenario_runners.fetch_user_collections import (
    FetchUserCollectionsRunner,
)
from pyetm.services.scenario_runners.update_collection import UpdateCollectionRunner
from pyetm.services.scenario_runners.delete_collection import DeleteCollectionRunner


# --- Model Validation Tests ---


def test_collection_validation_minimal():
    """Test Collection model validates with minimal required fields."""
    data = {
        "id": 1,
        "title": "Test Collection",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }
    collection = Collection.model_validate(data)
    assert collection.id == 1
    assert collection.title == "Test Collection"
    assert collection.interpolation is False


def test_collection_validation_full():
    """Test Collection model validates with all fields."""
    data = {
        "id": 123,
        "title": "Full Collection",
        "interpolation": True,
        "area_code": "nl",
        "end_year": 2050,
        "version": "2024.01",
        "owner": {"id": 1, "name": "Test User"},
        "saved_scenario_ids": [1, 2, 3],
        "scenario_ids": [101, 102],
        "collections_app_url": "https://example.com/collections/123",
        "interpolation_params": {"area_code": "nl", "end_years": [2030, 2040, 2050]},
        "discarded": False,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-02T00:00:00Z",
    }
    collection = Collection.model_validate(data)
    assert collection.id == 123
    assert collection.title == "Full Collection"
    assert collection.interpolation is True
    assert collection.area_code == "nl"
    assert collection.end_year == 2050
    assert collection.owner["name"] == "Test User"
    assert collection.saved_scenario_ids == [1, 2, 3]


def test_collection_equality():
    """Test collection equality based on ID."""
    collection1 = Collection(
        id=123,
        title="Test",
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
    )
    collection2 = Collection(
        id=123,
        title="Different Title",
        created_at="2024-01-02T00:00:00Z",
        updated_at="2024-01-02T00:00:00Z",
    )
    collection3 = Collection(
        id=456,
        title="Test",
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
    )

    assert collection1 == collection2  # Same ID
    assert collection1 != collection3  # Different ID


def test_collection_hash():
    """Test collection hashing based on ID."""
    collection1 = Collection(
        id=123,
        title="Test",
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
    )
    collection2 = Collection(
        id=123,
        title="Different",
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
    )

    assert hash(collection1) == hash(collection2)
    assert len({collection1, collection2}) == 1  # Should be treated as same in set


# --- Create Tests ---


def test_collection_create_success(monkeypatch, ok_service_result, mock_client):
    """Test successful Collection creation."""
    created_data = {
        "id": 456,
        "title": "New Collection",
        "interpolation": False,
        "saved_scenario_ids": [1, 2, 3],
        "scenario_ids": [],
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }

    monkeypatch.setattr(
        CreateCollectionRunner,
        "run",
        lambda client, collection_data: ok_service_result(created_data),
    )

    collection = Collection.create(
        title="New Collection",
        saved_scenario_ids=[1, 2, 3],
        client=mock_client,
    )
    assert collection.id == 456
    assert collection.title == "New Collection"
    assert collection.saved_scenario_ids == [1, 2, 3]
    assert collection.interpolation is False


def test_collection_create_interpolated(monkeypatch, ok_service_result, mock_client):
    """Test creation of interpolated collection."""
    created_data = {
        "id": 789,
        "title": "Interpolated Collection",
        "interpolation": True,
        "area_code": "nl",
        "end_year": 2050,
        "saved_scenario_ids": [1],
        "interpolation_params": {"area_code": "nl", "end_years": [2030, 2040, 2050]},
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }

    monkeypatch.setattr(
        CreateCollectionRunner,
        "run",
        lambda client, collection_data: ok_service_result(created_data),
    )

    collection = Collection.create(
        title="Interpolated Collection",
        saved_scenario_ids=[1],
        area_code="nl",
        end_year=2050,
        interpolation=True,
        client=mock_client,
    )
    assert collection.interpolation is True
    assert collection.area_code == "nl"
    assert collection.end_year == 2050


def test_collection_create_with_warnings(monkeypatch, ok_service_result, mock_client):
    """Test Collection creation with warnings."""
    created_data = {
        "id": 790,
        "title": "Collection",
        "saved_scenario_ids": [1],
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }
    warnings = ["Ignoring invalid field for create collection: 'invalid_field'"]

    monkeypatch.setattr(
        CreateCollectionRunner,
        "run",
        lambda client, collection_data: ok_service_result(created_data, warnings),
    )

    collection = Collection.create(
        title="Collection", saved_scenario_ids=[1], client=mock_client
    )
    assert collection.id == 790
    base_warnings = collection.warnings.get_by_field("base")
    assert len(base_warnings) == 1


def test_collection_create_failure(monkeypatch, fail_service_result, mock_client):
    """Test Collection creation failure raises CollectionError."""
    monkeypatch.setattr(
        CreateCollectionRunner,
        "run",
        lambda client, collection_data: fail_service_result(
            ["Missing required field: title"]
        ),
    )

    with pytest.raises(CollectionError, match="Could not create collection"):
        Collection.create(title="", saved_scenario_ids=[1], client=mock_client)


# --- Load Tests ---


def test_collection_load_success(monkeypatch, ok_service_result, mock_client):
    """Test successful loading of a collection."""
    collection_data = {
        "id": 123,
        "title": "Test Collection",
        "interpolation": False,
        "saved_scenario_ids": [1, 2, 3],
        "scenario_ids": [],
        "discarded": False,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-02T00:00:00Z",
        "owner": {"id": 1, "name": "Test User"},
        "collections_app_url": "https://example.com/collections/123",
    }

    monkeypatch.setattr(
        FetchCollectionRunner,
        "run",
        lambda client, collection_id: ok_service_result(collection_data),
    )

    collection = Collection.load(123, client=mock_client)
    assert collection.id == 123
    assert collection.title == "Test Collection"
    assert collection.saved_scenario_ids == [1, 2, 3]
    assert collection.owner["name"] == "Test User"


def test_collection_load_not_found(monkeypatch, fail_service_result, mock_client):
    """Test loading non-existent collection."""
    monkeypatch.setattr(
        FetchCollectionRunner,
        "run",
        lambda client, collection_id: fail_service_result(
            ["Collection 99999 not found on this environment"]
        ),
    )

    with pytest.raises(CollectionError, match="Could not load collection"):
        Collection.load(99999, client=mock_client)


def test_collection_load_all_success(monkeypatch, ok_service_result, mock_client):
    """Test loading all user collections."""
    collections_data = [
        {
            "id": 1,
            "title": "Collection 1",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        },
        {
            "id": 2,
            "title": "Collection 2",
            "created_at": "2024-01-02T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
        },
        {
            "id": 3,
            "title": "Collection 3",
            "created_at": "2024-01-03T00:00:00Z",
            "updated_at": "2024-01-03T00:00:00Z",
        },
    ]

    monkeypatch.setattr(
        FetchUserCollectionsRunner,
        "run",
        lambda client: ok_service_result(collections_data),
    )

    collections = Collection.load_all(client=mock_client)
    assert len(collections) == 3
    assert collections[0].id == 1
    assert collections[1].id == 2
    assert collections[2].id == 3
    assert all(isinstance(c, Collection) for c in collections)


def test_collection_load_all_empty(monkeypatch, ok_service_result, mock_client):
    """Test loading when user has no collections."""
    monkeypatch.setattr(
        FetchUserCollectionsRunner,
        "run",
        lambda client: ok_service_result([]),
    )

    collections = Collection.load_all(client=mock_client)
    assert len(collections) == 0
    assert isinstance(collections, list)


def test_collection_load_all_failure(monkeypatch, fail_service_result, mock_client):
    """Test load_all failure raises CollectionError."""
    monkeypatch.setattr(
        FetchUserCollectionsRunner,
        "run",
        lambda client: fail_service_result(["401: Unauthorized"]),
    )

    with pytest.raises(CollectionError, match="Could not load collections"):
        Collection.load_all(client=mock_client)


# --- Update Tests ---


def test_collection_update_success(monkeypatch, ok_service_result, mock_client):
    """Test successful update of a collection."""
    original_data = {
        "id": 123,
        "title": "Original Title",
        "saved_scenario_ids": [1, 2],
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }
    updated_data = {
        "id": 123,
        "title": "Updated Title",
        "saved_scenario_ids": [1, 2, 3, 4],
        "updated_at": "2024-01-02T00:00:00Z",
    }

    monkeypatch.setattr(
        UpdateCollectionRunner,
        "run",
        lambda client, collection_id, update_data: ok_service_result(updated_data),
    )

    collection = Collection.model_validate(original_data)
    collection._client = mock_client
    collection.update(title="Updated Title", saved_scenario_ids=[1, 2, 3, 4])

    assert collection.title == "Updated Title"
    assert collection.saved_scenario_ids == [1, 2, 3, 4]


def test_collection_update_with_warnings(monkeypatch, ok_service_result, mock_client):
    """Test collection update with warnings."""
    original_data = {
        "id": 123,
        "title": "Original",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }
    updated_data = {"id": 123, "title": "Updated"}
    warnings = ["Ignoring invalid field for update collection: 'invalid'"]

    monkeypatch.setattr(
        UpdateCollectionRunner,
        "run",
        lambda client, collection_id, update_data: ok_service_result(
            updated_data, warnings
        ),
    )

    collection = Collection.model_validate(original_data)
    collection._client = mock_client
    collection.update(title="Updated")

    update_warnings = collection.warnings.get_by_field("update")
    assert len(update_warnings) == 1


def test_collection_update_failure(monkeypatch, fail_service_result, mock_client):
    """Test update failure raises CollectionError."""
    collection_data = {
        "id": 123,
        "title": "Original",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }

    monkeypatch.setattr(
        UpdateCollectionRunner,
        "run",
        lambda client, collection_id, update_data: fail_service_result(
            ["403: Forbidden"]
        ),
    )

    collection = Collection.model_validate(collection_data)
    collection._client = mock_client

    with pytest.raises(CollectionError, match="Could not update collection"):
        collection.update(title="New Title")


# --- Delete Tests ---


def test_collection_delete_success(monkeypatch, ok_service_result, mock_client):
    """Test successful deletion of a collection."""
    delete_response = {"message": "Collection deleted"}

    monkeypatch.setattr(
        DeleteCollectionRunner,
        "run",
        lambda client, collection_id: ok_service_result(delete_response),
    )

    collection = Collection(
        id=123,
        title="Test",
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
    )
    collection._client = mock_client

    # Should not raise an exception
    collection.delete()


def test_collection_delete_failure(monkeypatch, fail_service_result, mock_client):
    """Test deletion failure raises CollectionError."""
    monkeypatch.setattr(
        DeleteCollectionRunner,
        "run",
        lambda client, collection_id: fail_service_result(
            ["403: Not authorized to delete this collection"]
        ),
    )

    collection = Collection(
        id=123,
        title="Test",
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
    )
    collection._client = mock_client

    with pytest.raises(CollectionError, match="Could not delete collection"):
        collection.delete()


def test_collection_delete_with_explicit_client(monkeypatch, ok_service_result, mock_client):
    """Test that delete can use an explicitly passed client."""
    delete_response = {"message": "Deleted"}

    # Track which client was used
    used_client = None

    def mock_delete(client, collection_id):
        nonlocal used_client
        used_client = client
        return ok_service_result(delete_response)

    monkeypatch.setattr(DeleteCollectionRunner, "run", mock_delete)

    collection = Collection(
        id=123,
        title="Test",
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
    )

    # Delete with explicit client
    collection.delete(client=mock_client)

    # Verify the explicit client was used
    assert used_client is mock_client


# --- to_dict Tests ---


def test_collection_to_dict():
    """Test converting collection to dictionary."""
    collection = Collection(
        id=123,
        title="Test Collection",
        interpolation=False,
        saved_scenario_ids=[1, 2, 3],
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
    )

    result = collection.to_dict()

    assert isinstance(result, dict)
    assert result["id"] == 123
    assert result["title"] == "Test Collection"
    assert result["saved_scenario_ids"] == [1, 2, 3]
