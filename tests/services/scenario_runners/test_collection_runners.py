"""Tests for collection-related runners."""

from pyetm.services.scenario_runners.fetch_user_collections import (
    FetchUserCollectionsRunner,
)
from pyetm.services.scenario_runners.fetch_collection import FetchCollectionRunner
from pyetm.services.scenario_runners.create_collection import CreateCollectionRunner
from pyetm.services.scenario_runners.update_collection import UpdateCollectionRunner
from pyetm.services.scenario_runners.delete_collection import DeleteCollectionRunner


# FetchUserCollectionsRunner tests


def test_fetch_user_collections_success(dummy_client, fake_response):
    """Test successful fetch of user collections."""
    body = {
        "collections": [
            {"id": 1, "title": "Collection 1", "created_at": "2024-01-01"},
            {"id": 2, "title": "Collection 2", "created_at": "2024-01-02"},
        ]
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserCollectionsRunner.run(client)
    assert result.success is True
    assert result.data == body["collections"]
    assert result.errors == []


def test_fetch_user_collections_empty_list(dummy_client, fake_response):
    """Test fetch with no collections."""
    body = {"collections": []}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserCollectionsRunner.run(client)
    assert result.success is True
    assert result.data == []
    assert result.errors == []


def test_fetch_user_collections_direct_list_response(dummy_client, fake_response):
    """Test backwards compatibility with direct list response."""
    body = [
        {"id": 1, "title": "Collection 1"},
        {"id": 2, "title": "Collection 2"},
    ]
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserCollectionsRunner.run(client)
    assert result.success is True
    assert result.data == body


def test_fetch_user_collections_http_failure_401(dummy_client, fake_response):
    """Test handling of 401 unauthorized error."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="get")

    result = FetchUserCollectionsRunner.run(client)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


# FetchCollectionRunner tests


def test_fetch_collection_success(dummy_client, fake_response):
    """Test successful fetch of a single collection."""
    body = {
        "id": 123,
        "title": "Test Collection",
        "interpolation": False,
        "saved_scenario_ids": [1, 2, 3],
        "created_at": "2024-01-01",
        "updated_at": "2024-01-02",
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchCollectionRunner.run(client, collection_id=123)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/collections/123", None)]


def test_fetch_collection_http_failure_404(dummy_client, fake_response):
    """Test handling of 404 not found error."""
    response = fake_response(ok=False, status_code=404, text="Collection not found")
    client = dummy_client(response, method="get")

    result = FetchCollectionRunner.run(client, collection_id=99999)
    assert result.success is False
    assert result.data is None
    assert "Collection 99999 not found" in result.errors[0]


def test_fetch_collection_invalid_id(dummy_client):
    """Test that invalid IDs are rejected."""
    client = dummy_client({}, method="get")

    result = FetchCollectionRunner.run(client, collection_id=-1)
    assert result.success is False
    assert "Invalid collection_id" in result.errors[0]
    assert len(client.calls) == 0


# CreateCollectionRunner tests


def test_create_collection_success(dummy_client, fake_response):
    """Test successful creation of a collection."""
    body = {
        "id": 456,
        "title": "New Collection",
        "interpolation": False,
        "saved_scenario_ids": [1, 2, 3],
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    collection_data = {
        "title": "New Collection",
        "saved_scenario_ids": [1, 2, 3],
        "interpolation": False,
    }

    result = CreateCollectionRunner.run(client, collection_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/collections", {"json": {"collection": collection_data}})
    ]


def test_create_collection_missing_title(dummy_client):
    """Test that missing title is rejected."""
    client = dummy_client({}, method="post")

    collection_data = {"saved_scenario_ids": [1, 2, 3]}

    result = CreateCollectionRunner.run(client, collection_data)
    assert result.success is False
    assert "title" in result.errors[0].lower()
    assert len(client.calls) == 0


def test_create_collection_missing_scenarios(dummy_client):
    """Test that at least one scenario list is required."""
    client = dummy_client({}, method="post")

    collection_data = {"title": "Test Collection"}

    result = CreateCollectionRunner.run(client, collection_data)
    assert result.success is False
    assert "at least one 'scenario_ids' or 'saved_scenario_ids'" in result.errors[0]
    assert len(client.calls) == 0


def test_create_collection_with_scenario_ids(dummy_client, fake_response):
    """Test creation with scenario_ids instead of saved_scenario_ids."""
    body = {"id": 456, "title": "New Collection", "scenario_ids": [101, 102]}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    collection_data = {
        "title": "New Collection",
        "scenario_ids": [101, 102],
    }

    result = CreateCollectionRunner.run(client, collection_data)
    assert result.success is True
    assert result.data == body


def test_create_collection_filters_invalid_fields(dummy_client, fake_response):
    """Test that invalid fields are filtered."""
    body = {"id": 456, "title": "New Collection"}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    collection_data = {
        "title": "New Collection",
        "saved_scenario_ids": [1, 2],
        "id": 999,  # Invalid
        "invalid_field": "value",  # Invalid
    }

    result = CreateCollectionRunner.run(client, collection_data)
    assert result.success is True
    # Should have warnings for filtered fields
    assert any("Ignoring invalid field" in err for err in result.errors)


# UpdateCollectionRunner tests


def test_update_collection_success(dummy_client, fake_response):
    """Test successful update of a collection."""
    body = {
        "id": 123,
        "title": "Updated Title",
        "saved_scenario_ids": [1, 2, 3, 4],
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {"title": "Updated Title", "saved_scenario_ids": [1, 2, 3, 4]}

    result = UpdateCollectionRunner.run(client, collection_id=123, update_data=update_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/collections/123", {"json": {"collection": update_data}})
    ]


def test_update_collection_empty_update_data(dummy_client):
    """Test that empty update data returns an error."""
    client = dummy_client({}, method="put")

    result = UpdateCollectionRunner.run(client, collection_id=123, update_data={})
    assert result.success is False
    assert "No fields provided for update" in result.errors
    assert len(client.calls) == 0


def test_update_collection_invalid_id(dummy_client):
    """Test that invalid IDs are rejected."""
    client = dummy_client({}, method="put")

    result = UpdateCollectionRunner.run(
        client, collection_id=0, update_data={"title": "New"}
    )
    assert result.success is False
    assert "Invalid collection_id" in result.errors[0]
    assert len(client.calls) == 0


def test_update_collection_filters_invalid_fields(dummy_client, fake_response):
    """Test that invalid fields are filtered."""
    body = {"id": 123, "title": "Updated Title"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    update_data = {
        "title": "Updated Title",  # Valid
        "id": 999,  # Invalid
        "created_at": "2024-01-01",  # Invalid
    }

    result = UpdateCollectionRunner.run(client, collection_id=123, update_data=update_data)
    assert result.success is True
    assert any("Ignoring invalid field" in err for err in result.errors)


# DeleteCollectionRunner tests


def test_delete_collection_success(dummy_client, fake_response):
    """Test successful deletion of a collection."""
    body = {"message": "Collection deleted successfully"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="delete")

    result = DeleteCollectionRunner.run(client, collection_id=123)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/collections/123", None)]


def test_delete_collection_http_failure_404(dummy_client, fake_response):
    """Test handling of 404 not found error."""
    response = fake_response(ok=False, status_code=404, text="Collection not found")
    client = dummy_client(response, method="delete")

    result = DeleteCollectionRunner.run(client, collection_id=99999)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Collection not found"]


def test_delete_collection_invalid_id(dummy_client):
    """Test that invalid IDs are rejected."""
    client = dummy_client({}, method="delete")

    result = DeleteCollectionRunner.run(client, collection_id=-1)
    assert result.success is False
    assert "Invalid collection_id" in result.errors[0]
    assert len(client.calls) == 0


def test_delete_collection_http_failure_403(dummy_client, fake_response):
    """Test handling of 403 forbidden error."""
    response = fake_response(
        ok=False, status_code=403, text="Not authorized to delete this collection"
    )
    client = dummy_client(response, method="delete")

    result = DeleteCollectionRunner.run(client, collection_id=123)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["403: Not authorized to delete this collection"]
