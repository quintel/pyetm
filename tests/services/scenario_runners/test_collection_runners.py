from pyetm.services.scenario_runners.create_collection import CreateCollectionRunner
from pyetm.services.scenario_runners.delete_collection import DeleteCollectionRunner
from pyetm.services.scenario_runners.fetch_collection import FetchCollectionRunner
from pyetm.services.scenario_runners.fetch_user_collections import FetchUserCollectionsRunner
from pyetm.services.scenario_runners.update_collection import UpdateCollectionRunner

COLLECTION = {"id": 4, "title": "Paths", "version": "latest", "saved_scenario_ids": [1, 2]}


def envelope(data):
    return {"data": data, "meta": {}}


def test_creates_under_the_collection_key(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 201, envelope(COLLECTION)), method="post")
    params = {"title": "Paths", "version": "latest", "saved_scenario_ids": [1, 2]}

    result = CreateCollectionRunner.run(client, params)

    assert result.data == COLLECTION
    assert client.calls == [("/collections", {"json": {"collection": params}})]


def test_create_needs_a_title_version_and_saved_scenarios(dummy_client):
    client = dummy_client({}, method="post")

    result = CreateCollectionRunner.run(client, {"title": "Paths"})

    assert result.errors == ["Missing required fields: version, saved_scenario_ids"]
    assert client.calls == []


def test_create_drops_fields_v4_does_not_accept(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 201, envelope(COLLECTION)), method="post")

    result = CreateCollectionRunner.run(
        client, {"title": "Paths", "version": "latest", "saved_scenario_ids": [1], "interpolation": True}
    )

    assert result.errors == ["Ignoring invalid field for create collection: 'interpolation'"]


def test_fetches_one(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 200, envelope(COLLECTION)))

    assert FetchCollectionRunner.run(client, 4).data == COLLECTION
    assert client.calls == [("/collections/4", None)]


def test_lists_every_page(dummy_client, fake_response):
    body = {"data": [COLLECTION], "meta": {"pagination": {"page": 1, "pages": 1}}}
    client = dummy_client(fake_response(True, 200, body))

    assert FetchUserCollectionsRunner.run(client).data == [COLLECTION]
    assert client.calls == [("/collections", {"params": {"page": 1, "limit": 100}})]


def test_updates_title_and_saved_scenarios(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 200, envelope(COLLECTION)), method="put")

    UpdateCollectionRunner.run(client, 4, {"title": "New", "saved_scenario_ids": [3]})

    assert client.calls == [("/collections/4", {"json": {"collection": {"title": "New", "saved_scenario_ids": [3]}}})]


def test_update_refuses_only_unaccepted_fields(dummy_client):
    result = UpdateCollectionRunner.run(dummy_client({}, method="put"), 4, {"discarded": True})

    assert result.errors[0] == "No valid fields provided for update"


def test_deletes(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 204), method="delete")

    assert DeleteCollectionRunner.run(client, 4).success
    assert client.calls == [("/collections/4", None)]


def test_delete_fails_when_refused(dummy_client):
    client = dummy_client(PermissionError("403: forbidden: Not yours"), method="delete")

    assert DeleteCollectionRunner.run(client, 4).errors == ["403: forbidden: Not yours"]
