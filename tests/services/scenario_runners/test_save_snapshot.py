import json

from pyetm.services.scenario_runners.save_snapshot import SaveSnapshotRunner


def test_posts_a_snapshot_and_unwraps_the_envelope(dummy_client, fake_response):
    body = {"data": {"id": 3, "scenario_id": 31, "scenario_id_history": [30]}, "meta": {}}
    client = dummy_client(fake_response(True, 201, body), method="post")

    result = SaveSnapshotRunner.run(client, 3)

    assert result.data == {"id": 3, "scenario_id": 31, "scenario_id_history": [30]}
    assert client.calls == [("/saved_scenarios/3/snapshots", None)]


def test_fails_with_the_conflict_for_a_discarded_scenario(dummy_client, fake_response):
    body = {"errors": [{"status": "409", "code": "scenario_discarded", "detail": "Saved scenario is discarded"}]}
    client = dummy_client(fake_response(False, 409, body, text=json.dumps(body)), method="post")

    result = SaveSnapshotRunner.run(client, 3)

    assert not result.success
    assert result.errors == ["409: scenario_discarded: Saved scenario is discarded"]
