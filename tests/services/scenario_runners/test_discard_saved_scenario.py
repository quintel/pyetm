from pyetm.services.scenario_runners.discard_saved_scenario import DiscardSavedScenarioRunner


def test_discards_and_unwraps_the_envelope(dummy_client, fake_response):
    body = {"data": {"id": 3, "discarded_at": "2026-11-20T10:00:00Z"}, "meta": {}}
    client = dummy_client(fake_response(True, 200, body), method="put")

    result = DiscardSavedScenarioRunner.run(client, 3)

    assert result.data["discarded_at"] == "2026-11-20T10:00:00Z"
    assert client.calls == [("/saved_scenarios/3/discard", None)]


def test_builds_the_same_request_for_batches():
    assert DiscardSavedScenarioRunner.build_request(3) == {"method": "put", "path": "/saved_scenarios/3/discard"}
