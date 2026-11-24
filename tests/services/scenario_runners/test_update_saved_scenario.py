from pyetm.services.scenario_runners.update_saved_scenario import UpdateSavedScenarioRunner


def test_updates_under_the_saved_scenario_key(dummy_client, fake_response):
    body = {"data": {"id": 1, "title": "New"}, "meta": {}}
    client = dummy_client(fake_response(True, 200, body), method="put")

    result = UpdateSavedScenarioRunner.run(client, 1, {"title": "New", "private": True})

    assert result.data == {"id": 1, "title": "New"}
    assert client.calls == [("/saved_scenarios/1", {"json": {"saved_scenario": {"title": "New", "private": True}}})]


def test_refuses_fields_v4_does_not_accept(dummy_client):
    client = dummy_client({}, method="put")

    result = UpdateSavedScenarioRunner.run(client, 1, {"scenario_id": 5, "discarded": True})

    assert not result.success
    assert result.errors[0] == "No valid fields provided for update"
    assert client.calls == []


def test_keeps_a_warning_for_a_dropped_field(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 200, {"data": {"id": 1}, "meta": {}}), method="put")

    result = UpdateSavedScenarioRunner.run(client, 1, {"title": "New", "scenario_id": 5})

    assert result.success
    assert result.errors == ["Ignoring invalid field for update saved scenario: 'scenario_id'"]
