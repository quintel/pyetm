from pyetm.services.scenario_runners.fetch_saved_scenario import FetchSavedScenarioRunner


def test_fetches_and_unwraps_the_envelope(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 200, {"data": {"id": 7, "title": "T"}, "meta": {}}))

    result = FetchSavedScenarioRunner.run(client, 7)

    assert result.data == {"id": 7, "title": "T"}
    assert client.calls == [("/saved_scenarios/7", None)]


def test_fails_with_the_not_found_message(dummy_client):
    client = dummy_client(ValueError("404: scenario_not_found: Saved scenario not found"))

    result = FetchSavedScenarioRunner.run(client, 7)

    assert result.errors == ["404: scenario_not_found: Saved scenario not found"]
