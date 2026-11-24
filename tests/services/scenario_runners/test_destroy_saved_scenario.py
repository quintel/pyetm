from pyetm.services.scenario_runners.destroy_saved_scenario import DestroySavedScenarioRunner


def test_deletes_the_saved_scenario(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 204), method="delete")

    result = DestroySavedScenarioRunner.run(client, 3)

    assert result.success
    assert client.calls == [("/saved_scenarios/3", None)]


def test_fails_when_refused(dummy_client):
    client = dummy_client(PermissionError("403: forbidden: Not allowed"), method="delete")

    assert DestroySavedScenarioRunner.run(client, 3).errors == ["403: forbidden: Not allowed"]
