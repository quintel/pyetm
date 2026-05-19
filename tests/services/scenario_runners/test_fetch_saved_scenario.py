from pyetm.services.scenario_runners.fetch_saved_scenario import (
    FetchSavedScenarioRunner,
)


def test_fetch_saved_scenario_success(dummy_client, fake_response, dummy_scenario):
    body = {
        "id": 1,
        "scenario_id": 123,
        "title": "Fetched Scenario",
        "private": False,
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    saved_scenario = dummy_scenario(1)

    result = FetchSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is True
    assert result.data["id"] == 1
    assert result.data["scenario_id"] == 123
    assert result.data["title"] == "Fetched Scenario"
    assert client.calls == [("/saved_scenarios/1", None)]


def test_fetch_saved_scenario_missing_fields_warning(dummy_client, fake_response, dummy_scenario):
    # Response missing some fields
    body = {
        "id": 2,
        "scenario_id": 456,
        "title": "Incomplete",
        # Missing: private, created_at, updated_at
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    saved_scenario = dummy_scenario(2)

    result = FetchSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is True
    assert result.data["id"] == 2
    # Should have warnings for missing fields
    assert any("Missing field in response" in err for err in result.errors)


def test_fetch_saved_scenario_not_found(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=404, text="Not Found")
    client = dummy_client(response, method="get")

    saved_scenario = dummy_scenario(999)

    result = FetchSavedScenarioRunner.run(client, saved_scenario)
    assert result.success is False
    assert result.errors == ["SavedScenario 999 not found on this environment"]
