from pyetm.services.scenario_runners.fetch_user_saved_scenarios import (
    FetchUserSavedScenariosRunner,
)


def test_fetch_user_saved_scenarios_success(dummy_client, fake_response):
    body = {
        "data": [
            {"id": 1, "title": "Scenario A", "private": False},
            {"id": 2, "title": "Scenario B", "private": True},
        ],
        "links": {},
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is True
    assert len(result.data) == 2
    assert result.data[0]["id"] == 1
    assert result.data[1]["title"] == "Scenario B"
    assert client.calls == [("/saved_scenarios", None)]


def test_fetch_user_saved_scenarios_empty(dummy_client, fake_response):
    body = {"data": [], "links": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is True
    assert result.data == []


def test_fetch_user_saved_scenarios_unauthorized(dummy_client, fake_response):
    response = fake_response(ok=False, status_code=403, text="Forbidden")
    client = dummy_client(response, method="get")

    result = FetchUserSavedScenariosRunner.run(client)
    assert result.success is False
    assert "403" in result.errors[0]
