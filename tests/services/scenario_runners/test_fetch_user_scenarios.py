from pyetm.services.scenario_runners.fetch_user_scenarios import FetchUserScenariosRunner


def test_fetch_user_scenarios_success(dummy_client, fake_response):
    body = {
        "data": [
            {"id": 1, "area_code": "nl2019", "end_year": 2050},
            {"id": 2, "area_code": "nl2019", "end_year": 2040},
        ],
        "links": {},
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is True
    assert len(result.data) == 2
    assert result.data[0]["id"] == 1
    assert result.data[1]["id"] == 2
    assert client.calls == [("/scenarios", None)]


def test_fetch_user_scenarios_with_pagination(dummy_client, fake_response):
    body = {"data": [{"id": 3, "area_code": "nl2019", "end_year": 2050}], "links": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client, page=2, limit=10)
    assert result.success is True
    assert len(result.data) == 1
    assert client.calls == [("/scenarios", {"params": {"page": 2, "limit": 10}})]


def test_fetch_user_scenarios_empty(dummy_client, fake_response):
    body = {"data": [], "links": {}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is True
    assert result.data == []


def test_fetch_user_scenarios_unauthorized(dummy_client, fake_response):
    response = fake_response(ok=False, status_code=403, text="Forbidden")
    client = dummy_client(response, method="get")

    result = FetchUserScenariosRunner.run(client)
    assert result.success is False
    assert "403" in result.errors[0]
