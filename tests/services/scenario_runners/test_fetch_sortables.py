from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner


def test_fetch_sortables_success(dummy_client, fake_response, dummy_scenario):
    body = {
        "forecast_storage": [1, 2],
        "heat_network": {"lt": [], "mt": [0], "ht": []},
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response)
    scenario = dummy_scenario(10)

    result = FetchSortablesRunner.run(client, scenario)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/10/user_sortables", None)]


def test_fetch_sortables_http_failure(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=403, text="Forbidden")
    client = dummy_client(response)
    scenario = dummy_scenario(11)

    result = FetchSortablesRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["403: Forbidden"]


def test_fetch_sortables_exception(dummy_client, dummy_scenario):
    client = dummy_client(Exception("unexpected failure"))
    scenario = dummy_scenario(12)

    result = FetchSortablesRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert any("unexpected failure" in err for err in result.errors)
