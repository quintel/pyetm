from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner


def test_fetch_inputs_success_no_defaults(dummy_client, fake_response, dummy_scenario):
    body = {"i1": {"min": 0.0}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response)
    scenario = dummy_scenario(1)

    result = FetchInputsRunner.run(client, scenario)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/1/inputs", None)]


def test_fetch_inputs_success_with_defaults(
    dummy_client, fake_response, dummy_scenario
):
    body = {"i2": {"default": 42}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response)
    scenario = dummy_scenario(2)

    result = FetchInputsRunner.run(client, scenario, defaults="original")
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/2/inputs", {"params": {"defaults": "original"}})
    ]


def test_fetch_inputs_http_failure(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=500, text="Server Error")
    client = dummy_client(response)
    scenario = dummy_scenario(3)

    result = FetchInputsRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["500: Server Error"]


def test_fetch_inputs_exception(dummy_client, dummy_scenario):
    client = dummy_client(RuntimeError("network error"))
    scenario = dummy_scenario(4)

    result = FetchInputsRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert any("network error" in err for err in result.errors)
