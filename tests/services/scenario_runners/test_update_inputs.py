from pyetm.services.scenario_runners.update_inputs import UpdateInputsRunner


def test_update_inputs_success(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 1, "user_values": {"input_1": 42.5, "input_2": 100.0}}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(1)
    inputs = {"input_1": 42.5, "input_2": 100.0}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/1", {"json": {"scenario": {"user_values": inputs}}})
    ]


def test_update_inputs_single_input(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 2, "user_values": {"co_firing_biocoal_share": 80}}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(2)
    inputs = {"co_firing_biocoal_share": 80}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is True
    print(result.data)
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/2", {"json": {"scenario": {"user_values": inputs}}})
    ]


def test_update_inputs_empty_inputs(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 3, "user_values": {}}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(3)
    inputs = {}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/3", {"json": {"scenario": {"user_values": {}}}})
    ]


def test_update_inputs_with_kwargs(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 4, "user_values": {"input_1": 50.0}}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(4)
    inputs = {"input_1": 50.0}

    result = UpdateInputsRunner.run(client, scenario, inputs, timeout=30)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Just verify the basic structure - kwargs handling might vary
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios/4"
    assert client.calls[0][1]["json"] == {"scenario": {"user_values": inputs}}


def test_update_inputs_http_failure_422(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=422, text="Validation Error")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(5)
    inputs = {"invalid_input": "bad_value"}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["422: Validation Error"]


def test_update_inputs_http_failure_404(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=404, text="Scenario not found")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(999)
    inputs = {"input_1": 42.5}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Scenario not found"]


def test_update_inputs_http_failure_500(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=500, text="Internal Server Error")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(6)
    inputs = {"input_1": 42.5}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["500: Internal Server Error"]


def test_update_inputs_connection_error(dummy_client, dummy_scenario):
    client = dummy_client(ConnectionError("Connection failed"), method="put")
    scenario = dummy_scenario(7)
    inputs = {"input_1": 42.5}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_update_inputs_permission_error(dummy_client, dummy_scenario):
    client = dummy_client(PermissionError("Access denied"), method="put")
    scenario = dummy_scenario(8)
    inputs = {"input_1": 42.5}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is False
    assert result.data is None
    assert any("Access denied" in err for err in result.errors)


def test_update_inputs_generic_exception(dummy_client, dummy_scenario):
    client = dummy_client(RuntimeError("Unexpected error"), method="put")
    scenario = dummy_scenario(9)
    inputs = {"input_1": 42.5}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is False
    assert result.data is None
    assert any("Unexpected error" in err for err in result.errors)


def test_update_inputs_payload_structure(dummy_client, fake_response, dummy_scenario):
    """Test that the payload is correctly structured for the API"""
    body = {"scenario": {"id": 10}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(10)
    inputs = {"input_a": 1.0, "input_b": 2.0, "input_c": 3.0}

    UpdateInputsRunner.run(client, scenario, inputs)

    # Verify the exact payload structure
    expected_call = (
        "/scenarios/10",
        {
            "json": {
                "scenario": {
                    "user_values": {"input_a": 1.0, "input_b": 2.0, "input_c": 3.0}
                }
            }
        },
    )
    assert client.calls == [expected_call]


def test_update_inputs_numeric_scenario_id(dummy_client, fake_response, dummy_scenario):
    """Test with different scenario ID types"""
    body = {"scenario": {"id": 12345}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(12345)
    inputs = {"input_1": 42.5}

    result = UpdateInputsRunner.run(client, scenario, inputs)
    assert result.success is True
    assert client.calls[0][0] == "/scenarios/12345"
