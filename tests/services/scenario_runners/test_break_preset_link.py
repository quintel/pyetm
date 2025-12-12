from pyetm.services.scenario_runners.break_preset_link import BreakPresetLinkRunner


def test_break_preset_link_with_scenario_object(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 123, "preset_scenario_id": None, "updated": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(123)

    result = BreakPresetLinkRunner.run(client, scenario)

    assert result.success is True
    assert result.data == body
    assert result.errors == []

    expected_payload = {"scenario": {"preset_scenario_id": None}}
    assert client.calls == [("/scenarios/123", {"json": expected_payload})]


def test_break_preset_link_with_scenario_id(dummy_client, fake_response):
    body = {"scenario": {"id": 456, "preset_scenario_id": None, "updated": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    result = BreakPresetLinkRunner.run(client, scenario=456)

    assert result.success is True
    assert result.data == body
    assert result.errors == []

    expected_payload = {"scenario": {"preset_scenario_id": None}}
    assert client.calls == [("/scenarios/456", {"json": expected_payload})]


def test_break_preset_link_http_failure_404(dummy_client, fake_response):
    response = fake_response(ok=False, status_code=404, text="Scenario not found")
    client = dummy_client(response, method="put")

    result = BreakPresetLinkRunner.run(client, scenario=99999)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Scenario not found"]


def test_break_preset_link_http_failure_401(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(123)

    result = BreakPresetLinkRunner.run(client, scenario)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


def test_break_preset_link_http_failure_422(dummy_client, fake_response, dummy_scenario):
    response = fake_response(
        ok=False, status_code=422, text="Cannot modify this scenario"
    )
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(123)

    result = BreakPresetLinkRunner.run(client, scenario)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["422: Cannot modify this scenario"]


def test_break_preset_link_connection_error(dummy_client, dummy_scenario):
    client = dummy_client(ConnectionError("Connection failed"), method="put")
    scenario = dummy_scenario(123)

    result = BreakPresetLinkRunner.run(client, scenario)

    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_break_preset_link_with_kwargs(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 789, "preset_scenario_id": None}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(789)

    result = BreakPresetLinkRunner.run(client, scenario, timeout=30)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify basic structure
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios/789"
    assert client.calls[0][1]["json"] == {"scenario": {"preset_scenario_id": None}}


def test_break_preset_link_payload_structure(dummy_client, fake_response):
    """Test that the payload is correctly structured for the API"""
    body = {"scenario": {"id": 321, "preset_scenario_id": None}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")

    BreakPresetLinkRunner.run(client, scenario=321)

    # Verify the exact payload structure
    expected_call = (
        "/scenarios/321",
        {"json": {"scenario": {"preset_scenario_id": None}}},
    )
    assert client.calls == [expected_call]
