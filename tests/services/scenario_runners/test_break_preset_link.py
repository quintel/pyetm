from pyetm.services.scenario_runners.break_preset_link import BreakPresetLinkRunner


def test_clears_the_preset_on_the_session_itself(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 123, "preset_scenario_id": None}}
    client = dummy_client(fake_response(ok=True, status_code=200, json_data=body), method="put")

    result = BreakPresetLinkRunner.run(client, dummy_scenario(123))

    assert result.data == body
    assert client.calls == [("/saved_scenarios/123/", {"json": {"scenario": {"preset_scenario_id": None}}})]


def test_fails_with_the_relayed_refusal(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=422, json_data={}, text='{"errors": {"base": ["locked"]}}')
    client = dummy_client(response, method="put")

    result = BreakPresetLinkRunner.run(client, dummy_scenario(123))

    assert result.errors == ["422: base: locked"]
