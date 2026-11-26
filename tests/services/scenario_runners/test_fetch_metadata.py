from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner

SESSION = {"id": 70, "start_year": 2023, "keep_compatible": True, "template": 69, "scaling": None}


def test_reads_the_current_session_through_the_base_path(dummy_client, fake_response, dummy_scenario):
    client = dummy_client(fake_response(ok=True, status_code=200, json_data=SESSION))

    result = FetchMetadataRunner.run(client, dummy_scenario(7))

    assert result.data == SESSION
    assert client.calls == [("/saved_scenarios/7/", None)]


def test_fails_when_the_engine_is_unavailable(dummy_client, fake_response, dummy_scenario):
    body = '{"errors":[{"status":502,"code":"engine_unavailable","detail":"The engine did not answer"}]}'
    client = dummy_client(fake_response(ok=False, status_code=502, text=body))

    result = FetchMetadataRunner.run(client, dummy_scenario(7))

    assert result.errors == ["502: engine_unavailable: The engine did not answer"]
