from pyetm.services.scenario_runners.create_saved_scenario import CreateSavedScenarioRunner

SAVED = {"id": 456, "scenario_id": 123, "title": "Mine", "version": "latest", "private": False}


def test_creates_from_an_area_and_end_year_and_unwraps_the_envelope(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 201, {"data": SAVED, "meta": {}}), method="post")
    params = {"title": "Mine", "version": "latest", "area_code": "nl2023", "end_year": 2050}

    result = CreateSavedScenarioRunner.run(client, params)

    assert result.success and result.data == SAVED
    assert client.calls == [("/saved_scenarios", {"json": {"saved_scenario": params}})]


def test_creates_from_a_source(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 201, {"data": SAVED, "meta": {}}), method="post")
    params = {"title": "Copy", "version": "latest", "source": 9, "end_year": 2040}

    CreateSavedScenarioRunner.run(client, params)

    assert client.calls[0][1]["json"] == {"saved_scenario": params}


def test_needs_a_title_and_a_version(dummy_client):
    client = dummy_client({}, method="post")

    result = CreateSavedScenarioRunner.run(client, {"area_code": "nl", "end_year": 2050})

    assert not result.success
    assert result.errors == ["Missing required fields: title, version"]
    assert client.calls == []


def test_drops_an_unknown_field_with_a_warning(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 201, {"data": SAVED, "meta": {}}), method="post")

    result = CreateSavedScenarioRunner.run(client, {"title": "Mine", "version": "latest", "scenario_id": 1})

    assert result.errors == ["Ignoring invalid field for create saved scenario: 'scenario_id'"]
    assert "scenario_id" not in client.calls[0][1]["json"]["saved_scenario"]


def test_fails_with_the_refusal(dummy_client):
    client = dummy_client(ValueError("422: validation_failed: must match (at /saved_scenario/version)"), method="post")

    result = CreateSavedScenarioRunner.run(client, {"title": "Mine", "version": "2025-01", "source": 9})

    assert not result.success
    assert result.errors == ["422: validation_failed: must match (at /saved_scenario/version)"]
