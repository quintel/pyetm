from pyetm.services.scenario_runners.create_scenario import CreateScenarioRunner


def test_create_scenario_success_minimal(dummy_client, fake_response):
    body = {
        "id": 12345,
        "area_code": "nl",
        "end_year": 2050,
        "private": False,
        "created_at": "2019-01-01T00:00:00Z",
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    scenario_data = {"area_code": "nl", "end_year": 2050}

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios", {"json": {"scenario": scenario_data}})]


def test_create_scenario_success_with_optional_fields(dummy_client, fake_response):
    body = {
        "id": 12346,
        "area_code": "nl",
        "end_year": 2050,
        "private": True,
        "start_year": 2019,
        "source": "test",
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    scenario_data = {
        "area_code": "de",
        "end_year": 2050,
        "private": True,
        "start_year": 2019,
        "source": "test",
    }

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios", {"json": {"scenario": scenario_data}})]


def test_create_scenario_with_metadata(dummy_client, fake_response):
    body = {"id": 12347, "area_code": "fr", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    scenario_data = {
        "area_code": "fr",
        "end_year": 2050,
        "metadata": {
            "description": "Test scenario",
            "author": "test_user",
            "tags": ["test", "pyetm"],
        },
    }

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios", {"json": {"scenario": scenario_data}})]


def test_create_scenario_missing_required_field_area_code(dummy_client, fake_response):
    client = dummy_client({}, method="post")

    scenario_data = {"end_year": 2050}  # Missing area_code

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is False
    assert result.data is None
    assert "Missing required fields: area_code" in result.errors[0]
    assert len(client.calls) == 0  # Should not make API call


def test_create_scenario_missing_required_field_end_year(dummy_client, fake_response):
    client = dummy_client({}, method="post")

    scenario_data = {"area_code": "nl"}  # Missing end_year

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is False
    assert result.data is None
    assert "Missing required fields: end_year" in result.errors[0]
    assert len(client.calls) == 0  # Should not make API call


def test_create_scenario_missing_both_required_fields(dummy_client, fake_response):
    client = dummy_client({}, method="post")

    scenario_data = {"private": True}  # Missing both required fields

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is False
    assert result.data is None
    error_msg = result.errors[0]
    assert "Missing required fields:" in error_msg
    assert "area_code" in error_msg
    assert "end_year" in error_msg
    assert len(client.calls) == 0  # Should not make API call


def test_create_scenario_filters_invalid_fields(dummy_client, fake_response):
    body = {"id": 12348, "area_code": "uk", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    scenario_data = {
        "area_code": "uk",
        "end_year": 2050,
        "private": True,  # Valid
        "id": 999,  # Invalid - should be filtered
        "created_at": "2019-01-01",  # Invalid - should be filtered
        "invalid_field": "value",  # Invalid - should be filtered
    }

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is True
    assert result.data == body

    # Should have warnings for filtered fields
    expected_warnings = [
        "Ignoring invalid field for scenario creation: 'id'",
        "Ignoring invalid field for scenario creation: 'created_at'",
        "Ignoring invalid field for scenario creation: 'invalid_field'",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    # Should only send valid fields
    expected_payload = {
        "scenario": {"area_code": "uk", "end_year": 2050, "private": True}
    }
    assert client.calls == [("/scenarios", {"json": expected_payload})]


def test_create_scenario_all_optional_fields(dummy_client, fake_response):
    body = {"id": 12349, "area_code": "be", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    scenario_data = {
        "area_code": "be",
        "end_year": 2050,
        "keep_compatible": True,
        "private": False,
        "source": "test",
        "metadata": {"test": "data"},
        "start_year": 2024,
        "scaling": {"factor": 1.5},
        "template": 123,
        "url": "https://example.com",
    }

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios", {"json": {"scenario": scenario_data}})]


def test_create_scenario_http_failure_422(dummy_client, fake_response):
    response = fake_response(ok=False, status_code=422, text="Validation Error")
    client = dummy_client(response, method="post")

    scenario_data = {"area_code": "invalid", "end_year": 1999}  # Invalid end year

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["422: Validation Error"]


def test_create_scenario_http_failure_401(dummy_client, fake_response):
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="post")

    scenario_data = {"area_code": "nl", "end_year": 2050}

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


def test_create_scenario_connection_error(dummy_client):
    client = dummy_client(ConnectionError("Connection failed"), method="post")

    scenario_data = {"area_code": "nl", "end_year": 2050}

    result = CreateScenarioRunner.run(client, scenario_data)
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_create_scenario_with_kwargs(dummy_client, fake_response):
    body = {"id": 12350, "area_code": "nl", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    scenario_data = {"area_code": "nl", "end_year": 2050}

    result = CreateScenarioRunner.run(client, scenario_data, timeout=30)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify basic structure
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios"
    assert client.calls[0][1]["json"] == {"scenario": scenario_data}


def test_create_scenario_payload_structure(dummy_client, fake_response):
    """Test that the payload is correctly structured for the API"""
    body = {"id": 12351, "area_code": "at", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    scenario_data = {
        "area_code": "at",
        "end_year": 2050,
        "private": True,
        "source": "test",
    }

    CreateScenarioRunner.run(client, scenario_data)

    # Verify the exact payload structure
    expected_call = ("/scenarios", {"json": {"scenario": scenario_data}})
    assert client.calls == [expected_call]
