from pyetm.services.scenario_runners.copy_scenario import CopyScenarioRunner


def test_copy_scenario_success_no_overrides(dummy_client, fake_response):
    body = {
        "id": 67890,
        "area_code": "nl",
        "end_year": 2050,
        "private": False,
        "title": "Copy of Original Scenario",
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    result = CopyScenarioRunner.run(client, scenario_id=12345)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios", {"json": {"scenario": {"scenario_id": 12345}}})]


def test_copy_scenario_with_metadata_override_including_title(dummy_client, fake_response):
    """Test that title can be set via metadata"""
    body = {
        "id": 67891,
        "area_code": "nl",
        "end_year": 2050,
        "metadata": {"title": "My Custom Copy"},
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    overrides = {"metadata": {"title": "My Custom Copy"}}
    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides=overrides)

    assert result.success is True
    assert result.data == body
    assert result.errors == []

    expected_payload = {"scenario": {"scenario_id": 12345, "metadata": {"title": "My Custom Copy"}}}
    assert client.calls == [("/scenarios", {"json": expected_payload})]


def test_copy_scenario_with_multiple_overrides(dummy_client, fake_response):
    body = {
        "id": 67892,
        "private": True,
        "source": "test",
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    overrides = {
        "private": True,
        "source": "test",
    }
    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides=overrides)

    assert result.success is True
    assert result.data == body
    assert result.errors == []

    expected_payload = {
        "scenario": {
            "scenario_id": 12345,
            "private": True,
            "source": "test",
        }
    }
    assert client.calls == [("/scenarios", {"json": expected_payload})]


def test_copy_scenario_with_metadata_override(dummy_client, fake_response):
    body = {
        "id": 67893,
        "area_code": "nl",
        "end_year": 2050,
        "metadata": {"tags": ["test"]},
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    overrides = {"metadata": {"tags": ["test"]}}
    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides=overrides)

    assert result.success is True
    assert result.data == body
    assert result.errors == []

    expected_payload = {
        "scenario": {
            "scenario_id": 12345,
            "metadata": {"tags": ["test"]},
        }
    }
    assert client.calls == [("/scenarios", {"json": expected_payload})]


def test_copy_scenario_title_is_filtered(dummy_client, fake_response):
    """Test that title as a top-level field is filtered out with a warning"""
    body = {
        "id": 67894,
        "area_code": "nl",
        "end_year": 2050,
    }
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    overrides = {
        "title": "My Copy",  # This should be filtered
        "private": True,
    }
    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides=overrides)

    assert result.success is True
    assert result.data == body

    # Should have warning for title being filtered
    assert "Ignoring invalid field for scenario copy: 'title'" in result.errors

    expected_payload = {
        "scenario": {
            "scenario_id": 12345,
            "private": True,
        }
    }
    assert client.calls == [("/scenarios", {"json": expected_payload})]


def test_copy_scenario_filters_set_preset_roles(dummy_client, fake_response):
    """Test that set_preset_roles is filtered out with a warning"""
    body = {"id": 67895, "area_code": "nl", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    overrides = {"set_preset_roles": True}
    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides=overrides)

    assert result.success is True
    assert result.data == body

    # Should have warning for set_preset_roles being filtered
    assert "Ignoring invalid field for scenario copy: 'set_preset_roles'" in result.errors

    expected_payload = {"scenario": {"scenario_id": 12345}}
    assert client.calls == [("/scenarios", {"json": expected_payload})]


def test_copy_scenario_filters_invalid_fields(dummy_client, fake_response):
    body = {"id": 67896, "area_code": "nl", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    overrides = {
        "private": True,  # Valid
        "id": 999,  # Invalid - should be filtered
        "created_at": "2019-01-01",  # Invalid - should be filtered
        "invalid_field": "value",  # Invalid - should be filtered
    }

    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides=overrides)

    assert result.success is True
    assert result.data == body

    # Should have warnings for filtered fields
    expected_warnings = [
        "Ignoring invalid field for scenario copy: 'id'",
        "Ignoring invalid field for scenario copy: 'created_at'",
        "Ignoring invalid field for scenario copy: 'invalid_field'",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    # Should only send valid fields
    expected_payload = {
        "scenario": {
            "scenario_id": 12345,
            "private": True,
        }
    }
    assert client.calls == [("/scenarios", {"json": expected_payload})]


def test_copy_scenario_all_allowed_overrides(dummy_client, fake_response):
    body = {"id": 67897, "area_code": "be", "end_year": 2040}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    overrides = {
        "metadata": {"test": "data"},
        "source": "test",
        "private": False,
        "keep_compatible": True,
    }

    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides=overrides)

    assert result.success is True
    assert result.data == body
    assert result.errors == []

    expected_payload = {
        "scenario": {
            "scenario_id": 12345,
            "metadata": {"test": "data"},
            "source": "test",
            "private": False,
            "keep_compatible": True,
        }
    }
    assert client.calls == [("/scenarios", {"json": expected_payload})]


def test_copy_scenario_http_failure_404(dummy_client, fake_response):
    response = fake_response(ok=False, status_code=404, text="Scenario not found")
    client = dummy_client(response, method="post")

    result = CopyScenarioRunner.run(client, scenario_id=99999)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Scenario not found"]


def test_copy_scenario_http_failure_401(dummy_client, fake_response):
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="post")

    result = CopyScenarioRunner.run(client, scenario_id=12345)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


def test_copy_scenario_connection_error(dummy_client):
    client = dummy_client(ConnectionError("Connection failed"), method="post")

    result = CopyScenarioRunner.run(client, scenario_id=12345)

    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_copy_scenario_with_kwargs(dummy_client, fake_response):
    body = {"id": 67897, "area_code": "nl", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    result = CopyScenarioRunner.run(client, scenario_id=12345, timeout=30)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify basic structure
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios"
    assert client.calls[0][1]["json"] == {"scenario": {"scenario_id": 12345}}


def test_copy_scenario_none_overrides(dummy_client, fake_response):
    """Test that None overrides is handled correctly"""
    body = {"id": 67898, "area_code": "nl", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides=None)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios", {"json": {"scenario": {"scenario_id": 12345}}})]


def test_copy_scenario_empty_overrides(dummy_client, fake_response):
    """Test that empty overrides dict is handled correctly"""
    body = {"id": 67899, "area_code": "nl", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    result = CopyScenarioRunner.run(client, scenario_id=12345, overrides={})

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios", {"json": {"scenario": {"scenario_id": 12345}}})]


def test_copy_scenario_payload_structure(dummy_client, fake_response):
    """Test that the payload is correctly structured for the API"""
    body = {"id": 67900, "area_code": "fr", "end_year": 2050}
    response = fake_response(ok=True, status_code=201, json_data=body)
    client = dummy_client(response, method="post")

    overrides = {"private": True}
    CopyScenarioRunner.run(client, scenario_id=12345, overrides=overrides)

    # Verify the exact payload structure
    expected_call = (
        "/scenarios",
        {
            "json": {
                "scenario": {
                    "scenario_id": 12345,
                    "private": True,
                }
            }
        },
    )
    assert client.calls == [expected_call]
