from pyetm.services.scenario_runners.update_metadata import UpdateMetadataRunner


def test_update_metadata_success(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 1, "private": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(1)
    metadata = {"private": True}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/1", {"json": {"scenario": metadata}})]


def test_update_metadata_single_field(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 2, "source": "pyetm"}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(2)
    metadata = {"source": "pyetm"}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/2", {"json": {"scenario": metadata}})]


def test_update_metadata_multiple_valid_fields(
    dummy_client, fake_response, dummy_scenario
):
    body = {"scenario": {"id": 3}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(3)
    metadata = {
        "private": False,
        "keep_compatible": True,
        "source": "api_update",
        "title": "My Test Scenario",
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/3", {"json": {"scenario": metadata}})]


def test_update_metadata_filters_non_updatable_fields(
    dummy_client, fake_response, dummy_scenario
):
    body = {"scenario": {"id": 4, "private": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(4)
    metadata = {
        "private": True,  # Valid
        "end_year": 2050,  # Invalid - should be filtered
        "area_code": "nl",  # Invalid - should be filtered
        "id": 999,  # Invalid - should be filtered
        "created_at": "2023-01-01",  # Invalid - should be filtered
        "updated_at": "2023-01-02",  # Invalid - should be filtered
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body

    # Should have warnings about filtered fields
    expected_warnings = [
        "Ignoring non-updatable metadata field: 'end_year'",
        "Ignoring non-updatable metadata field: 'area_code'",
        "Ignoring non-updatable metadata field: 'id'",
        "Ignoring non-updatable metadata field: 'created_at'",
        "Ignoring non-updatable metadata field: 'updated_at'",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    # Should only send valid fields
    expected_payload = {"scenario": {"private": True}}
    assert client.calls == [("/scenarios/4", {"json": expected_payload})]


def test_update_metadata_empty_metadata(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 5}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(5)
    metadata = {}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/5", {"json": {"scenario": {}}})]


def test_update_metadata_with_nested_metadata_field(
    dummy_client, fake_response, dummy_scenario
):
    body = {"scenario": {"id": 6}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(6)
    metadata = {
        "metadata": {
            "description": "Updated scenario",
            "author": "bert",
            "tags": ["test", "pyetm"],
        }
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/6", {"json": {"scenario": metadata}})]


def test_update_metadata_with_kwargs(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 7, "title": "Updated Scenario"}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(7)
    metadata = {"title": "Updated Scenario"}

    result = UpdateMetadataRunner.run(client, scenario, metadata, timeout=30)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify the structure
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios/7"
    assert client.calls[0][1]["json"] == {"scenario": metadata}


def test_update_metadata_http_failure_422(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=422, text="Validation Error")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(8)
    metadata = {"private": "invalid_value"}  # Invalid value for private field

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["422: Validation Error"]


def test_update_metadata_http_failure_404(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=404, text="Scenario not found")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(999)
    metadata = {"private": True}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Scenario not found"]


def test_update_metadata_connection_error(dummy_client, dummy_scenario):
    client = dummy_client(ConnectionError("Connection failed"), method="put")
    scenario = dummy_scenario(9)
    metadata = {"private": True}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_update_metadata_all_valid_fields(dummy_client, fake_response, dummy_scenario):
    """Test updating all valid metadata fields"""
    body = {"scenario": {"id": 10}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(10)

    # Only include the valid META_KEYS
    metadata = {
        "keep_compatible": True,
        "private": False,
        "source": "pyetm",
        "metadata": {"test": "data", "author": "test_user"},
        "title": "Complete Test Scenario",
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/10", {"json": {"scenario": metadata}})]


def test_update_metadata_payload_structure(dummy_client, fake_response, dummy_scenario):
    """Test that the payload is correctly structured for the API"""
    body = {"scenario": {"id": 11}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(11)
    metadata = {"private": True, "source": "pyetm", "title": "Test Scenario"}

    UpdateMetadataRunner.run(client, scenario, metadata)

    expected_call = (
        "/scenarios/11",
        {
            "json": {
                "scenario": {
                    "private": True,
                    "source": "pyetm",
                    "title": "Test Scenario",
                }
            }
        },
    )
    assert client.calls == [expected_call]


def test_update_metadata_mixed_valid_invalid_fields(
    dummy_client, fake_response, dummy_scenario
):
    """Test updating with a mix of valid and invalid fields"""
    body = {"scenario": {"id": 12, "private": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(12)

    metadata = {
        "private": True,  # Valid
        "title": "Test",  # Valid
        "end_year": 2050,  # Invalid - not in META_KEYS
        "area_code": "nl",  # Invalid - not in META_KEYS
        "id": 999,  # Invalid - not in META_KEYS
        "created_at": "2023-01-01",  # Invalid - not in META_KEYS
        "invalid_field": "value",  # Invalid - not in META_KEYS
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body

    # Should have warnings for all filtered fields (5 invalid fields)
    assert len([err for err in result.errors if "Ignoring non-updatable" in err]) == 5

    # Should only send valid fields
    expected_payload = {"scenario": {"private": True, "title": "Test"}}
    assert client.calls == [("/scenarios/12", {"json": expected_payload})]
