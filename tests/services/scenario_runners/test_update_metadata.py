from pyetm.services.scenario_runners.update_metadata import UpdateMetadataRunner


def test_update_metadata_success(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 1, "end_year": 2050, "private": True}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(1)
    metadata = {"end_year": 2050, "private": True}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/1", {"json": {"scenario": metadata}})]


def test_update_metadata_single_field(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 2, "area_code": "nl"}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(2)
    metadata = {"area_code": "nl"}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/2", {"json": {"scenario": metadata}})]


def test_update_metadata_multiple_fields(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 3}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(3)
    metadata = {
        "end_year": 2050,
        "start_year": 2019,
        "private": False,
        "area_code": "nl",
        "source": "api_update",
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/3", {"json": {"scenario": metadata}})]


def test_update_metadata_filters_non_updatable_fields(
    dummy_client, fake_response, dummy_scenario
):
    body = {"scenario": {"id": 4, "end_year": 2050}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(4)
    metadata = {
        "end_year": 2050,
        "id": 999,
        "created_at": "2023-01-01",
        "updated_at": "2023-01-02",
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body

    # Should have warnings about filtered fields
    expected_warnings = [
        "Ignoring non-updatable metadata field: 'id'",
        "Ignoring non-updatable metadata field: 'created_at'",
        "Ignoring non-updatable metadata field: 'updated_at'",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    # Should only send updatable fields
    expected_payload = {"scenario": {"end_year": 2050}}
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
    body = {"scenario": {"id": 7, "end_year": 2050}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(7)
    metadata = {"end_year": 2050}

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
    metadata = {"end_year": 1999}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["422: Validation Error"]


def test_update_metadata_http_failure_404(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=404, text="Scenario not found")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(999)
    metadata = {"end_year": 2050}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Scenario not found"]


def test_update_metadata_connection_error(dummy_client, dummy_scenario):
    client = dummy_client(ConnectionError("Connection failed"), method="put")
    scenario = dummy_scenario(9)
    metadata = {"end_year": 2050}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_update_metadata_all_updatable_fields(
    dummy_client, fake_response, dummy_scenario
):
    """Test updating all valid metadata fields"""
    body = {"scenario": {"id": 10}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(10)

    metadata = {
        "end_year": 2050,
        "keep_compatible": True,
        "private": False,
        "area_code": "nl",
        "source": "test",
        "metadata": {"test": "data"},
        "start_year": 2019,
        "scaling": {"factor": 1.5},
        "template": 123,
        "url": "https://example.com",
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
    metadata = {"end_year": 2050, "private": True, "area_code": "nl"}

    UpdateMetadataRunner.run(client, scenario, metadata)

    expected_call = (
        "/scenarios/11",
        {"json": {"scenario": {"end_year": 2050, "private": True, "area_code": "nl"}}},
    )
    assert client.calls == [expected_call]


def test_update_metadata_mixed_valid_invalid_fields(
    dummy_client, fake_response, dummy_scenario
):
    """Test updating with a mix of valid and invalid fields"""
    body = {"scenario": {"id": 12, "end_year": 2050}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(12)

    metadata = {
        "end_year": 2050,  # Valid
        "private": True,  # Valid
        "id": 999,  # Invalid
        "created_at": "2023-01-01//7",  # Invalid
        "invalid_field": "value",  # Invalid
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body

    # Should have warnings for all filtered fields
    assert len([err for err in result.errors if "Ignoring non-updatable" in err]) == 3

    # Should only send valid fields
    expected_payload = {"scenario": {"end_year": 2050, "private": True}}
    assert client.calls == [("/scenarios/12", {"json": expected_payload})]
