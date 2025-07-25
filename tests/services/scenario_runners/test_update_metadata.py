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
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/3", {"json": {"scenario": metadata}})]


def test_update_metadata_auto_nests_unrecognized_fields(
    dummy_client, fake_response, dummy_scenario
):
    """Test that unrecognized fields are automatically nested under 'metadata'"""
    body = {"scenario": {"id": 4}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(4)
    metadata = {
        "private": True,  # Direct field
        "title": "My Test",  # Should be nested
        "description": "A test scenario",  # Should be nested
        "author": "John Doe",  # Should be nested
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert len(result.errors) == 1
    assert (
        "Field 'title' cannot be updated directly and has been added to nested metadata instead"
        in result.errors
    )

    # Should auto-nest unrecognized fields
    expected_payload = {
        "scenario": {
            "private": True,
            "metadata": {
                "title": "My Test",
                "description": "A test scenario",
                "author": "John Doe",
            },
        }
    }
    assert client.calls == [("/scenarios/4", {"json": expected_payload})]


def test_update_metadata_filters_and_nests_fields(
    dummy_client, fake_response, dummy_scenario
):
    """Test mixed direct fields and fields that should be auto-nested"""
    body = {"scenario": {"id": 5}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(5)
    metadata = {
        "private": True,  # Direct field
        "keep_compatible": True,  # Direct field
        "end_year": 2050,  # Direct field (settable)
        "area_code": "nl",  # Should be nested with warning (unsettable)
        "title": "Test Scenario",  # Should be nested with warning (unsettable)
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert len(result.errors) == 2
    expected_warnings = [
        "Field 'area_code' cannot be updated directly and has been added to nested metadata instead",
        "Field 'title' cannot be updated directly and has been added to nested metadata instead",
    ]
    for warning in expected_warnings:
        assert warning in result.errors

    expected_payload = {
        "scenario": {
            "private": True,
            "keep_compatible": True,
            "end_year": 2050,
            "metadata": {
                "area_code": "nl",
                "title": "Test Scenario",
            },
        }
    }
    assert client.calls == [("/scenarios/5", {"json": expected_payload})]


def test_update_metadata_empty_metadata(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 6}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(6)
    metadata = {}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/6", {"json": {"scenario": {}}})]


def test_update_metadata_with_nested_metadata_field(
    dummy_client, fake_response, dummy_scenario
):
    """Test when user explicitly provides a metadata field"""
    body = {"scenario": {"id": 7}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(7)
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
    assert client.calls == [("/scenarios/7", {"json": {"scenario": metadata}})]


def test_update_metadata_merges_explicit_and_auto_nested_metadata(
    dummy_client, fake_response, dummy_scenario
):
    """Test merging when user provides both explicit metadata and fields that should be auto-nested"""
    body = {"scenario": {"id": 8}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(8)
    metadata = {
        "private": True,
        "metadata": {"description": "Existing metadata", "tags": ["existing"]},
        "title": "Auto-nested title",
        "author": "Auto-nested author",
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert len(result.errors) == 1
    assert (
        "Field 'title' cannot be updated directly and has been added to nested metadata instead"
        in result.errors
    )

    expected_payload = {
        "scenario": {
            "private": True,
            "metadata": {
                "description": "Existing metadata",
                "tags": ["existing"],
                "title": "Auto-nested title",
                "author": "Auto-nested author",
            },
        }
    }
    assert client.calls == [("/scenarios/8", {"json": expected_payload})]


def test_update_metadata_replaces_non_dict_metadata(
    dummy_client, fake_response, dummy_scenario
):
    """Test that non-dict metadata field gets replaced when auto-nesting occurs"""
    body = {"scenario": {"id": 9}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(9)
    metadata = {
        "metadata": "not a dict",
        "title": "New title",
        "author": "New author",
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert len(result.errors) == 1
    assert (
        "Field 'title' cannot be updated directly and has been added to nested metadata instead"
        in result.errors
    )

    expected_payload = {
        "scenario": {"metadata": {"title": "New title", "author": "New author"}}
    }
    assert client.calls == [("/scenarios/9", {"json": expected_payload})]


def test_update_metadata_with_kwargs(dummy_client, fake_response, dummy_scenario):
    body = {"scenario": {"id": 10}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(10)
    metadata = {"title": "Updated Scenario"}

    result = UpdateMetadataRunner.run(client, scenario, metadata, timeout=30)
    assert result.success is True
    assert result.data == body
    assert len(result.errors) == 1
    assert (
        "Field 'title' cannot be updated directly and has been added to nested metadata instead"
        in result.errors
    )

    expected_payload = {"scenario": {"metadata": {"title": "Updated Scenario"}}}
    assert client.calls == [("/scenarios/10", {"json": expected_payload})]


def test_update_metadata_http_failure_422(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=422, text="Validation Error")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(11)
    metadata = {"private": "invalid_value"}

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
    scenario = dummy_scenario(12)
    metadata = {"private": True}

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_update_metadata_all_valid_direct_fields(
    dummy_client, fake_response, dummy_scenario
):
    """Test updating all valid direct META_KEYS fields"""
    body = {"scenario": {"id": 13}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(13)

    metadata = {
        "keep_compatible": True,
        "private": False,
        "source": "pyetm",
        "metadata": {"test": "data", "author": "test_user"},
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/13", {"json": {"scenario": metadata}})]


def test_update_metadata_only_nested_fields(
    dummy_client, fake_response, dummy_scenario
):
    """Test updating only fields that should be auto-nested"""
    body = {"scenario": {"id": 14}}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(14)

    metadata = {
        "title": "Test Scenario",
        "description": "A test",
        "author": "John Doe",
        "end_year": 2050,
    }

    result = UpdateMetadataRunner.run(client, scenario, metadata)
    assert result.success is True
    assert result.data == body
    assert len(result.errors) == 1
    expected_warning = "Field 'title' cannot be updated directly and has been added to nested metadata instead"
    assert expected_warning in result.errors

    expected_payload = {
        "scenario": {
            "end_year": 2050,
            "metadata": {
                "title": "Test Scenario",
                "description": "A test",
                "author": "John Doe",
            },
        }
    }
    assert client.calls == [("/scenarios/14", {"json": expected_payload})]
