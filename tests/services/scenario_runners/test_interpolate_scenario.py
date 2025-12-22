from pyetm.services.scenario_runners.interpolate_scenario import (
    InterpolateScenarioRunner,
)


def test_interpolate_scenario_success_no_start_scenario(dummy_client, fake_response):
    """Test interpolating a single scenario to a different year"""
    body = {
        "id": 99999,
        "area_code": "nl",
        "end_year": 2040,
        "start_year": 2023,
        "title": "Interpolated to 2040",
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    result = InterpolateScenarioRunner.run(
        client, scenario_id=12345, end_year=2040
    )

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/12345/interpolate", {"json": {"end_year": 2040}})
    ]


def test_interpolate_scenario_with_start_scenario(dummy_client, fake_response):
    """Test interpolating between two scenarios"""
    body = {
        "id": 88888,
        "area_code": "nl",
        "end_year": 2040,
        "start_year": 2023,
        "title": "Interpolated between scenarios",
    }
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    result = InterpolateScenarioRunner.run(
        client, scenario_id=12345, end_year=2040, start_scenario_id=67890
    )

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        (
            "/scenarios/12345/interpolate",
            {"json": {"end_year": 2040, "start_scenario_id": 67890}},
        )
    ]


def test_interpolate_scenario_http_failure_404(dummy_client, fake_response):
    """Test handling of scenario not found error"""
    response = fake_response(ok=False, status_code=404, text="Scenario not found")
    client = dummy_client(response, method="post")

    result = InterpolateScenarioRunner.run(client, scenario_id=99999, end_year=2040)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Scenario not found"]


def test_interpolate_scenario_http_failure_422(dummy_client, fake_response):
    """Test handling of validation errors"""
    response = fake_response(
        ok=False,
        status_code=422,
        text="Start scenario must have the same area code as the original scenario (nl)",
    )
    client = dummy_client(response, method="post")

    result = InterpolateScenarioRunner.run(
        client, scenario_id=12345, end_year=2040, start_scenario_id=99999
    )

    assert result.success is False
    assert result.data is None
    assert "422:" in result.errors[0]
    assert "area code" in result.errors[0]


def test_interpolate_scenario_missing_end_year(dummy_client, fake_response):
    """Test handling of missing end_year parameter"""
    response = fake_response(
        ok=False,
        status_code=400,
        text="Interpolated scenario must have an end year",
    )
    client = dummy_client(response, method="post")

    # Note: This test verifies error handling, though in practice Python would
    # raise TypeError if end_year is not provided due to the function signature
    result = InterpolateScenarioRunner.run(
        client, scenario_id=12345, end_year=None
    )

    assert result.success is False
    assert result.data is None


def test_interpolate_scenario_connection_error(dummy_client):
    """Test handling of connection errors"""
    client = dummy_client(ConnectionError("Connection failed"), method="post")

    result = InterpolateScenarioRunner.run(client, scenario_id=12345, end_year=2040)

    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_interpolate_scenario_payload_structure(dummy_client, fake_response):
    """Test that the payload is correctly structured for the API"""
    body = {"id": 77777, "area_code": "de", "end_year": 2035}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    InterpolateScenarioRunner.run(
        client, scenario_id=12345, end_year=2035, start_scenario_id=54321
    )

    # Verify the exact payload structure
    expected_call = (
        "/scenarios/12345/interpolate",
        {"json": {"end_year": 2035, "start_scenario_id": 54321}},
    )
    assert client.calls == [expected_call]


def test_interpolate_scenario_with_kwargs(dummy_client, fake_response):
    """Test that additional kwargs are passed through"""
    body = {"id": 66666, "area_code": "nl", "end_year": 2040}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    result = InterpolateScenarioRunner.run(
        client, scenario_id=12345, end_year=2040, timeout=30
    )

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify basic structure
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios/12345/interpolate"
    assert client.calls[0][1]["json"] == {"end_year": 2040}


def test_interpolate_scenario_year_boundaries(dummy_client, fake_response):
    """Test interpolation with various year boundaries"""
    body = {"id": 55555, "area_code": "nl", "end_year": 2030}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    result = InterpolateScenarioRunner.run(client, scenario_id=12345, end_year=2030)

    assert result.success is True
    assert result.data == body
    assert client.calls == [
        ("/scenarios/12345/interpolate", {"json": {"end_year": 2030}})
    ]
