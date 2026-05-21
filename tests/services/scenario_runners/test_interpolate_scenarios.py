from pyetm.services.scenario_runners.interpolate_scenarios import (
    InterpolateScenariosRunner,
)


def test_interpolate_success_two_scenarios(dummy_client, fake_response):
    """Test batch interpolating between two scenarios"""
    body = [
        {
            "id": 88881,
            "area_code": "nl",
            "end_year": 2040,
            "start_year": 2023,
            "title": "Interpolated to 2040",
        }
    ]
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 67890], end_years=[2040]
    )

    assert result.success is True
    assert result.data == body
    assert len(result.data) == 1
    assert result.data[0]["end_year"] == 2040
    assert result.errors == []
    assert client.calls == [
        (
            "/scenarios/interpolate",
            {"json": {"scenario_ids": [12345, 67890], "end_years": [2040]}},
        )
    ]


def test_interpolate_success_three_scenarios(dummy_client, fake_response):
    """Test batch interpolating with three scenarios and two target years"""
    body = [
        {
            "id": 88881,
            "area_code": "nl",
            "end_year": 2040,
            "start_year": 2023,
            "title": "Interpolated to 2040",
        },
        {
            "id": 88882,
            "area_code": "nl",
            "end_year": 2060,
            "start_year": 2023,
            "title": "Interpolated to 2060",
        },
    ]
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 45678, 67890], end_years=[2040, 2060]
    )

    assert result.success is True
    assert result.data == body
    assert len(result.data) == 2
    assert result.data[0]["end_year"] == 2040
    assert result.data[1]["end_year"] == 2060
    assert result.errors == []


def test_interpolate_unordered_scenario_ids(dummy_client, fake_response):
    """Test that unordered scenario IDs are handled (server sorts by end_year)"""
    body = [
        {
            "id": 88881,
            "area_code": "nl",
            "end_year": 2040,
            "start_year": 2023,
            "title": "Interpolated to 2040",
        }
    ]
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    # Deliberately pass scenario IDs in non-chronological order
    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[67890, 12345, 45678], end_years=[2040]
    )

    assert result.success is True
    assert result.data == body
    # Verify the payload was sent as-is (server will sort)
    assert client.calls[0][1]["json"]["scenario_ids"] == [67890, 12345, 45678]


def test_interpolate_failure_too_few_scenarios(dummy_client, fake_response):
    """Test handling of validation error: fewer than 2 scenarios"""
    error_response = {"errors": {"scenario_ids": ["must contain at least 2 scenarios"]}}
    response = fake_response(ok=False, status_code=422, json_data=error_response)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345], end_years=[2040]
    )

    assert result.success is False
    assert result.data is None
    assert len(result.errors) > 0
    assert "scenario_ids: must contain at least 2 scenarios" == result.errors[0]


def test_interpolate_failure_empty_end_years(dummy_client, fake_response):
    """Test handling of validation error: empty end_years"""
    error_response = {"errors": {"end_years": ["must be filled"]}}
    response = fake_response(ok=False, status_code=422, json_data=error_response)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 67890], end_years=[]
    )

    assert result.success is False
    assert result.data is None
    assert len(result.errors) > 0


def test_interpolate_failure_missing_scenario(dummy_client, fake_response):
    """Test handling of scenario not found error"""
    error_response = {"errors": {"scenario_ids": ["scenarios not found: 999999"]}}
    response = fake_response(ok=False, status_code=422, json_data=error_response)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 999999], end_years=[2040]
    )

    assert result.success is False
    assert result.data is None
    assert len(result.errors) > 0


def test_interpolate_failure_mismatched_area_codes(dummy_client, fake_response):
    """Test handling of validation error: scenarios with different area codes"""
    error_response = {
        "errors": {"scenario_ids": ["all scenarios must have the same area code"]}
    }
    response = fake_response(ok=False, status_code=422, json_data=error_response)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 67890], end_years=[2040]
    )

    assert result.success is False
    assert result.data is None
    assert len(result.errors) > 0


def test_interpolate_failure_invalid_target_year(dummy_client, fake_response):
    """Test handling of validation error: target year out of bounds"""
    error_response = {
        "errors": {"end_years": ["2055 must be prior to the latest scenario end year"]}
    }
    response = fake_response(ok=False, status_code=422, json_data=error_response)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 67890], end_years=[2055]
    )

    assert result.success is False
    assert result.data is None
    assert len(result.errors) > 0


def test_interpolate_failure_scaled_scenario(dummy_client, fake_response):
    """Test handling of validation error: cannot interpolate scaled scenarios"""
    error_response = {
        "errors": {"scenario_ids": ["cannot interpolate scaled scenario 12345"]}
    }
    response = fake_response(ok=False, status_code=422, json_data=error_response)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 67890], end_years=[2040]
    )

    assert result.success is False
    assert result.data is None
    assert len(result.errors) > 0


def test_interpolate_connection_error(dummy_client):
    """Test handling of connection errors"""
    client = dummy_client(ConnectionError("Connection failed"), method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 67890], end_years=[2040]
    )

    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_interpolate_payload_structure(dummy_client, fake_response):
    """Test that the payload is correctly structured for the API"""
    body = [
        {"id": 77771, "area_code": "de", "end_year": 2035},
        {"id": 77772, "area_code": "de", "end_year": 2045},
    ]
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    InterpolateScenariosRunner.run(
        client, scenario_ids=[11111, 22222, 33333], end_years=[2035, 2045]
    )

    # Verify the exact payload structure
    expected_call = (
        "/scenarios/interpolate",
        {"json": {"scenario_ids": [11111, 22222, 33333], "end_years": [2035, 2045]}},
    )
    assert client.calls == [expected_call]


def test_interpolate_multiple_target_years_different_order(dummy_client, fake_response):
    """Test interpolating to multiple target years in non-sorted order"""
    body = [
        {"id": 88881, "area_code": "nl", "end_year": 2035},
        {"id": 88882, "area_code": "nl", "end_year": 2060},
    ]
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    # Pass target years in non-sorted order (server will sort them)
    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 67890, 99999], end_years=[2060, 2035]
    )

    assert result.success is True
    assert len(result.data) == 2
    # Verify payload sent with unsorted years (server handles sorting)
    assert client.calls[0][1]["json"]["end_years"] == [2060, 2035]


def test_interpolate_with_kwargs(dummy_client, fake_response):
    """Test that additional kwargs are passed through"""
    body = [{"id": 66666, "area_code": "nl", "end_year": 2040}]
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="post")

    result = InterpolateScenariosRunner.run(
        client, scenario_ids=[12345, 67890], end_years=[2040], timeout=30
    )

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify basic structure
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios/interpolate"
    assert client.calls[0][1]["json"] == {
        "scenario_ids": [12345, 67890],
        "end_years": [2040],
    }
