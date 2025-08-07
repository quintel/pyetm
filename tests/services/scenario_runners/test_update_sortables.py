from pyetm.services.scenario_runners.update_sortables import UpdateSortablesRunner


def test_update_sortables_success(dummy_client, fake_response, dummy_scenario):
    """Test successful sortables update"""
    body = {"order": ["item_1", "item_2", "item_3"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(1)
    order = ["item_1", "item_2", "item_3"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/1/user_sortables/demand", {"json": {"order": order}})
    ]


def test_update_sortables_with_subtype(dummy_client, fake_response, dummy_scenario):
    """Test sortables update with subtype parameter"""
    body = {"order": ["heat_item_1", "heat_item_2"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(2)
    order = ["heat_item_1", "heat_item_2"]

    result = UpdateSortablesRunner.run(
        client, scenario, "heat_network", order, subtype="lt"
    )

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        (
            "/scenarios/2/user_sortables/heat_network?subtype=lt",
            {"json": {"order": order}},
        )
    ]


def test_update_sortables_heat_network_mt_subtype(
    dummy_client, fake_response, dummy_scenario
):
    """Test heat network sortables update with medium temperature subtype"""
    body = {"order": ["mt_source_1", "mt_source_2", "mt_source_3"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(3)
    order = ["mt_source_1", "mt_source_2", "mt_source_3"]

    result = UpdateSortablesRunner.run(
        client, scenario, "heat_network", order, subtype="mt"
    )

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        (
            "/scenarios/3/user_sortables/heat_network?subtype=mt",
            {"json": {"order": order}},
        )
    ]


def test_update_sortables_heat_network_ht_subtype(
    dummy_client, fake_response, dummy_scenario
):
    """Test heat network sortables update with high temperature subtype"""
    body = {"order": ["ht_source_1", "ht_source_2"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(4)
    order = ["ht_source_1", "ht_source_2"]

    result = UpdateSortablesRunner.run(
        client, scenario, "heat_network", order, subtype="ht"
    )

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        (
            "/scenarios/4/user_sortables/heat_network?subtype=ht",
            {"json": {"order": order}},
        )
    ]


def test_update_sortables_empty_order(dummy_client, fake_response, dummy_scenario):
    """Test sortables update with empty order list"""
    body = {"order": []}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(5)
    order = []

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/5/user_sortables/demand", {"json": {"order": []}})
    ]


def test_update_sortables_single_item(dummy_client, fake_response, dummy_scenario):
    """Test sortables update with single item in order"""
    body = {"order": ["single_item"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(6)
    order = ["single_item"]

    result = UpdateSortablesRunner.run(client, scenario, "supply", order)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/6/user_sortables/supply", {"json": {"order": order}})
    ]


def test_update_sortables_numeric_order_items(
    dummy_client, fake_response, dummy_scenario
):
    """Test sortables update with numeric items in order"""
    body = {"order": [1, 2, 3, 4]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(7)
    order = [1, 2, 3, 4]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/7/user_sortables/demand", {"json": {"order": order}})
    ]


def test_update_sortables_mixed_type_order_items(
    dummy_client, fake_response, dummy_scenario
):
    """Test sortables update with mixed type items in order"""
    body = {"order": ["item_1", 2, "item_3", 4]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(8)
    order = ["item_1", 2, "item_3", 4]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/8/user_sortables/demand", {"json": {"order": order}})
    ]


def test_update_sortables_with_kwargs(dummy_client, fake_response, dummy_scenario):
    """Test sortables update with additional kwargs"""
    body = {"order": ["item_1", "item_2"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(9)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order, timeout=30)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    # Verify the basic structure - kwargs handling might vary
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios/9/user_sortables/demand"
    assert client.calls[0][1]["json"] == {"order": order}


def test_update_sortables_large_scenario_id(
    dummy_client, fake_response, dummy_scenario
):
    """Test with large scenario ID"""
    body = {"order": ["item_1", "item_2"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(999999)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is True
    assert client.calls[0][0] == "/scenarios/999999/user_sortables/demand"


def test_update_sortables_http_failure_422(dummy_client, fake_response, dummy_scenario):
    """Test HTTP 422 validation error"""
    response = fake_response(ok=False, status_code=422, text="Invalid sortable order")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(10)
    order = ["invalid_item"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["422: Invalid sortable order"]


def test_update_sortables_http_failure_404(dummy_client, fake_response, dummy_scenario):
    """Test HTTP 404 scenario not found"""
    response = fake_response(ok=False, status_code=404, text="Scenario not found")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(999)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Scenario not found"]


def test_update_sortables_http_failure_400(dummy_client, fake_response, dummy_scenario):
    """Test HTTP 400 bad request"""
    response = fake_response(
        ok=False, status_code=400, text="Bad request - invalid sortable type"
    )
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(11)
    order = ["item_1"]

    result = UpdateSortablesRunner.run(client, scenario, "invalid_type", order)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["400: Bad request - invalid sortable type"]


def test_update_sortables_http_failure_403(dummy_client, fake_response, dummy_scenario):
    """Test HTTP 403 forbidden access"""
    response = fake_response(
        ok=False, status_code=403, text="Forbidden - access denied"
    )
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(12)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["403: Forbidden - access denied"]


def test_update_sortables_http_failure_500(dummy_client, fake_response, dummy_scenario):
    """Test HTTP 500 internal server error"""
    response = fake_response(ok=False, status_code=500, text="Internal Server Error")
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(13)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is False
    assert result.data is None
    assert result.errors == ["500: Internal Server Error"]


def test_update_sortables_connection_error(dummy_client, dummy_scenario):
    """Test connection error handling"""
    client = dummy_client(ConnectionError("Connection failed"), method="put")
    scenario = dummy_scenario(14)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_update_sortables_permission_error(dummy_client, dummy_scenario):
    """Test permission error handling"""
    client = dummy_client(PermissionError("Access denied"), method="put")
    scenario = dummy_scenario(15)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is False
    assert result.data is None
    assert any("Access denied" in err for err in result.errors)


def test_update_sortables_value_error(dummy_client, dummy_scenario):
    """Test value error handling"""
    client = dummy_client(ValueError("Invalid value provided"), method="put")
    scenario = dummy_scenario(16)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is False
    assert result.data is None
    assert any("Invalid value provided" in err for err in result.errors)


def test_update_sortables_generic_exception(dummy_client, dummy_scenario):
    """Test generic exception handling"""
    client = dummy_client(RuntimeError("Unexpected error"), method="put")
    scenario = dummy_scenario(17)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is False
    assert result.data is None
    assert any("Unexpected error" in err for err in result.errors)


def test_update_sortables_payload_structure(
    dummy_client, fake_response, dummy_scenario
):
    """Test that the payload is correctly structured for the API"""
    body = {"order": ["a", "b", "c", "d"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(18)
    order = ["a", "b", "c", "d"]

    UpdateSortablesRunner.run(client, scenario, "demand", order)

    # Verify the exact payload structure
    expected_call = (
        "/scenarios/18/user_sortables/demand",
        {"json": {"order": ["a", "b", "c", "d"]}},
    )
    assert client.calls == [expected_call]


def test_update_sortables_url_construction_no_subtype(
    dummy_client, fake_response, dummy_scenario
):
    """Test URL construction without subtype"""
    body = {"order": ["item_1"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(19)
    order = ["item_1"]

    UpdateSortablesRunner.run(client, scenario, "supply", order)

    assert client.calls[0][0] == "/scenarios/19/user_sortables/supply"


def test_update_sortables_url_construction_with_subtype(
    dummy_client, fake_response, dummy_scenario
):
    """Test URL construction with subtype"""
    body = {"order": ["item_1"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(20)
    order = ["item_1"]

    UpdateSortablesRunner.run(client, scenario, "heat_network", order, subtype="mt")

    assert client.calls[0][0] == "/scenarios/20/user_sortables/heat_network?subtype=mt"


def test_update_sortables_different_sortable_types(
    dummy_client, fake_response, dummy_scenario
):
    """Test different sortable types"""
    body = {"order": ["item_1", "item_2"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(21)
    order = ["item_1", "item_2"]

    # Test various sortable types
    sortable_types = ["demand", "supply", "heat_network", "storage", "conversion"]

    for i, sortable_type in enumerate(sortable_types):
        scenario_obj = dummy_scenario(21 + i)
        result = UpdateSortablesRunner.run(client, scenario_obj, sortable_type, order)

        assert result.success is True
        expected_url = f"/scenarios/{21 + i}/user_sortables/{sortable_type}"
        assert client.calls[i][0] == expected_url


def test_update_sortables_subtype_none_explicitly(
    dummy_client, fake_response, dummy_scenario
):
    """Test with subtype explicitly set to None"""
    body = {"order": ["item_1", "item_2"]}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(22)
    order = ["item_1", "item_2"]

    result = UpdateSortablesRunner.run(client, scenario, "demand", order, subtype=None)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/22/user_sortables/demand", {"json": {"order": order}})
    ]


def test_update_sortables_complex_order_data(
    dummy_client, fake_response, dummy_scenario
):
    """Test with complex order data including dictionaries"""
    order = [
        {"id": 1, "name": "item_1"},
        {"id": 2, "name": "item_2"},
        {"id": 3, "name": "item_3"},
    ]
    body = {"order": order}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="put")
    scenario = dummy_scenario(23)

    result = UpdateSortablesRunner.run(client, scenario, "demand", order)

    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [
        ("/scenarios/23/user_sortables/demand", {"json": {"order": order}})
    ]
