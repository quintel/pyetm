from pyetm.models.sortables import Sortable, Sortables


def test_collection_from_json(valid_sortable_collection_json):
    coll = Sortables.from_json(valid_sortable_collection_json)

    assert coll
    # 1 (forecast_storage) + 3 (heat_network subtypes) + 1 (hydrogen_supply) = 5
    assert len(coll) == 5

    first = next(iter(coll))
    assert isinstance(first, Sortable)
    assert first.type == "forecast_storage"
    assert first.order == ["fs1", "fs2"]
    assert first.subtype is None

    assert coll.keys() == [
        "forecast_storage",
        "heat_network",
        "heat_network",
        "heat_network",
        "hydrogen_supply",
    ]


def test_names_method(valid_sortable_collection_json):
    coll = Sortables.from_json(valid_sortable_collection_json)

    names = coll.names()
    expected_names = [
        "forecast_storage",
        "heat_network_lt",
        "heat_network_mt",
        "heat_network_ht",
        "hydrogen_supply",
    ]
    assert set(names) == set(expected_names)


def test_as_dict_roundtrip(valid_sortable_collection_json):
    coll = Sortables.from_json(valid_sortable_collection_json)
    rebuilt = coll.as_dict()

    assert rebuilt == valid_sortable_collection_json


def test_to_dataframe(valid_sortable_collection_json):
    coll = Sortables.from_json(valid_sortable_collection_json)

    df = coll._to_dataframe()
    assert df["forecast_storage"][0] == "fs1"
    assert df["heat_network_lt"][0] == "hn1"


def test_is_valid_update():
    """Test the is_valid_update method"""
    coll = Sortables.from_json(
        {"forecast_storage": ["a", "b"], "heat_network": {"lt": ["c", "d"]}}
    )

    # Valid updates
    valid_updates = {"forecast_storage": ["x", "y"], "heat_network_lt": ["z"]}
    warnings = coll.is_valid_update(valid_updates)
    assert warnings == {}

    # Invalid updates - non-existent sortable
    invalid_updates = {"nonexistent": ["a", "b"], "forecast_storage": ["valid"]}
    warnings = coll.is_valid_update(invalid_updates)
    assert "forecast_storage" not in warnings

    # Invalid updates - validation errors
    invalid_order_updates = {"forecast_storage": [1, 2, 2]}
    warnings = coll.is_valid_update(invalid_order_updates)
    assert "forecast_storage" in warnings
    assert len(warnings["forecast_storage"]) > 0


def test_update_method():
    coll = Sortables.from_json(
        {"forecast_storage": ["a", "b"], "heat_network": {"lt": ["c", "d"]}}
    )

    updates = {"forecast_storage": ["x", "y", "z"], "heat_network_lt": ["w"]}
    coll.update(updates)

    sortable_by_name = {s.name(): s for s in coll.sortables}
    assert sortable_by_name["forecast_storage"].order == ["x", "y", "z"]
    assert sortable_by_name["heat_network_lt"].order == ["w"]


def test_validation_duplicate_sortable_names():
    sortables_list = [
        Sortable(type="forecast_storage", order=["a"]),
        Sortable(type="forecast_storage", order=["b"]),  # Duplicate name
    ]

    # This should create warnings about duplicate names
    coll = Sortables(sortables=sortables_list)
    assert len(coll.warnings) > 0
    # Flatten all warning messages to search
    all_warnings = []
    for warning_list in coll.warnings.values():
        all_warnings.extend(warning_list)
    warning_text = " ".join(all_warnings)
    assert "duplicate" in warning_text.lower()


def test_validation_heat_network_consistency():
    sortables_list = [
        Sortable(type="heat_network", order=["a"], subtype="lt"),  # Valid
        Sortable(type="heat_network", order=["b"]),  # Invalid - no subtype
    ]

    coll = Sortables(sortables=sortables_list)
    assert len(coll.warnings) > 0


def test_collection_merges_individual_warnings():
    data_with_issues = {"heat_network": ["no_subtype"]}  # This will cause warnings

    coll = Sortables.from_json(data_with_issues)
    assert len(coll.warnings) > 0
