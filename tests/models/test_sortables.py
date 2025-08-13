import pytest
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
    assert len(warnings) == 0

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
    all_warnings = [w.message for w in coll.warnings]
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


@pytest.mark.parametrize(
    "payload, expected_type, expected_order, expected_subtype",
    [
        # flat list → one Sortable, no subtype
        (
            ("forecast_storage", ["a", "b", "c"]),
            "forecast_storage",
            ["a", "b", "c"],
            None,
        ),
        (("hydrogen_supply", ["x", "y"]), "hydrogen_supply", ["x", "y"], None),
    ],
)
def test_from_json_with_list(payload, expected_type, expected_order, expected_subtype):
    result = list(Sortable.from_json(payload))
    assert isinstance(result, list) and len(result) == 1
    sortable = result[0]
    assert sortable.type == expected_type
    assert sortable.order == expected_order
    assert sortable.subtype is expected_subtype


def test_from_json_with_list_heat_network_generates_warning():
    """heat_network without subtype should generate a validation warning"""
    payload = ("heat_network", ["x", "y"])
    result = list(Sortable.from_json(payload))

    assert len(result) == 1
    sortable = result[0]
    assert sortable.type == "heat_network"
    assert sortable.order == ["x", "y"]
    assert sortable.subtype is None  # No subtype provided

    # Should have validation warning about missing subtype
    assert len(sortable.warnings) > 0
    all_warnings = [w.message for w in sortable.warnings]
    warning_text = " ".join(all_warnings)
    assert "heat_network type requires a subtype" in warning_text


def test_from_json_with_dict():
    # nested dict → one Sortable per subtype
    payload = ("heat_network", {"lt": [1, 2], "mt": [3, 4], "ht": []})
    result = list(Sortable.from_json(payload))

    assert isinstance(result, list) and len(result) == 3

    got = {(s.type, s.subtype, tuple(s.order)) for s in result}
    expected = {
        ("heat_network", "lt", (1, 2)),
        ("heat_network", "mt", (3, 4)),
        ("heat_network", "ht", ()),
    }
    assert got == expected

    # These should not have warnings since they have proper subtypes
    for sortable in result:
        assert len(sortable.warnings) == 0


def test_validation_duplicate_order_items():
    """Test that duplicate items in order generate warnings"""
    payload = ("forecast_storage", [1, 2, 2, 3])
    result = list(Sortable.from_json(payload))

    assert len(result) == 1
    sortable = result[0]
    assert sortable.type == "forecast_storage"
    assert sortable.order == [1, 2, 2, 3]

    # Should have validation warning about duplicates
    assert len(sortable.warnings) > 0
    all_warnings = [w.message for w in sortable.warnings]
    warning_text = " ".join(all_warnings)
    assert "duplicate" in warning_text.lower()


def test_validation_order_too_long():
    """Test that orders with too many items generate warnings"""
    long_order = list(range(20))  # More than 17 items
    payload = ("forecast_storage", long_order)
    result = list(Sortable.from_json(payload))

    assert len(result) == 1
    sortable = result[0]
    assert sortable.type == "forecast_storage"
    assert sortable.order == long_order

    # Should have validation warning about length
    assert len(sortable.warnings) > 0
    all_warnings = [w.message for w in sortable.warnings]
    warning_text = " ".join(all_warnings)
    assert "more than 17 items" in warning_text


@pytest.mark.parametrize(
    "payload",
    [
        ("forecast_storage", None),
        ("heat_network", 123),
        ("foo", object()),
    ],
)
def test_from_json_creates_warning_on_invalid(payload):
    """Test that invalid payloads create sortables with warnings instead of raising exceptions"""
    result = list(Sortable.from_json(payload))

    # Should always yield exactly one sortable
    assert len(result) == 1
    sortable = result[0]

    # Should have the correct type and empty order
    assert sortable.type == payload[0]
    assert sortable.order == []
    assert sortable.subtype is None

    assert hasattr(sortable, "warnings")
    assert len(sortable.warnings) > 0
    all_warnings = [w.message for w in sortable.warnings]
    warning_text = " ".join(all_warnings)
    # Could be either unexpected payload warning or validation warning
    assert (
        "Unexpected payload" in warning_text
        or "heat_network type requires a subtype" in warning_text
    )
    assert str(payload[1]) in warning_text


def test_sortable_is_valid_update():
    """Test the is_valid_update method"""
    sortable = Sortable(type="forecast_storage", order=[1, 2, 3])

    # Valid update - no warnings
    warnings = sortable.is_valid_update([4, 5, 6])
    assert len(warnings) == 0

    # Invalid update - duplicates
    warnings = sortable.is_valid_update([1, 2, 2])
    assert len(warnings) > 0
    all_warnings = [w.message for w in warnings]
    warning_text = " ".join(all_warnings)
    assert "duplicate" in warning_text.lower()

    # Invalid update - too long
    warnings = sortable.is_valid_update(list(range(18)))
    assert len(warnings) > 0
    all_warnings = [w.message for w in warnings]
    warning_text = " ".join(all_warnings)
    assert "more than 17 items" in warning_text


def test_name_method():
    sortable1 = Sortable(type="forecast_storage", order=[1, 2])
    assert sortable1.name() == "forecast_storage"

    sortable2 = Sortable(type="heat_network", subtype="lt", order=[3, 4])
    assert sortable2.name() == "heat_network_lt"
