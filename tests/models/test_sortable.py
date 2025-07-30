import pytest
from pyetm.models.sortables import Sortable


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
    all_warnings = []
    for warning_list in sortable.warnings.values():
        all_warnings.extend(warning_list)
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
    all_warnings = []
    for warning_list in sortable.warnings.values():
        all_warnings.extend(warning_list)
    warning_text = " ".join(all_warnings)
    assert "duplicate" in warning_text.lower()


def test_validation_order_too_long():
    """Test that orders with too many items generate warnings"""
    long_order = list(range(15))  # More than 10 items
    payload = ("forecast_storage", long_order)
    result = list(Sortable.from_json(payload))

    assert len(result) == 1
    sortable = result[0]
    assert sortable.type == "forecast_storage"
    assert sortable.order == long_order

    # Should have validation warning about length
    assert len(sortable.warnings) > 0
    all_warnings = []
    for warning_list in sortable.warnings.values():
        all_warnings.extend(warning_list)
    warning_text = " ".join(all_warnings)
    assert "more than 10 items" in warning_text


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
    all_warnings = []
    for warning_list in sortable.warnings.values():
        all_warnings.extend(warning_list)
    warning_text = " ".join(all_warnings)
    # Could be either unexpected payload warning or validation warning
    assert (
        "Unexpected payload" in warning_text
        or "heat_network type requires a subtype" in warning_text
    )
    assert str(payload[1]) in warning_text


def test_is_valid_update():
    """Test the is_valid_update method"""
    sortable = Sortable(type="forecast_storage", order=[1, 2, 3])

    # Valid update - no warnings
    warnings = sortable.is_valid_update([4, 5, 6])
    assert warnings == {}

    # Invalid update - duplicates
    warnings = sortable.is_valid_update([1, 2, 2])
    assert len(warnings) > 0
    all_warnings = []
    for warning_list in warnings.values():
        all_warnings.extend(warning_list)
    warning_text = " ".join(all_warnings)
    assert "duplicate" in warning_text.lower()

    # Invalid update - too long
    warnings = sortable.is_valid_update(list(range(15)))
    assert len(warnings) > 0
    all_warnings = []
    for warning_list in warnings.values():
        all_warnings.extend(warning_list)
    warning_text = " ".join(all_warnings)
    assert "more than 10 items" in warning_text


def test_name_method():
    sortable1 = Sortable(type="forecast_storage", order=[1, 2])
    assert sortable1.name() == "forecast_storage"

    sortable2 = Sortable(type="heat_network", subtype="lt", order=[3, 4])
    assert sortable2.name() == "heat_network_lt"
