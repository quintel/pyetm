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
        # flat list for heat_network treated the same
        (("heat_network", ["x", "y"]), "heat_network", ["x", "y"], None),
    ],
)
def test_from_json_with_list(payload, expected_type, expected_order, expected_subtype):
    result = list(Sortable.from_json(payload))
    assert isinstance(result, list) and len(result) == 1
    sortable = result[0]
    assert sortable.type == expected_type
    assert sortable.order == expected_order
    assert sortable.subtype is expected_subtype


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

    # Should have a warning about the unexpected payload
    assert hasattr(sortable, "warnings")
    assert 'type' in sortable.warnings
    assert "Unexpected payload" in sortable.warnings['type'][0]
    assert str(payload[1]) in sortable.warnings['type'][0]
