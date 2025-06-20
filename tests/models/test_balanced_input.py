import pytest
from pyetm.models.balanced_input import BalancedInput


@pytest.mark.parametrize(
    "kv, expected_key, expected_value",
    [
        (("foo_float", 3.14), "foo_float", 3.14),
        (("bar_str", "medium"), "bar_str", "medium"),
        (("baz_bool", True), "baz_bool", True),
        (("zero_int", 0), "zero_int", 0),
    ],
)
def test_balanced_input_from_json(kv, expected_key, expected_value):
    bi = BalancedInput.from_json(kv)
    assert bi.key == expected_key
    assert bi.value == expected_value
    assert bi.model_dump() == {"key": expected_key, "value": expected_value}
