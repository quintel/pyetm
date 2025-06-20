import pytest
from pyetm.models import BalancedInputCollection, BalancedInput


@pytest.fixture
def balanced_values_json():
    return {
        "a_float": 1.23,
        "a_string": "choice",
        "a_bool": False,
        "another_int": 42,
    }


def test_collection_from_json(balanced_values_json):
    """
    BalancedInputCollection.from_json should
    1) produce one BalancedInput per key
    2) preserve key and value exactly
    """
    bic = BalancedInputCollection.from_json(balanced_values_json)

    # 1) length matches
    assert len(bic.inputs) == len(balanced_values_json)

    # 2) each element is a BalancedInput with correct key/value
    got = {bi.key: bi.value for bi in bic.inputs}
    assert got == balanced_values_json

    # And that every item is truly a BalancedInput
    for bi in bic.inputs:
        assert isinstance(bi, BalancedInput)


def test_collection_dict_export(balanced_values_json):
    """
    The Pydantic dict() export should include exactly the 'inputs' list
    with key/value dicts in the same order as the input .items() iteration.
    """
    bic = BalancedInputCollection.from_json(balanced_values_json)
    exported = bic.model_dump()

    # Must have only one top‐level key: "inputs"
    assert set(exported.keys()) == {"inputs"}

    # Each entry under 'inputs' should be a mapping with key/value
    assert all(set(item.keys()) == {"key", "value"} for item in exported["inputs"])

    # And round‐trip values are preserved
    roundtrip = {item["key"]: item["value"] for item in exported["inputs"]}
    assert roundtrip == balanced_values_json
