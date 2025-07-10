from pyetm.models import Inputs


def test_collection_from_json(input_collection_json):
    input_collection = Inputs.from_json(input_collection_json)

    # Check if valid!
    assert input_collection
    assert len(input_collection) == 4
    assert next(iter(input_collection)).key == "investment_costs_co2_ccs"
    assert len(input_collection.keys()) == 4
