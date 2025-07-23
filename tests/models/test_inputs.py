from pyetm.models import Inputs


def test_collection_from_json(inputs_json):
    input_collection = Inputs.from_json(inputs_json)

    # Check if valid!
    assert input_collection
    assert len(input_collection) == 4
    assert next(iter(input_collection)).key == "investment_costs_co2_ccs"
    assert len(input_collection.keys()) == 4


def test_to_dataframe(inputs_json):
    input_collection = Inputs.from_json(inputs_json)

    df_standard = input_collection.to_dataframe()
    df_with_defaults = input_collection.to_dataframe(columns=["user", "default"])

    assert "user" in df_standard.columns
    assert "user" in df_with_defaults.columns

    assert "default" not in df_standard.columns
    assert "default" in df_with_defaults.columns

    df_with_non_existing = input_collection.to_dataframe(columns="foo")

    assert df_with_non_existing["foo"].isnull().all()


def test_valid_update(inputs_json):
    input_collection = Inputs.from_json(inputs_json)

    # A good update
    warnings = input_collection.is_valid_update({"investment_costs_co2_ccs": 50.0})
    assert len(warnings) == 0

    # An update that will trigger validation
    warnings = input_collection.is_valid_update({"investment_costs_co2_ccs": "hello"})
    assert len(warnings) > 0
    assert "investment_costs_co2_ccs" in warnings
    assert warnings["investment_costs_co2_ccs"]["user"] == [
        "Input should be a valid number, unable to parse string as a number"
    ]

    # An update of a non existent key
    warnings = input_collection.is_valid_update({"hello": "hello"})
    assert len(warnings) > 0
    assert "hello" in warnings
    assert warnings["hello"] == "Key does not exist"
