from pyetm.models import Inputs


def test_collection_from_json(input_collection_json):
    input_collection = Inputs.from_json(input_collection_json)

    # Check if valid!
    assert input_collection
    assert len(input_collection) == 4
    assert next(iter(input_collection)).key == "investment_costs_co2_ccs"
    assert len(input_collection.keys()) == 4


def test_to_dataframe(input_collection_json):
    input_collection = Inputs.from_json(input_collection_json)

    df_standard = input_collection.to_dataframe()
    df_with_defaults = input_collection.to_dataframe(values=['user', 'default'])

    assert 'user' in df_standard.columns
    assert 'user' in df_with_defaults.columns

    assert 'default' not in df_standard.columns
    assert 'default' in df_with_defaults.columns

    df_with_non_existing = input_collection.to_dataframe(values='foo')

    assert df_with_non_existing['foo'].isnull().all()
