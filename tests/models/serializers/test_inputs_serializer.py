import pytest
import pandas as pd
from pyetm.models.serializers.base_serializer import SerializationMode
from pyetm.models.serializers.input_serializer import InputsSerializer


class TestInputsSerializer:
    """Test InputsSerializer using existing fixtures"""

    def test_serialize_to_dataframe_matches_existing_method(self, inputs_model):
        """Test that serializer produces same output as existing to_dataframe()"""
        # Get results from both methods
        existing_df = inputs_model.to_dataframe()
        serialized_df = inputs_model.serialize_to_dataframe()

        # Set the index name on existing_df to match serializer behavior
        existing_df.index.name = "input"

        # They should be identical
        pd.testing.assert_frame_equal(existing_df, serialized_df)

        # Verify expected structure
        assert serialized_df.index.name == "input"
        assert list(serialized_df.columns) == ["unit", "value", "default"]
        assert len(serialized_df) == 4  # from input_collection_json fixture

    def test_serialize_json_mode(self, serializer, inputs_model):
        """Test JSON serialization mode"""
        result = serializer.serialize(inputs_model, SerializationMode.JSON)

        assert isinstance(result, dict)
        assert "inputs" in result
        assert "count" in result
        assert result["count"] == 4
        assert len(result["inputs"]) == 4

    def test_serialize_dict_export_mode(self, serializer, inputs_model):
        """Test dict export mode"""
        result = serializer.serialize(inputs_model, SerializationMode.DICT_EXPORT)

        assert isinstance(result, dict)
        assert len(result) == 4

        # Check that input keys are preserved
        expected_keys = {
            "investment_costs_co2_ccs",
            "transport_car_fuel_type",
            "has_electricity_storage",
            "legacy_input",
        }
        assert set(result.keys()) == expected_keys

    def test_unsupported_mode_raises_error(self, serializer, inputs_model):
        """Test that unsupported modes raise ValueError"""
        with pytest.raises(ValueError, match="Unsupported mode.*for Inputs"):
            serializer.serialize(inputs_model, SerializationMode.SERIES)


class TestSerializerMixin:
    """Test SerializerMixin functionality with real models"""

    def test_serialize_methods_exist(self, fixed_input_collection_json):
        """Test that serializer methods are available on Inputs model"""
        from pyetm.models.inputs import Inputs

        inputs = Inputs.from_json(fixed_input_collection_json)

        # Check methods exist
        assert hasattr(inputs, "serialize_to_dataframe")
        assert hasattr(inputs, "serialize_to_dict")
        assert hasattr(inputs, "_serializer")

        # Check serializer is attached
        assert inputs._serializer is not None
        assert isinstance(inputs._serializer, InputsSerializer)

    def test_serialize_to_dataframe_works(self, fixed_input_collection_json):
        """Test serialize_to_dataframe method works end-to-end"""
        from pyetm.models.inputs import Inputs

        inputs = Inputs.from_json(fixed_input_collection_json)

        result = inputs.serialize_to_dataframe()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        assert result.index.name == "input"

    def test_serialize_to_dict_works(self, fixed_input_collection_json):
        """Test serialize_to_dict method works end-to-end"""
        from pyetm.models.inputs import Inputs

        inputs = Inputs.from_json(fixed_input_collection_json)

        result = inputs.serialize_to_dict()

        assert isinstance(result, dict)
        assert len(result) == 4


class TestInputTypesHandling:
    """Test that different input types are handled correctly"""

    def test_float_input_serialization(self, float_input_json):
        """Test serialization of float input"""
        from pyetm.models.inputs import Inputs

        inputs = Inputs.from_json(float_input_json)

        df = inputs.serialize_to_dataframe()

        assert len(df) == 1
        assert df.loc["investment_costs_co2_ccs", "unit"] == "EUR/tonne"
        assert df.loc["investment_costs_co2_ccs", "default"] == 500.0

    def test_bool_input_serialization(self, bool_input_json):
        """Test serialization of boolean input"""
        from pyetm.models.inputs import Inputs

        inputs = Inputs.from_json(bool_input_json)

        df = inputs.serialize_to_dataframe()

        assert len(df) == 1
        assert df.loc["has_electricity_storage", "unit"] == "bool"
        assert df.loc["has_electricity_storage", "default"] == 0

    def test_enum_input_serialization(self, enum_input_json):
        """Test serialization of enum input"""
        from pyetm.models.inputs import Inputs

        inputs = Inputs.from_json(enum_input_json)

        df = inputs.serialize_to_dataframe()

        assert len(df) == 1
        assert df.loc["transport_car_fuel_type", "unit"] == "enum"
        assert df.loc["transport_car_fuel_type", "default"] == 0
