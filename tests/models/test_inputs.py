import pytest
from pyetm.models import Inputs, Input
from pyetm.models.inputs import BoolInput, EnumInput, FloatInput


def test_collection_from_json(inputs_json):
    """Test creating Inputs collection from JSON data."""
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
    """Test validation of updates using new WarningCollector system."""
    input_collection = Inputs.from_json(inputs_json)

    # A good update - should return empty dict
    warnings = input_collection.is_valid_update({"investment_costs_co2_ccs": 50.0})
    assert len(warnings) == 0

    # An update that will trigger validation - returns WarningCollector objects
    warnings = input_collection.is_valid_update({"investment_costs_co2_ccs": "hello"})
    assert len(warnings) > 0
    assert "investment_costs_co2_ccs" in warnings

    # Check the WarningCollector object
    warning_collector = warnings["investment_costs_co2_ccs"]
    assert warning_collector.has_warnings("user")

    user_warnings = warning_collector.get_by_field("user")
    assert len(user_warnings) > 0
    assert "unable to parse string as a number" in user_warnings[0].message.lower()

    # An update of a non-existent key
    warnings = input_collection.is_valid_update({"hello": "hello"})
    assert len(warnings) > 0
    assert "hello" in warnings

    # Check non-existent key warning
    hello_warnings = warnings["hello"]
    assert hello_warnings.has_warnings("hello")
    hello_warning_msgs = [w.message for w in hello_warnings.get_by_field("hello")]
    assert "Key does not exist" in hello_warning_msgs


def test_collection_update_method(inputs_json):
    """Test the update method applies changes correctly."""
    input_collection = Inputs.from_json(inputs_json)

    # Get original value
    original_input = next(
        inp for inp in input_collection if inp.key == "investment_costs_co2_ccs"
    )
    original_value = original_input.user

    # Update with valid value
    input_collection.update({"investment_costs_co2_ccs": 75.0})
    assert original_input.user == 75.0

    # Update with invalid value - should not change and add warning
    input_collection.update({"investment_costs_co2_ccs": "invalid"})
    assert original_input.user == 75.0  # Should not change
    assert original_input.warnings.has_warnings("user")


def test_collection_with_invalid_inputs():
    """Test collection creation when individual inputs have issues."""
    # Create data that will cause some inputs to have warnings
    problematic_data = {
        "good_input": {"unit": "float", "min": 0, "max": 100, "user": 50},
        "bad_input": {"unit": "float"},  # Missing required min/max
        "unknown_unit": {"unit": "weird_unit", "min": 0, "max": 100},
    }

    collection = Inputs.from_json(problematic_data)

    # Collection should be created
    assert len(collection) == 3

    # Collection should have warnings merged from problematic inputs
    assert len(collection.warnings) > 0


@pytest.mark.parametrize(
    "json_fixture",
    ["float_input_json", "enum_input_json", "bool_input_json", "disabled_input_json"],
)
def test_input_from_json(json_fixture, request):
    """Test Input creation from JSON data."""
    input_json = request.getfixturevalue(json_fixture)
    input_obj = Input.from_json(next(iter(input_json.items())))

    # Assert valid input
    assert input_obj


def test_bool_input():
    """Test BoolInput validation and warning behavior."""
    input_obj = BoolInput(key="my_bool", unit="bool", default=0.0)

    # Setting the input
    input_obj.user = 0.0
    assert input_obj.user == 0.0

    # Is it valid to update to string? - should return WarningCollector
    validity_warnings = input_obj.is_valid_update("true")
    assert validity_warnings.has_warnings("user")

    user_warnings = validity_warnings.get_by_field("user")
    assert any(
        "unable to parse string as a number" in w.message.lower() for w in user_warnings
    )

    # Is it valid to update to 0.5?
    validity_warnings = input_obj.is_valid_update(0.5)
    assert validity_warnings.has_warnings("user")

    user_warnings = validity_warnings.get_by_field("user")
    assert any("0.5 should be 1.0 or 0.0" in w.message for w in user_warnings)

    # Try to update to 0.5 - should not change value but add warning
    input_obj.user = 0.5
    assert input_obj.user == 0.0  # Should not change
    assert input_obj.warnings.has_warnings("user")  # Should have warning

    # Reset the input
    input_obj.user = "reset"
    assert input_obj.user is None
    assert not input_obj.warnings.has_warnings("user")  # Warnings should be cleared


def test_enum_input():
    """Test EnumInput validation and warning behavior."""
    input_obj = EnumInput(
        key="my_enum",
        unit="enum",
        default="diesel",
        permitted_values=["diesel", "gasoline"],
    )

    # Setting the input
    input_obj.user = "gasoline"
    assert input_obj.user == "gasoline"

    # Is it valid to update to kerosene?
    validity_warnings = input_obj.is_valid_update("kerosene")
    assert validity_warnings.has_warnings("user")

    user_warnings = validity_warnings.get_by_field("user")
    assert any(
        "kerosene should be in ['diesel', 'gasoline']" in w.message
        for w in user_warnings
    )

    # Try to update to invalid number - should not change value but add warning
    input_obj.user = 0.5
    assert input_obj.warnings.has_warnings("user")
    assert input_obj.user == "gasoline"  # Should not change

    # Try to update to kerosene - should not change value but add warning
    input_obj.warnings.clear()  # Clear previous warnings
    input_obj.user = "kerosene"
    assert input_obj.warnings.has_warnings("user")
    assert input_obj.user == "gasoline"  # Should not change

    # Reset the input
    input_obj.user = "reset"
    assert input_obj.user is None
    assert not input_obj.warnings.has_warnings("user")


def test_float_input():
    """Test FloatInput validation and warning behavior."""
    input_obj = FloatInput(key="my_float", unit="euro", min=0.0, max=20.0)

    # Setting the input
    input_obj.user = 2.0
    assert input_obj.user == 2.0

    # Is it valid to update to -1.0?
    validity_warnings = input_obj.is_valid_update(-1.0)
    assert validity_warnings.has_warnings("user")

    user_warnings = validity_warnings.get_by_field("user")
    assert any(
        "-1.0 should be between 0.0 and 20.0" in w.message for w in user_warnings
    )

    # Try to update to 30 - should not change value but add warning
    input_obj.user = 30.0
    assert input_obj.warnings.has_warnings("user")

    # Check the warning message
    user_warnings = input_obj.warnings.get_by_field("user")
    assert any(
        "30.0 should be between 0.0 and 20.0" in w.message for w in user_warnings
    )
    assert input_obj.user == 2.0  # Should not change

    # Reset the input
    input_obj.user = "reset"
    assert input_obj.user is None
    assert not input_obj.warnings.has_warnings("user")


def test_input_warning_severity_levels():
    """Test that different validation failures can have different severity levels."""
    input_obj = FloatInput(key="test", unit="float", min=0, max=100)

    # Create input with validation error (should be 'error' severity from __init__)
    bad_input = FloatInput(
        key="test", unit="float", min=0, max=100, user="not_a_number"
    )

    # Check that initialization warnings exist
    if len(bad_input.warnings) > 0:
        warnings = list(bad_input.warnings)
        # Should have error-level warnings from failed initialization
        assert any(w.severity == "error" for w in warnings)


def test_input_warning_timestamps():
    """Test that warnings have timestamps."""
    input_obj = BoolInput(key="test", unit="bool")

    # Create a warning
    input_obj.user = 0.5  # Invalid value

    if len(input_obj.warnings) > 0:
        warnings = list(input_obj.warnings)
        for warning in warnings:
            assert hasattr(warning, "timestamp")
            assert warning.timestamp is not None


def test_warning_collector_methods():
    """Test WarningCollector methods work correctly with Input objects."""
    input_obj = FloatInput(key="test", unit="float", min=0, max=100)

    # Add some warnings
    input_obj.user = -5  # Out of bounds
    input_obj.add_warning("custom_field", "Custom warning", "info")

    warnings = input_obj.warnings

    # Test various methods
    assert len(warnings) > 0
    assert warnings.has_warnings()
    assert warnings.has_warnings("user")
    assert warnings.has_warnings("custom_field")

    fields_with_warnings = warnings.get_fields_with_warnings()
    assert "user" in fields_with_warnings
    assert "custom_field" in fields_with_warnings

    # Test clearing specific field
    warnings.clear("custom_field")
    assert not warnings.has_warnings("custom_field")
    assert warnings.has_warnings("user")  # Should still have user warnings


def test_inputs_collection_warning_aggregation():
    """Test that Inputs collection properly aggregates warnings from individual inputs."""
    # Create inputs with various issues
    inputs_data = {
        "good_input": {"unit": "float", "min": 0, "max": 100, "user": 50},
        "bad_float": {
            "unit": "float",
            "min": 0,
            "max": 100,
            "user": 150,
        },  # Out of bounds
        "bad_bool": {"unit": "bool", "user": 0.5},  # Invalid bool value
        "missing_data": {"unit": "enum"},  # Missing required permitted_values
    }

    collection = Inputs.from_json(inputs_data)

    # Collection should exist
    assert len(collection) == 4

    # Should have aggregated warnings
    assert len(collection.warnings) > 0

    # Check that warnings from individual inputs are properly prefixed
    warning_fields = collection.warnings.get_fields_with_warnings()

    # Should have warnings with input key prefixes
    assert any("Input(key=" in field for field in warning_fields)


def test_input_serializable_fields():
    """Test that different input types return correct serializable fields."""
    # Test base Input
    base_input = Input(key="test", unit="simple")
    base_fields = base_input._get_serializable_fields()
    expected_base = [
        "key",
        "unit",
        "default",
        "user",
        "disabled",
        "coupling_disabled",
        "coupling_groups",
        "disabled_by",
    ]
    for field in expected_base:
        assert field in base_fields

    # Test EnumInput includes permitted_values
    enum_input = EnumInput(key="test", unit="enum", permitted_values=["a", "b"])
    enum_fields = enum_input._get_serializable_fields()
    assert "permitted_values" in enum_fields

    # Test FloatInput includes min/max
    float_input = FloatInput(key="test", unit="float", min=0, max=100)
    float_fields = float_input._get_serializable_fields()
    assert "min" in float_fields
    assert "max" in float_fields
    assert "step" in float_fields
    assert "share_group" in float_fields


def test_input_reset_functionality():
    """Test that 'reset' string properly clears user values across all input types."""
    # Test FloatInput
    float_input = FloatInput(key="test_float", unit="float", min=0, max=100, user=50.0)
    assert float_input.user == 50.0
    float_input.user = "reset"
    assert float_input.user is None

    # Test BoolInput
    bool_input = BoolInput(key="test_bool", unit="bool", user=1.0)
    assert bool_input.user == 1.0
    bool_input.user = "reset"
    assert bool_input.user is None

    # Test EnumInput
    enum_input = EnumInput(
        key="test_enum", unit="enum", permitted_values=["a", "b"], user="a"
    )
    assert enum_input.user == "a"
    enum_input.user = "reset"
    assert enum_input.user is None


def test_collection_iteration_and_access():
    """Test that Inputs collection supports proper iteration and key access."""
    inputs_data = {
        "input1": {"unit": "float", "min": 0, "max": 100},
        "input2": {"unit": "bool"},
        "input3": {"unit": "enum", "permitted_values": ["a", "b"]},
    }

    collection = Inputs.from_json(inputs_data)

    # Test length
    assert len(collection) == 3

    # Test iteration
    input_keys = [inp.key for inp in collection]
    assert "input1" in input_keys
    assert "input2" in input_keys
    assert "input3" in input_keys

    # Test keys() method
    keys = collection.keys()
    assert len(keys) == 3
    assert "input1" in keys
    assert "input2" in keys
    assert "input3" in keys
