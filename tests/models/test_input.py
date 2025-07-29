import pytest
from pyetm.models import Input
from pyetm.models.inputs import BoolInput, EnumInput


@pytest.mark.parametrize(
    "json_fixture",
    ["float_input_json", "enum_input_json", "bool_input_json", "disabled_input_json"],
)
def test_input_from_json(json_fixture, request):
    input_json = request.getfixturevalue(json_fixture)
    input = Input.from_json(next(iter(input_json.items())))

    # Assert valid input
    assert input


def test_bool_input():
    input = BoolInput(key='my_bool', unit='bool', default=0.0)

    # Setting the input
    input.user = 0.0
    assert input.user == 0.0

    # Is it valid to update to string?
    validity = input.is_valid_update('true')
    assert len(validity) > 0
    assert 'user: Input should be a valid number, unable to parse string as a number' in validity

    # Is it valid to update to 0.5?
    validity = input.is_valid_update(0.5)
    assert len(validity) > 0
    assert 'user: Value error, 0.5 should be 1.0 or 0.0 representing True/False, or On/Off' in validity

    # Try to update to 0.5
    input.user = 0.5
    assert input.user == 0.0

    # Reset the input
    input.user = "reset"
    assert input.user is None

    # TODO: @Louis OLD warnings persist!
    assert len(input.warnings) == 0

def test_enum_input():
    input = EnumInput(
        key='my_enum',
        unit='enum',
        default='diesel',
        permitted_values=['diesel', 'gasoline']
    )

    # Setting the input
    input.user = 'gasoline'
    assert input.user == 'gasoline'

    # TODO: @Louis ValidationError is not caught in .warnings?? why?
    # Is it valid to update to kerosene?
    validity = input.is_valid_update('kerosene')
    assert len(validity) > 0
    assert "user: Value error, kerosene should be in ['diesel', 'gasoline']" in validity

    # Try to update to 0.5
    input.user = 0.5
    assert input.user == 'gasoline'

    # Reset the input
    input.user = "reset"
    assert input.user is None
