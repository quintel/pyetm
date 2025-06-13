import pytest

from pyetm.models import Input

@pytest.mark.parametrize(
    'json_fixture',
    ['float_input_json', 'enum_input_json', 'bool_input_json', 'disabled_input_json']
)
def test_input_from_json(json_fixture, request):
    input = Input.from_json(next(iter(request.getfixturevalue(json_fixture).items())))

    # Assert valid input
    assert input
