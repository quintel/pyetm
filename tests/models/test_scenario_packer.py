import pytest

from pyetm.models import ScenarioPacker, InputCollection, Input

def test_inputs(scenario):
    packer = ScenarioPacker()
    scenario._inputs = InputCollection(
        inputs=[Input(key='test_input',unit='MW')]
    )

    packer.add_inputs(scenario)

    dataframe = packer.inputs()

    assert dataframe['unit']['test_input'] == 'MW'


def test_main_info(scenario):
    packer = ScenarioPacker()

    scenario.end_year = 2050
    scenario.area_code = 'nl2015'

    packer.add_inputs(scenario)

    dataframe = packer.main_info()

    assert dataframe[scenario.id]['area_code'] == 'nl2015'
