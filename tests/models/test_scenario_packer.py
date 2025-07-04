from pathlib import Path

from pyetm.models import ScenarioPacker, InputCollection, Input, CustomCurves, Scenario
from pyetm.models.custom_curves import CustomCurve


def test_inputs(scenario):
    packer = ScenarioPacker()
    scenario._inputs = InputCollection(inputs=[Input(key="test_input", unit="MW")])

    packer.add_inputs(scenario)

    dataframe = packer.inputs()

    assert dataframe["unit"]["test_input"] == "MW"


def test_main_info(scenario):
    packer = ScenarioPacker()

    scenario = Scenario(id=scenario.id, area_code="nl2015", end_year=2050)

    packer.add_inputs(scenario)

    dataframe = packer.main_info()

    assert dataframe[scenario.id]["area_code"] == "nl2015"


def test_custom_curves(scenario):
    # Mock some stuff
    curve = CustomCurve(
        key="interconnector_2_export_availability",
        type="something",
        file_path=Path("tests/fixtures/interconnector_2_export_availability.csv"),
    )
    scenario._custom_curves = CustomCurves(curves=[curve])

    # Set up Packer
    packer = ScenarioPacker()
    packer.add_curves(scenario)

    dataframe = packer.custom_curves()

    assert dataframe["interconnector_2_export_availability"][0] == 1.0

def test_custom_curves_when_not_set(scenario):
    # Empty collection
    scenario._custom_curves = CustomCurves(curves=[])

     # Set up Packer
    packer = ScenarioPacker()
    packer.add_curves(scenario)

    dataframe = packer.custom_curves()

    assert dataframe.empty
