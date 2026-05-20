import pandas as pd
from unittest.mock import Mock, MagicMock
import pytest

from pyetm.models.packables.inputs_pack import InputsPack


def make_input_object(
    key, user_value=None, default_value=None, min_value=None, max_value=None
):
    """Create a mock input object with specified attributes."""
    input_obj = Mock()
    input_obj.key = key
    input_obj.user = user_value
    input_obj.default = default_value
    input_obj.min = min_value
    input_obj.max = max_value
    # Add merged_value property
    input_obj.merged_value = user_value if user_value is not None else default_value
    return input_obj


def make_scenario(id_val=1, identifier="S1", inputs_data=None):
    """Create a mock scenario with inputs."""
    s = Mock()
    s.id = id_val
    s.identifier = Mock(return_value=identifier)

    # Create mock inputs
    s.inputs = Mock()

    if inputs_data:
        # Set up input objects
        input_objects = []
        for key, values in inputs_data.items():
            input_obj = make_input_object(
                key=key,
                user_value=values.get("user"),
                default_value=values.get("default"),
                min_value=values.get("min"),
                max_value=values.get("max"),
            )
            input_objects.append(input_obj)
        s.inputs.__iter__ = Mock(return_value=iter(input_objects))

        # Set up to_dataframe method
        def mock_to_dataframe(fields=None):
            if isinstance(fields, list):
                fields = fields
            else:
                fields = [fields] if fields else ["value"]

            data = {}
            for field in fields:
                # Handle value field by preferring user over default
                if field == "value":
                    data[field] = [
                        inputs_data[key].get("user")
                        if inputs_data[key].get("user") is not None
                        else inputs_data[key].get("default")
                        for key in inputs_data.keys()
                    ]
                else:
                    data[field] = [
                        inputs_data[key].get(field) for key in inputs_data.keys()
                    ]

            df = pd.DataFrame(data, index=list(inputs_data.keys()))
            return df

        s.inputs.to_dataframe = Mock(side_effect=mock_to_dataframe)
    else:
        s.inputs.__iter__ = Mock(return_value=iter([]))
        s.inputs.to_dataframe = Mock(return_value=pd.DataFrame())

    # Mock update_user_values
    s.update_user_values = Mock()
    s._inputs = None  # For warning logs

    return s


def test_set_scenario_short_names():
    pack = InputsPack()
    short_names = {"1": "Base", "2": "Alternative"}

    pack.set_scenario_short_names(short_names)

    assert pack._scenario_short_names == short_names


def test_set_scenario_short_names_with_none():
    pack = InputsPack()

    pack.set_scenario_short_names(None)

    assert pack._scenario_short_names == {}


def test_get_scenario_display_key_uses_short_name():
    pack = InputsPack()
    pack.set_scenario_short_names({"1": "Base"})

    scenario = make_scenario(id_val=1)

    result = pack._get_scenario_display_key(scenario)

    assert result == "Base"


def test_get_scenario_display_key_uses_identifier():
    pack = InputsPack()

    scenario = make_scenario(id_val=1, identifier="Scenario1")

    result = pack._get_scenario_display_key(scenario)

    assert result == "Scenario1"


def test_get_scenario_display_key_falls_back_to_id():
    pack = InputsPack()

    scenario = make_scenario(id_val=1)
    scenario.identifier.side_effect = Exception("No identifier")

    result = pack._get_scenario_display_key(scenario)

    assert result == 1


def test_resolve_scenario_by_short_name():
    s1 = make_scenario(id_val=1, identifier="S1")
    s2 = make_scenario(id_val=2, identifier="S2")

    pack = InputsPack()
    pack.add(s1, s2)
    pack.set_scenario_short_names({"1": "Base", "2": "Alt"})

    result = pack.resolve_scenario("Base")

    assert result == s1


def test_resolve_scenario_by_numeric_id():
    s1 = make_scenario(id_val=1, identifier="S1")
    s2 = make_scenario(id_val=2, identifier="S2")

    pack = InputsPack()
    pack.add(s1, s2)
    # Mock _find_by_identifier to return None (not found by identifier)
    with pytest.MonkeyPatch().context() as m:
        m.setattr(
            "pyetm.models.packables.packable.Packable._find_by_identifier",
            Mock(return_value=None),
        )

        result = pack.resolve_scenario("2")

        assert result == s2


def test_resolve_scenario_returns_none_for_invalid():
    pack = InputsPack()

    result = pack.resolve_scenario("nonexistent")

    assert result is None


def test_resolve_scenario_returns_none_for_none():
    pack = InputsPack()

    result = pack.resolve_scenario(None)

    assert result is None


def test_to_dataframe_with_scenarios():
    inputs_data1 = {"input1": {"user": 10}, "input2": {"user": 20}}
    inputs_data2 = {"input1": {"user": 15}, "input2": {"user": 25}}

    s1 = make_scenario(id_val=1, identifier="S1", inputs_data=inputs_data1)
    s2 = make_scenario(id_val=2, identifier="S2", inputs_data=inputs_data2)

    pack = InputsPack()
    pack.add(s1, s2)

    df = pack.to_dataframe()

    assert not df.empty
    assert "S1" in df.columns.get_level_values("scenario")
    assert "S2" in df.columns.get_level_values("scenario")


def test_to_dataframe_empty_scenarios():
    pack = InputsPack()

    df = pack.to_dataframe()

    assert df.empty


def test_to_dataframe_with_include_defaults():
    inputs_data = {"input1": {"user": 10, "default": 5}}
    s1 = make_scenario(inputs_data=inputs_data)

    pack = InputsPack()
    pack.add(s1)

    df = pack.to_dataframe(include_defaults=True)

    assert "default" in df.columns.get_level_values("field")


def test_to_dataframe_with_include_min_max():
    inputs_data = {"input1": {"user": 10, "min": 0, "max": 100}}
    s1 = make_scenario(inputs_data=inputs_data)

    pack = InputsPack()
    pack.add(s1)

    df = pack.to_dataframe(include_min_max=True)

    assert "min" in df.columns.get_level_values("field")
    assert "max" in df.columns.get_level_values("field")


def test_from_dataframe_handles_missing_scenario(caplog):
    s1 = make_scenario(id_val=1, identifier="S1")

    pack = InputsPack()
    pack.add(s1)

    df = pd.DataFrame(
        {
            "Input": ["", "input1"],
            "S1": ["Scenario 1", "10"],
            "Unknown": ["Unknown Scenario", "20"],
        }
    )

    with caplog.at_level("WARNING"):
        pack.from_dataframe(df)

        assert "Could not find scenario" in caplog.text
        assert "Unknown" in caplog.text


def test_from_dataframe_early_returns():
    pack = InputsPack()

    # Should not raise exceptions
    pack.from_dataframe(None)
    pack.from_dataframe(pd.DataFrame())
