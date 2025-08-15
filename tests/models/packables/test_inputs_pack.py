import pandas as pd
from unittest.mock import Mock

from pyetm.models.packables.inputs_pack import InputsPack


class DummyInput:
    def __init__(self, key, user):
        self.key = key
        self.user = user


def make_scenario(id_val, identifier=None):
    s = Mock()
    s.id = id_val
    if identifier is None:
        s.identifier = Mock(return_value=str(id_val))
    else:
        s.identifier = (
            Mock(side_effect=identifier)
            if callable(identifier)
            else Mock(return_value=identifier)
        )
    s.update_user_values = Mock()
    return s


def test_key_for_prefers_short_name_and_fallbacks():
    s1 = make_scenario(1, identifier="id-1")
    s2 = make_scenario(2, identifier="id-2")

    pack = InputsPack()
    pack.set_scenario_short_names({"1": "S1"})

    assert pack._get_scenario_display_key(s1) == "S1"  # short name wins
    assert pack._get_scenario_display_key(s2) == "id-2"  # falls back to identifier

    s3 = make_scenario(
        3, identifier=lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    assert (
        pack._get_scenario_display_key(s3) == 3
    )  # falls back to id when identifier fails


def test_resolve_scenario_by_short_identifier_and_numeric():
    s1, s2, s3 = (
        make_scenario(1, "ID1"),
        make_scenario(2, "ID2"),
        make_scenario(3, "ID3"),
    )
    pack = InputsPack()
    pack.add(s1, s2, s3)
    pack.set_scenario_short_names({"1": "S1"})

    assert pack.resolve_scenario("S1") is s1  # short name
    assert pack.resolve_scenario("ID2") is s2  # identifier
    assert pack.resolve_scenario("3") is s3  # numeric id
    assert pack.resolve_scenario("missing") is None


def test_to_dataframe_from_iterable_inputs_only():
    s = make_scenario(1, "S1")
    s.inputs = [DummyInput("a", 10), DummyInput("b", 20)]

    pack = InputsPack()
    pack.add(s)
    df = pack.to_dataframe()

    assert list(df.index) == ["a", "b"]
    assert "S1" in df.columns or 1 in df.columns
    col = "S1" if "S1" in df.columns else 1
    assert df.loc["a", col] == 10
    assert df.loc["b", col] == 20
    assert df.index.name == "input"


def test_to_dataframe_from_df_and_series_variants():
    s1 = make_scenario(1, "S1")
    s1.inputs = Mock()
    s1.inputs.__iter__ = Mock(side_effect=TypeError())
    s1.inputs.to_dataframe = Mock(
        return_value=pd.DataFrame(
            {"user": [1, 2], "unit": ["MW", "MW"]}, index=["a", "b"]
        ).set_index("unit", append=True)
    )

    s2 = make_scenario(2, "S2")
    s2.inputs = Mock()
    s2.inputs.__iter__ = Mock(side_effect=TypeError())
    s2.inputs.to_dataframe = Mock(
        return_value=pd.DataFrame({"value": [3, 4]}, index=["c", "d"])
    )

    # From Series
    s3 = make_scenario(3, "S3")
    s3.inputs = Mock()
    s3.inputs.__iter__ = Mock(side_effect=TypeError())
    s3.inputs.to_dataframe = Mock(return_value=pd.Series([5], index=["e"]))

    pack = InputsPack()
    pack.add(s1, s2, s3)
    df = pack.to_dataframe()

    # All keys present
    for key in ["a", "b", "c", "d", "e"]:
        assert key in df.index

    assert df.loc["a", "S1"] == 1
    assert df.loc["c", "S2"] == 3
    assert df.loc["e", "S3"] == 5


def test_to_dataframe_returns_empty_when_no_data():
    s = make_scenario(1, "S1")
    s.inputs = Mock()
    s.inputs.__iter__ = Mock(side_effect=TypeError())
    s.inputs.to_dataframe = Mock(return_value=pd.DataFrame())

    pack = InputsPack()
    pack.add(s)
    df = pack.to_dataframe()
    assert df.empty


def test_from_dataframe_parses_and_updates(caplog):
    s1 = make_scenario(1, "S1")
    s2 = make_scenario(2, "S2")
    s3 = make_scenario(3, "S3")

    pack = InputsPack()
    pack.add(s1, s2, s3)
    pack.set_scenario_short_names({"1": "Short1"})

    df = pd.DataFrame(
        [
            ["input", "Short1", "3", "Unknown"],
            ["a", 1, 10, 99],
            ["b", " ", "nan", 88],
        ]
    )

    with caplog.at_level("WARNING"):
        pack.from_dataframe(df)

    s1.update_user_values.assert_called_once_with({"a": 1})
    s3.update_user_values.assert_called_once_with({"a": 10})
    # Unknown column should produce a warning and not call any scenario
    assert "Could not find scenario for SLIDER_SETTINGS column label" in caplog.text


def test_from_dataframe_handles_update_exception(caplog):
    s1 = make_scenario(1, "S1")
    s1.update_user_values.side_effect = RuntimeError("fail")

    pack = InputsPack()
    pack.add(s1)

    df = pd.DataFrame([["input", "S1"], ["a", 1]])

    with caplog.at_level("WARNING"):
        pack.from_dataframe(df)
        assert "Failed updating inputs for scenario" in caplog.text


def test_from_dataframe_early_returns():
    pack = InputsPack()
    # None and empty
    pack.from_dataframe(None)
    pack.from_dataframe(pd.DataFrame())
    # No header rows
    pack.from_dataframe(pd.DataFrame([[None], [None]]))
    # After header but no data columns
    pack.from_dataframe(pd.DataFrame([["only-one-col"], [1]]))
