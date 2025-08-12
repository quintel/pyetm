import pandas as pd
from unittest.mock import Mock

from pyetm.models.packables.output_curves_pack import OutputCurvesPack


def make_scenario(id_val="S"):
    s = Mock()
    s.identifier = Mock(return_value=str(id_val))
    return s


def test_to_dataframe_collects_series():
    s = make_scenario()
    s.all_output_curves.return_value = [
        pd.Series([1, 2], name="c1"),
        pd.Series([3, 4], name="c2"),
    ]

    pack = OutputCurvesPack()
    pack.add(s)

    df = pack.to_dataframe()
    assert not df.empty
    assert "c1" in df.columns.get_level_values(1) or "c1" in df.columns


def test_to_dataframe_handles_exception_and_empty(caplog):
    s = make_scenario()
    s.all_output_curves.side_effect = RuntimeError("fail")

    pack = OutputCurvesPack()
    pack.add(s)

    with caplog.at_level("WARNING"):
        df = pack.to_dataframe()
        assert df.empty
        assert "Failed extracting output curves" in caplog.text

    s.all_output_curves.side_effect = None
    s.all_output_curves.return_value = []
    df2 = pack.to_dataframe()
    assert df2.empty
