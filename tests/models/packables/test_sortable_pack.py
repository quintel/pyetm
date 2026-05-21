import pandas as pd
from unittest.mock import Mock

from pyetm.models.packables.sortable_pack import SortablePack


def make_scenario(id_val="S1"):
    s = Mock()
    s.identifier = Mock(return_value=str(id_val))
    s.sortables = Mock()
    s.session.short_name = None
    return s


def test_to_dataframe_builds_from_scenarios():
    s1 = make_scenario("S1")
    s2 = make_scenario("S2")
    s1.sortables.to_dataframe.return_value = pd.DataFrame({"a": [1]})
    s2.sortables.to_dataframe.return_value = pd.DataFrame({"b": [2]})

    pack = SortablePack()
    pack.add(s1, s2)

    df = pack.to_dataframe()
    assert not df.empty


def test_to_dataframe_handles_exception_and_empty(caplog):
    s = make_scenario("S")
    s.sortables.to_dataframe.side_effect = RuntimeError("boom")
    pack = SortablePack()
    pack.add(s)

    with caplog.at_level("WARNING"):
        df = pack.to_dataframe()
        assert df.empty
        assert "Failed extracting sortables" in caplog.text

    s.sortables.to_dataframe.side_effect = None
    s.sortables.to_dataframe.return_value = pd.DataFrame()
    df2 = pack.to_dataframe()
    assert df2.empty
