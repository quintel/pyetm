import pandas as pd
from unittest.mock import Mock

from pyetm.models.packables.query_pack import QueryPack


def make_scenario(id_val="S"):
    s = Mock()
    s.identifier = Mock(return_value=str(id_val))
    s.results = Mock(
        return_value=pd.DataFrame(
            {"future": [1], "unit": ["MW"]}, index=["q1"]
        ).set_index("unit", append=True)
    )
    s.add_queries = Mock()
    return s


def test_to_dataframe_calls_results_and_builds(caplog):
    s1 = make_scenario("S1")
    s2 = make_scenario("S2")

    pack = QueryPack()
    pack.add(s1, s2)

    df = pack.to_dataframe()
    assert not df.empty
    assert "S1" in df.columns or "S2" in df.columns


def test_to_dataframe_handles_exception(caplog):
    s = make_scenario()
    s.results.side_effect = RuntimeError("bad")

    pack = QueryPack()
    pack.add(s)

    with caplog.at_level("WARNING"):
        df = pack.to_dataframe()
        assert df.empty
        assert "Failed building gquery results" in caplog.text


def test_from_dataframe_applies_unique_queries():
    s1 = make_scenario()
    s2 = make_scenario()

    pack = QueryPack()
    pack.add(s1, s2)

    df = pd.DataFrame({"queries": ["a", " a ", "b", None, "nan", "B"]})
    pack.from_dataframe(df)

    # Should deduplicate and strip, keep case of non-'nan' values
    expected = ["a", "b", "B"]
    s1.add_queries.assert_called_once_with(expected)
    s2.add_queries.assert_called_once_with(expected)


def test_from_dataframe_early_returns():
    pack = QueryPack()
    pack.from_dataframe(None)
    pack.from_dataframe(pd.DataFrame())
