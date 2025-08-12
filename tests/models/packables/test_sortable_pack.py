import pandas as pd
from unittest.mock import Mock

from pyetm.models.packables.sortable_pack import SortablePack


def make_scenario(id_val="S1"):
    s = Mock()
    s.identifier = Mock(return_value=str(id_val))
    s.sortables = Mock()
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


def test_from_dataframe_multiindex_and_single_block(monkeypatch):
    s1 = make_scenario("S1")
    s2 = make_scenario("S2")
    pack = SortablePack()
    pack.add(s1, s2)
    cols = pd.MultiIndex.from_tuples([("S1", "a"), ("S2", "a")])
    df = pd.DataFrame([[1, 2]], columns=cols)
    monkeypatch.setattr(pack, "_normalize_sortables_dataframe", lambda d: d)
    pack.from_dataframe(df)

    assert s1.set_sortables_from_dataframe.called
    assert s2.set_sortables_from_dataframe.called


def test_from_dataframe_normalize_errors_and_empty(caplog, monkeypatch):
    s = make_scenario("S")
    pack = SortablePack()
    pack.add(s)

    with caplog.at_level("WARNING"):
        monkeypatch.setattr(
            pack,
            "_normalize_sortables_dataframe",
            lambda d: (_ for _ in ()).throw(RuntimeError("bad")),
        )
        pack.from_dataframe(pd.DataFrame([[1]]))
        assert "Failed to normalize sortables sheet" in caplog.text

    # empty after normalize
    monkeypatch.setattr(
        pack, "_normalize_sortables_dataframe", lambda d: pd.DataFrame()
    )
    pack.from_dataframe(pd.DataFrame([[1]]))
    assert not s.set_sortables_from_dataframe.called
