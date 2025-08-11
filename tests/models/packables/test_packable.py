import pytest
import pandas as pd
from pyetm.models.packables.packable import Packable


class MockScenario:
    def __init__(self, id):
        self._id = id

    def identifier(self):
        return self._id


@pytest.fixture
def packable():
    return Packable()


def test_add_discard_clear(packable):
    s1 = MockScenario("a")
    s2 = MockScenario("b")

    packable.add(s1)
    assert s1 in packable.scenarios
    assert packable._scenario_id_cache is None

    packable.add(s2)
    assert s2 in packable.scenarios

    packable.discard(s1)
    assert s1 not in packable.scenarios
    assert packable._scenario_id_cache is None

    packable.clear()
    assert len(packable.scenarios) == 0
    assert packable._scenario_id_cache is None


def test_summary(packable):
    s1 = MockScenario("id1")
    s2 = MockScenario("id2")
    packable.add(s1, s2)
    summary = packable.summary()
    assert "base_pack" in summary
    assert summary["base_pack"]["scenario_count"] == 2


def test_key_for_returns_identifier(packable):
    s = MockScenario("sc1")
    assert packable._key_for(s) == "sc1"


def test_build_pack_dataframe_calls_and_concat(monkeypatch, packable):
    s1 = MockScenario("sc1")
    s2 = MockScenario("sc2")
    packable.add(s1, s2)

    # Mock _build_dataframe_for_scenario to return a simple df
    def fake_build_df(scenario, **kwargs):
        return pd.DataFrame({f"{scenario.identifier()}_col": [1, 2]})

    monkeypatch.setattr(packable, "_build_dataframe_for_scenario", fake_build_df)
    monkeypatch.setattr(packable, "_concat_frames", lambda frames, keys: (frames, keys))

    frames, keys = packable.build_pack_dataframe()

    assert isinstance(frames[0], pd.DataFrame)
    assert keys == ["sc1", "sc2"] or keys == ["sc2", "sc1"]  # order not guaranteed


def test_build_pack_dataframe_skips_none_empty(monkeypatch, packable):
    s = MockScenario("sc")
    packable.add(s)

    monkeypatch.setattr(packable, "_build_dataframe_for_scenario", lambda s, **kw: None)
    df = packable.build_pack_dataframe()
    assert df.empty

    monkeypatch.setattr(
        packable, "_build_dataframe_for_scenario", lambda s, **kw: pd.DataFrame()
    )
    df = packable.build_pack_dataframe()
    assert df.empty


def test_build_pack_dataframe_handles_exceptions(monkeypatch, packable):
    s = MockScenario("sc")
    packable.add(s)

    def raise_exc(scenario, **kwargs):
        raise RuntimeError("fail")

    monkeypatch.setattr(packable, "_build_dataframe_for_scenario", raise_exc)

    # Should not raise, just skip scenario
    df = packable.build_pack_dataframe()
    assert df.empty


def test_to_dataframe_returns_empty_if_no_scenarios(monkeypatch, packable):
    assert packable.to_dataframe().empty

    monkeypatch.setattr(
        packable, "_to_dataframe", lambda **kwargs: pd.DataFrame({"a": [1]})
    )
    packable.add(MockScenario("sc"))
    df = packable.to_dataframe()
    assert "a" in df.columns


def test_refresh_cache_and_find_by_identifier(packable):
    s1 = MockScenario("sc1")
    s2 = MockScenario("sc2")
    packable.add(s1, s2)

    packable._scenario_id_cache = None
    packable._refresh_cache()

    assert "sc1" in packable._scenario_id_cache
    assert packable._find_by_identifier("sc2") == s2
    assert packable._find_by_identifier("missing") is None


def test_resolve_scenario(packable):
    s = MockScenario("foo")
    packable.add(s)
    assert packable.resolve_scenario("foo") == s
    assert packable.resolve_scenario(None) is None
    assert packable.resolve_scenario("bar") is None


def test_is_blank():
    assert Packable.is_blank(None)
    assert Packable.is_blank(float("nan"))
    assert Packable.is_blank("")
    assert Packable.is_blank("   ")
    assert not Packable.is_blank("x")
    assert not Packable.is_blank(123)


def test_drop_all_blank():
    df = pd.DataFrame({"a": [None, None], "b": [None, None]})
    result = Packable.drop_all_blank(df)
    assert result.empty

    df2 = pd.DataFrame({"a": [None, 1], "b": [None, 2]})
    result2 = Packable.drop_all_blank(df2)
    assert len(result2) == 1


def test_first_non_empty_row_positions():
    df = pd.DataFrame({"a": [None, 1, 2], "b": [None, None, 3]})
    positions = Packable.first_non_empty_row_positions(df, count=2)
    assert positions == [1, 2]

    positions = Packable.first_non_empty_row_positions(pd.DataFrame(), count=2)
    assert positions == []


def test_apply_identifier_blocks(monkeypatch, packable):
    s1 = MockScenario("sc1")
    s2 = MockScenario("sc2")
    packable.add(s1, s2)

    columns = pd.MultiIndex.from_tuples(
        [("sc1", "a"), ("sc1", "b"), ("sc2", "a")], names=["id", "curve"]
    )
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=columns)

    called = {}

    def apply_block(scenario, block):
        called[scenario.identifier()] = block.sum().sum()

    packable.apply_identifier_blocks(df, apply_block)

    assert "sc1" in called
    assert "sc2" in called

    # Test with resolve function overriding
    def resolve_override(label):
        return s1 if label == "sc1" else None

    called.clear()
    packable.apply_identifier_blocks(df, apply_block, resolve=resolve_override)
    assert "sc1" in called
    assert "sc2" in called

    # Test with non-MultiIndex columns
    packable.apply_identifier_blocks(pd.DataFrame({"a": [1, 2]}), apply_block)


def test_apply_identifier_blocks_logs(monkeypatch, caplog, packable):
    s1 = MockScenario("sc1")
    packable.add(s1)

    columns = pd.MultiIndex.from_tuples([("sc1", "a")], names=["id", "curve"])
    df = pd.DataFrame([[1]], columns=columns)

    def fail_block(scenario, block):
        raise ValueError("fail")

    with caplog.at_level("WARNING"):
        packable.apply_identifier_blocks(df, fail_block)
        assert "Failed applying block" in caplog.text


def test_normalize_two_header_sheet_basic(packable):
    df = pd.DataFrame(
        [
            ["id1", "id2"],
            ["curve1", "curve2"],
            [1, 2],
            [3, 4],
        ]
    )
    result = packable._normalize_two_header_sheet(df, reset_index=True)
    assert isinstance(result.columns, pd.MultiIndex)
    assert result.shape == (2, 2)
    assert result.index.equals(pd.RangeIndex(0, 2))


def test_normalize_two_header_sheet_single_header(packable):
    df = pd.DataFrame(
        [
            ["id1", "id2"],
            [1, 2],
            [3, 4],
        ]
    )
    result = packable._normalize_two_header_sheet(df)
    assert isinstance(result.columns, pd.MultiIndex)
    assert result.shape[0] == 1 or result.shape[0] == 2


def test_normalize_two_header_sheet_with_helpers(packable):
    df = pd.DataFrame(
        [
            ["helper", "id2"],
            ["helpercurve", "curve2"],
            [1, 2],
            [3, 4],
        ]
    )
    result = packable._normalize_two_header_sheet(
        df,
        helper_level0={"helper"},
        helper_level1={"helpercurve"},
        drop_empty_level0=True,
        drop_empty_level1=True,
    )
    # "helper" and "helpercurve" columns should be removed
    for lvl0 in result.columns.get_level_values(0):
        assert lvl0.lower() != "helper"
    for lvl1 in result.columns.get_level_values(1):
        assert lvl1.lower() != "helpercurve"
