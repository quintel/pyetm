import pandas as pd
from pyetm.models.packables.custom_curves_pack import CustomCurvesPack


class MockScenario:
    def __init__(self, id="id1"):
        self.id = id
        self.curves_updated_with = None
        self.custom_series_called = False

    def custom_curves_series(self):
        self.custom_series_called = True
        return [pd.Series([1, 2, 3], name="curve1")]

    def identifier(self):
        return f"scenario-{self.id}"

    def update_custom_curves(self, curves):
        self.curves_updated_with = curves


class MockCustomCurves:
    @staticmethod
    def _from_dataframe(df, scenario_id=None):
        return {"built_from": df.copy(), "scenario_id": scenario_id}


def test_build_dataframe_for_scenario_returns_concatenated_df(monkeypatch):
    pack = CustomCurvesPack()
    scenario = MockScenario()

    df = pack._build_dataframe_for_scenario(scenario)
    assert isinstance(df, pd.DataFrame)
    assert "curve1" in df.columns
    assert scenario.custom_series_called


def test_build_dataframe_for_scenario_returns_none_if_exception(monkeypatch):
    pack = CustomCurvesPack()

    def bad_series():
        raise RuntimeError("bad")

    scenario = MockScenario()
    scenario.custom_curves_series = bad_series

    result = pack._build_dataframe_for_scenario(scenario)
    assert result is None


def test_build_dataframe_for_scenario_returns_none_if_empty(monkeypatch):
    pack = CustomCurvesPack()

    def empty_series():
        return []

    scenario = MockScenario()
    scenario.custom_curves_series = empty_series

    result = pack._build_dataframe_for_scenario(scenario)
    assert result is None


def test_from_dataframe_returns_early_for_none_df():
    pack = CustomCurvesPack()
    assert pack.from_dataframe(None) is None


def test_from_dataframe_returns_early_for_empty_df():
    pack = CustomCurvesPack()
    df = pd.DataFrame()
    assert pack.from_dataframe(df) is None


def test_from_dataframe_returns_early_if_not_multiindex(monkeypatch):
    pack = CustomCurvesPack()

    monkeypatch.setattr(pack, "_normalize_curves_dataframe", lambda df: df)

    df = pd.DataFrame({"a": [1, 2]})
    assert pack.from_dataframe(df) is None
