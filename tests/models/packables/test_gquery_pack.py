import pandas as pd


class MockScenario:
    def __init__(self):
        self.received_queries = None

    def add_queries(self, queries):
        self.received_queries = queries


class DummyClass:
    def __init__(self, scenarios):
        self.scenarios = scenarios

    def from_dataframe(self, df: pd.DataFrame):
        if df is None or df.empty:
            return

        first_col = df.iloc[:, 0].dropna().astype(str).str.strip()

        # Filter out empty strings and literal "nan"
        filtered = [q for q in first_col if q and q.lower() != "nan"]

        # Remove duplicates while preserving order
        unique_queries = list(dict.fromkeys(filtered))

        if unique_queries:
            for scenario in self.scenarios:
                scenario.add_queries(unique_queries)


def test_from_dataframe_with_valid_data():
    scenario1 = MockScenario()
    scenario2 = MockScenario()
    obj = DummyClass([scenario1, scenario2])

    df = pd.DataFrame({"queries": ["q1", " q2 ", "q1", None, "nan", "  "]})

    obj.from_dataframe(df)

    expected = ["q1", "q2"]
    assert scenario1.received_queries == expected
    assert scenario2.received_queries == expected


def test_from_dataframe_with_empty_df():
    scenario = MockScenario()
    obj = DummyClass([scenario])

    df = pd.DataFrame({"queries": []})
    obj.from_dataframe(df)

    assert scenario.received_queries is None


def test_from_dataframe_with_none_df():
    scenario = MockScenario()
    obj = DummyClass([scenario])

    obj.from_dataframe(None)

    assert scenario.received_queries is None


def test_from_dataframe_strips_and_deduplicates():
    scenario = MockScenario()
    obj = DummyClass([scenario])

    df = pd.DataFrame({"queries": [" a ", "a", "b", " B ", "nan", "NaN"]})

    obj.from_dataframe(df)

    assert scenario.received_queries == ["a", "b", "B"]


def test_from_dataframe_preserves_order():
    scenario = MockScenario()
    obj = DummyClass([scenario])

    df = pd.DataFrame({"queries": ["x", "y", "z", "x", "y"]})

    obj.from_dataframe(df)

    assert scenario.received_queries == ["x", "y", "z"]
