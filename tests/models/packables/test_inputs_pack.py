import pandas as pd
from unittest.mock import Mock, patch
import numpy as np

from pyetm.models.packables.inputs_pack import InputsPack


class DummyInput:
    def __init__(self, key, user, default=None, min_val=None, max_val=None):
        self.key = key
        self.user = user
        self.default = default
        self.min = min_val
        self.max = max_val


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


# Existing tests
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


# New tests for 100% coverage


def test_class_variables():
    """Test class variables are set correctly."""
    assert InputsPack.key == "inputs"
    assert InputsPack.sheet_name == "SLIDER_SETTINGS"


def test_init_with_kwargs():
    """Test initialization with kwargs."""
    pack = InputsPack(some_param="value")
    assert pack._scenario_short_names == {}


def test_set_scenario_short_names_with_none():
    """Test setting short names with None value."""
    pack = InputsPack()
    pack.set_scenario_short_names(None)
    assert pack._scenario_short_names == {}


def test_get_scenario_display_key_with_non_string_identifier():
    """Test _get_scenario_display_key when identifier returns non-string/int."""
    s = make_scenario(1)
    s.identifier.return_value = {"complex": "object"}

    pack = InputsPack()
    result = pack._get_scenario_display_key(s)
    assert result == 1  # Falls back to ID


def test_resolve_scenario_with_none():
    """Test resolve_scenario with None input."""
    pack = InputsPack()
    assert pack.resolve_scenario(None) is None


def test_resolve_scenario_with_whitespace():
    """Test resolve_scenario strips whitespace."""
    s = make_scenario(1, "ID1")
    pack = InputsPack()
    pack.add(s)

    assert pack.resolve_scenario("  ID1  ") is s


def test_resolve_scenario_numeric_conversion_error():
    """Test resolve_scenario when numeric conversion fails."""
    s = make_scenario(1, "ID1")
    pack = InputsPack()
    pack.add(s)

    # Non-numeric string should not crash
    assert pack.resolve_scenario("not_a_number") is None


def test_extract_from_input_objects_no_key():
    """Test _extract_from_input_objects with input missing key attribute."""
    s = make_scenario(1)
    input_obj = Mock()
    input_obj.key = None  # Explicitly set key to None
    input_obj.user = 10
    s.inputs = [input_obj]

    pack = InputsPack()
    result = pack._extract_from_input_objects(s, "user")
    assert result == {}  # Should skip objects with None key


def test_extract_from_input_objects_exception():
    """Test _extract_from_input_objects with iteration exception."""
    s = make_scenario(1)
    s.inputs = Mock()
    s.inputs.__iter__ = Mock(side_effect=Exception("iteration failed"))

    pack = InputsPack()
    result = pack._extract_from_input_objects(s, "user")
    assert result == {}


def test_extract_from_dataframe_to_dataframe_exception():
    """Test _extract_from_dataframe when to_dataframe raises exception."""
    s = make_scenario(1)
    s.inputs = Mock()
    s.inputs.to_dataframe = Mock(side_effect=Exception("dataframe failed"))

    pack = InputsPack()
    result = pack._extract_from_dataframe(s, "user")
    assert result == {}


def test_extract_from_dataframe_none_result():
    """Test _extract_from_dataframe when to_dataframe returns None."""
    s = make_scenario(1)
    s.inputs = Mock()
    s.inputs.to_dataframe = Mock(return_value=None)

    pack = InputsPack()
    result = pack._extract_from_dataframe(s, "user")
    assert result == {}


def test_extract_from_dataframe_empty_result():
    """Test _extract_from_dataframe when to_dataframe returns empty DataFrame."""
    s = make_scenario(1)
    s.inputs = Mock()
    empty_df = pd.DataFrame()
    s.inputs.to_dataframe = Mock(return_value=empty_df)

    pack = InputsPack()
    result = pack._extract_from_dataframe(s, "user")
    assert result == {}


def test_normalize_dataframe_index_no_multiindex():
    """Test _normalize_dataframe_index with regular index."""
    pack = InputsPack()
    df = pd.DataFrame({"user": [1, 2]}, index=["a", "b"])
    result = pack._normalize_dataframe_index(df)
    assert result.equals(df)


def test_normalize_dataframe_index_no_unit_level():
    """Test _normalize_dataframe_index with MultiIndex but no 'unit' level."""
    pack = InputsPack()
    df = pd.DataFrame(
        {"user": [1, 2]},
        index=pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y")], names=["key", "other"]
        ),
    )
    result = pack._normalize_dataframe_index(df)
    assert result.equals(df)


def test_dataframe_to_series_with_series_input():
    """Test _dataframe_to_series when input is already a Series."""
    pack = InputsPack()
    series = pd.Series([1, 2], index=["a", "b"])
    result = pack._dataframe_to_series(series, "user")
    assert result.equals(series)


def test_dataframe_to_series_with_default_field():
    """Test _dataframe_to_series finding 'default' column."""
    pack = InputsPack()
    df = pd.DataFrame({"default": [1, 2], "other": [3, 4]}, index=["a", "b"])
    result = pack._dataframe_to_series(df, "missing_field")
    assert result.equals(df["default"])


def test_dataframe_to_series_with_value_field():
    """Test _dataframe_to_series finding 'value' column."""
    pack = InputsPack()
    df = pd.DataFrame({"value": [1, 2], "other": [3, 4]}, index=["a", "b"])
    result = pack._dataframe_to_series(df, "missing_field")
    assert result.equals(df["value"])


def test_dataframe_to_series_fallback_to_first_column():
    """Test _dataframe_to_series falling back to first column."""
    pack = InputsPack()
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=["a", "b"])
    result = pack._dataframe_to_series(df, "missing_field")
    assert result.equals(df.iloc[:, 0])


def test_build_consolidated_dataframe_no_scenarios():
    """Test _build_consolidated_dataframe with no scenarios."""
    pack = InputsPack()
    result = pack._build_consolidated_dataframe({})
    assert result.empty


def test_build_consolidated_dataframe_no_relevant_scenarios():
    """Test _build_consolidated_dataframe with no relevant scenarios in field_mappings."""
    s = make_scenario(1)
    pack = InputsPack()
    pack.add(s)

    result = pack._build_consolidated_dataframe({})  # Empty field mappings
    assert result.empty


def test_build_consolidated_dataframe_no_input_keys():
    """Test _build_consolidated_dataframe when no input keys are found."""
    s = make_scenario(1)
    s.inputs = []
    pack = InputsPack()
    pack.add(s)

    result = pack._build_consolidated_dataframe({s: ["user"]})
    assert result.empty


def test_build_consolidated_dataframe_no_scenario_frames():
    """Test _build_consolidated_dataframe when no scenario frames are built."""
    s = make_scenario(1)
    s.inputs = Mock()
    s.inputs.__iter__ = Mock(side_effect=Exception())
    s.inputs.to_dataframe = Mock(side_effect=Exception())
    pack = InputsPack()
    pack.add(s)

    result = pack._build_consolidated_dataframe({s: ["user"]})
    assert result.empty


def test_build_scenario_data_empty_fields():
    """Test _build_scenario_data with empty fields list."""
    s = make_scenario(1)
    pack = InputsPack()

    result = pack._build_scenario_data(s, [], ["a", "b"])
    assert result == {}


def test_build_simple_dataframe_no_scenarios():
    """Test _build_simple_dataframe with no scenarios."""
    pack = InputsPack()
    result = pack._build_simple_dataframe()
    assert result.empty


def test_build_simple_dataframe_no_input_keys():
    """Test _build_simple_dataframe when no input keys found."""
    s = make_scenario(1)
    s.inputs = []

    pack = InputsPack()
    pack.add(s)

    result = pack._build_simple_dataframe()
    assert result.empty


def test_build_bounds_dataframe_no_scenarios():
    """Test _build_bounds_dataframe with no scenarios."""
    pack = InputsPack()
    result = pack._build_bounds_dataframe()
    assert result.empty


def test_build_bounds_dataframe_no_input_keys():
    """Test _build_bounds_dataframe when no input keys found."""
    s = make_scenario(1)
    s.inputs = []

    pack = InputsPack()
    pack.add(s)

    result = pack._build_bounds_dataframe()
    assert result.empty


def test_build_bounds_dataframe_from_objects():
    """Test _build_bounds_dataframe extracting from input objects."""
    s = make_scenario(1)
    s.inputs = [
        DummyInput("a", 10, min_val=0, max_val=100),
        DummyInput("b", 20, min_val=5, max_val=50),
    ]
    pack = InputsPack()
    pack.add(s)

    result = pack._build_bounds_dataframe()
    assert not result.empty
    assert ("", "min") in result.columns
    assert ("", "max") in result.columns


def test_build_bounds_dataframe_from_dataframe_exception():
    """Test _build_bounds_dataframe when both input iteration and dataframe fail."""
    s = make_scenario(1)
    s.inputs = Mock()
    s.inputs.__iter__ = Mock(side_effect=Exception())
    s.inputs.to_dataframe = Mock(side_effect=Exception())
    pack = InputsPack()
    pack.add(s)

    result = pack._build_bounds_dataframe()
    assert result.empty


def test_build_bounds_dataframe_early_break():
    """Test _build_bounds_dataframe early break when all values found."""
    s1 = make_scenario(1)
    s1.inputs = [DummyInput("a", 10, min_val=0, max_val=100)]
    s2 = make_scenario(2)
    s2.inputs = [DummyInput("a", 20, min_val=0, max_val=100)]
    pack = InputsPack()
    pack.add(s1, s2)

    result = pack._build_bounds_dataframe()
    assert not result.empty
    # Should have values from first scenario due to early break


def test_to_dataframe_empty_columns():
    """Test _to_dataframe with empty columns parameter."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10)]
    pack = InputsPack()
    pack.add(s)

    result = pack._to_dataframe(columns="")
    assert not result.empty


def test_to_dataframe_non_string_columns():
    """Test _to_dataframe with non-string columns parameter."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10)]
    pack = InputsPack()
    pack.add(s)

    result = pack._to_dataframe(columns=123)
    assert not result.empty


def test_to_dataframe_per_scenario_fields():
    """Test to_dataframe_per_scenario_fields."""
    s1 = make_scenario(1)
    s1.inputs = [DummyInput("a", 10, default=5)]
    s2 = make_scenario(2)
    s2.inputs = [DummyInput("a", 20)]
    pack = InputsPack()
    pack.add(s1, s2)

    fields_map = {s1: ["user", "default"], s2: ["user"]}
    result = pack.to_dataframe_per_scenario_fields(fields_map)
    assert not result.empty


def test_to_dataframe_defaults():
    """Test to_dataframe_defaults."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10, default=5)]
    pack = InputsPack()
    pack.add(s)

    result = pack.to_dataframe_defaults()
    assert not result.empty


def test_to_dataframe_min_max():
    """Test to_dataframe_min_max."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10, min_val=0, max_val=100)]
    pack = InputsPack()
    pack.add(s)

    result = pack.to_dataframe_min_max()
    assert not result.empty


def test_from_dataframe_exception_handling(caplog):
    """Test from_dataframe general exception handling."""
    pack = InputsPack()
    df = Mock()
    df.empty = False
    df.dropna = Mock(side_effect=Exception("processing failed"))

    with caplog.at_level("WARNING"):
        pack.from_dataframe(df)
        assert "Failed to parse simplified SLIDER_SETTINGS sheet" in caplog.text


def test_from_dataframe_empty_after_dropna():
    """Test from_dataframe when DataFrame is empty after dropna."""
    pack = InputsPack()
    df = pd.DataFrame([[None, None], [None, None]])
    pack.from_dataframe(df)  # Should return early


def test_is_blank_value():
    """Test _is_blank_value method."""
    pack = InputsPack()

    assert pack._is_blank_value(None) is True
    assert pack._is_blank_value(np.nan) is True
    assert pack._is_blank_value("") is True
    assert pack._is_blank_value("  ") is True
    assert pack._is_blank_value("nan") is True
    assert pack._is_blank_value("NaN") is True
    assert pack._is_blank_value(0) is False
    assert pack._is_blank_value("0") is False
    assert pack._is_blank_value("value") is False


def test_build_combined_dataframe_no_scenarios():
    """Test build_combined_dataframe with no scenarios."""
    pack = InputsPack()
    result = pack.build_combined_dataframe()
    assert result.empty


def test_build_combined_dataframe_defaults_only():
    """Test build_combined_dataframe with defaults only."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10, default=5)]
    pack = InputsPack()
    pack.add(s)
    result = pack.build_combined_dataframe(include_defaults=True, include_min_max=False)
    assert not result.empty


def test_build_combined_dataframe_user_with_bounds():
    """Test build_combined_dataframe with user values and bounds but no defaults."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10, min_val=0, max_val=100)]
    pack = InputsPack()
    pack.add(s)
    result = pack.build_combined_dataframe(include_defaults=False, include_min_max=True)
    assert isinstance(result, pd.DataFrame)


def test_build_combined_dataframe_full():
    """Test build_combined_dataframe with all options."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10, default=5, min_val=0, max_val=100)]
    pack = InputsPack()
    pack.add(s)
    result = pack.build_combined_dataframe(include_defaults=True, include_min_max=True)


def test_build_full_combined_dataframe_exception():
    """Test _build_full_combined_dataframe exception handling."""
    pack = InputsPack()
    with patch.object(pack, "_build_consolidated_dataframe", side_effect=Exception()):
        with patch.object(pack, "_build_bounds_dataframe", return_value=pd.DataFrame()):
            result = pack._build_full_combined_dataframe()


def test_build_full_combined_dataframe_empty_core():
    """Test _build_full_combined_dataframe with empty core DataFrame."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10, min_val=0, max_val=100)]

    pack = InputsPack()
    pack.add(s)

    with patch.object(
        pack, "_build_consolidated_dataframe", return_value=pd.DataFrame()
    ):
        result = pack._build_full_combined_dataframe()


def test_build_full_combined_dataframe_empty_bounds():
    """Test _build_full_combined_dataframe with empty bounds DataFrame."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10, default=5)]

    pack = InputsPack()
    pack.add(s)

    with patch.object(pack, "_build_bounds_dataframe", return_value=pd.DataFrame()):
        result = pack._build_full_combined_dataframe()


def test_log_scenario_input_warnings():
    """Test _log_scenario_input_warnings with scenario having _inputs."""
    s = make_scenario(1)
    mock_inputs = Mock()
    mock_inputs.log_warnings = Mock()
    s._inputs = mock_inputs
    pack = InputsPack()
    pack._log_scenario_input_warnings(s)
    mock_inputs.log_warnings.assert_called_once()


def test_log_scenario_input_warnings_no_inputs():
    """Test _log_scenario_input_warnings with scenario missing _inputs."""
    s = make_scenario(1)
    pack = InputsPack()
    pack._log_scenario_input_warnings(s)


def test_log_scenario_input_warnings_none_inputs():
    """Test _log_scenario_input_warnings with _inputs = None."""
    s = make_scenario(1)
    s._inputs = None
    pack = InputsPack()
    pack._log_scenario_input_warnings(s)


def test_log_scenario_input_warnings_exception():
    """Test _log_scenario_input_warnings with exception during logging."""
    s = make_scenario(1)
    mock_inputs = Mock()
    mock_inputs.log_warnings = Mock(side_effect=Exception("logging failed"))
    s._inputs = mock_inputs
    pack = InputsPack()
    pack._log_scenario_input_warnings(s)


def test_from_dataframe_calls_log_warnings():
    """Test from_dataframe calls _log_scenario_input_warnings."""
    s = make_scenario(1, "S1")
    pack = InputsPack()
    pack.add(s)

    with patch.object(pack, "_log_scenario_input_warnings") as mock_log:
        with patch(
            "pyetm.models.packables.inputs_pack.InputsPack.first_non_empty_row_positions",
            return_value=[0],
        ):
            df = pd.DataFrame([["input", "S1"], ["a", 10]])
            pack.from_dataframe(df)
            mock_log.assert_called_once_with(s)


def test_extract_input_values_prefers_objects():
    """Test _extract_input_values prefers input objects over dataframe."""
    s = make_scenario(1)
    s.inputs = [DummyInput("a", 10)]

    pack = InputsPack()

    with patch.object(pack, "_extract_from_dataframe") as mock_df:
        result = pack._extract_input_values(s, "user")
        mock_df.assert_not_called()
        assert result == {"a": 10}


def test_extract_input_values_fallback_to_dataframe():
    """Test _extract_input_values falls back to dataframe."""
    s = make_scenario(1)
    s.inputs = Mock()
    s.inputs.__iter__ = Mock(side_effect=Exception())
    s.inputs.to_dataframe = Mock(return_value=pd.DataFrame({"user": [10]}, index=["a"]))

    pack = InputsPack()
    result = pack._extract_input_values(s, "user")
    assert result == {"a": 10}
