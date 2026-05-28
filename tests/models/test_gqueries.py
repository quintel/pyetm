from pyetm.models import Gqueries


def test_queries_from_list(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    assert not queries.is_ready()
    assert valid_queries[0] == queries.query_keys()[0]


def test_update(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    assert not queries.is_ready()

    queries.update({valid_queries[0]: 20.5, valid_queries[1]: 1.0})

    assert queries.is_ready()
    assert queries.get(valid_queries[0]) == 20.5

    assert queries.get("invalid_query") is None


def test_add_one_query(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.add("extra_query")

    assert not queries.is_ready()
    assert "extra_query" in queries.query_keys()
    assert queries.get("extra_query") is None
    assert valid_queries[0] in queries.query_keys()
    assert queries.get(valid_queries[0]) is None


def test_add_one_query_when_queries_were_already_run(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.update({valid_queries[0]: 20.5, valid_queries[1]: 1.0})

    queries.add("extra_query")

    assert not queries.is_ready()
    assert "extra_query" in queries.query_keys()
    assert queries.get("extra_query") is None
    assert queries.get(valid_queries[0]) == 20.5


def test_add_mulitple_queries(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.add("extra_query", "extra_query_2")

    assert not queries.is_ready()
    assert "extra_query" in queries.query_keys()
    assert "extra_query_2" in queries.query_keys()

    assert queries.get("extra_query") is None
    assert queries.get("extra_query_2") is None


def test_add_multiple_queries_but_one_is_already_present(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.update({valid_queries[0]: 20.5, valid_queries[1]: 1.0})

    queries.add("extra_query", valid_queries[0])

    assert not queries.is_ready()
    assert "extra_query" in queries.query_keys()
    # Was not overwritten
    assert queries.get(valid_queries[0]) == 20.5


def test_to_dataframe(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.update(
        {
            valid_queries[0]: {"present": 0.0, "future": 20.5, "unit": "euros"},
            valid_queries[1]: {"present": 1.0, "future": 1.0, "unit": "PJ"},
        }
    )

    dataframe = queries.to_dataframe()
    dataframe = dataframe.reset_index(level="unit")
    assert dataframe.loc[valid_queries[0], "future"] == 20.5
    assert dataframe.loc[valid_queries[0], "unit"] == "euros"
    assert dataframe.loc[valid_queries[1], "future"] == 1.0
    assert dataframe.loc[valid_queries[1], "unit"] == "PJ"


def test_update_with_curve_result():
    """Test that curve results filter out the present key"""
    queries = Gqueries.from_list(["my_curve_query"])

    # Simulate API response with curve result
    curve_data = list(range(8760))  # Simplified 8760 hourly values
    queries.update(
        {
            "my_curve_query": {
                "present": [],  # API returns empty array for present
                "future": curve_data,
                "unit": "curve",
            }
        }
    )

    result = queries.get("my_curve_query")

    # Present key should be filtered out
    assert result["future"] == curve_data
    assert result["unit"] == "curve"


def test_update_with_mixed_results():
    """Test that curve and scalar results can coexist"""
    queries = Gqueries.from_list(["scalar_query", "curve_query"])

    curve_data = [1.5, 2.5, 3.5] * 2920  # 8760 values
    queries.update(
        {
            "scalar_query": {"present": 0.0, "future": 20.5, "unit": "euros"},
            "curve_query": {
                "present": [],
                "future": curve_data,
                "unit": "curve",
            },
        }
    )

    # Scalar result unchanged
    scalar_result = queries.get("scalar_query")
    assert "present" in scalar_result
    assert scalar_result["present"] == 0.0
    assert scalar_result["future"] == 20.5

    curve_result = queries.get("curve_query")
    assert curve_result["future"] == curve_data
    assert curve_result["unit"] == "curve"


def test_to_dataframe_with_curve_results():
    """Test that DataFrame handles curve results stored as objects"""
    queries = Gqueries.from_list(["scalar_query", "curve_query"])

    curve_data = [1.0, 2.0, 3.0] * 2920
    queries.update(
        {
            "scalar_query": {"present": 0.0, "future": 20.5, "unit": "euros"},
            "curve_query": {"present": [], "future": curve_data, "unit": "curve"},
        }
    )

    dataframe = queries.to_dataframe()
    dataframe = dataframe.reset_index(level="unit")

    # Scalar value is numeric
    assert dataframe.loc["scalar_query", "future"] == 20.5

    # Curve value is stored as object (list)
    curve_value = dataframe.loc["curve_query", "future"]
    assert isinstance(curve_value, list)
    assert len(curve_value) == 8760
    assert curve_value == curve_data


def test_remove_valid_queries(valid_queries):
    """Test removing valid query keys from collection"""
    queries = Gqueries.from_list(valid_queries)
    queries.update({valid_queries[0]: 20.5, valid_queries[1]: 1.0})

    # Remove one query
    queries.remove(valid_queries[0])

    assert valid_queries[0] not in queries.query_keys()
    assert valid_queries[1] in queries.query_keys()
    assert queries.get(valid_queries[0]) is None
    assert queries.get(valid_queries[1]) == 1.0


def test_remove_multiple_queries(valid_queries):
    """Test removing multiple query keys at once"""
    queries = Gqueries.from_list(valid_queries)
    queries.update({valid_queries[0]: 20.5, valid_queries[1]: 1.0})

    # Remove both queries
    queries.remove(valid_queries[0], valid_queries[1])

    assert valid_queries[0] not in queries.query_keys()
    assert valid_queries[1] not in queries.query_keys()
    assert len(queries.query_keys()) == 0


def test_remove_invalid_query_warns(valid_queries):
    """Test that removing non-existent query key produces warning"""
    queries = Gqueries.from_list(valid_queries)

    # Remove non-existent query
    queries.remove("non_existent_query")

    # Should have warning
    assert len(queries.warnings) > 0
    assert queries.warnings.has_warnings("non_existent_query")
    warning_msgs = [w.message for w in queries.warnings.get_by_field("non_existent_query")]
    assert any("not found in collection" in msg for msg in warning_msgs)


def test_remove_clears_stale_warnings(valid_queries):
    """Test that warnings auto-clear on each remove() call"""
    queries = Gqueries.from_list(valid_queries)

    # First removal with invalid key
    queries.remove("invalid_key_1")
    assert len(queries.warnings) > 0

    # Second removal with valid key should clear previous warnings
    queries.remove(valid_queries[0])
    # Warnings should be cleared (no warnings for valid removal)
    assert len(queries.warnings) == 0


def test_clear_removes_all_queries(valid_queries):
    """Test that clear() removes all queries and warnings"""
    queries = Gqueries.from_list(valid_queries)
    queries.update({valid_queries[0]: 20.5, valid_queries[1]: 1.0})

    # Add a warning manually
    queries.add_warning("test", "test warning")
    assert len(queries.warnings) > 0

    # Clear all
    queries.clear()

    assert len(queries.query_keys()) == 0
    assert len(queries.warnings) == 0
