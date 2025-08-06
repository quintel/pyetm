from pyetm.models.base import Base


def test_valid_initialization_has_no_warnings(dummy_base_model):
    """Test that valid initialization produces no warnings."""
    d = dummy_base_model(a=10, b="string", c=3.14)

    assert d.a == 10
    assert d.b == "string"
    assert d.c == 3.14
    assert len(d.warnings) == 0  # New: check length instead of empty dict
    assert not d.warnings.has_warnings()  # New: use has_warnings method


def test_invalid_initialization_becomes_warning_not_exception(dummy_base_model):
    """Test that invalid initialization creates warnings instead of exceptions."""
    d = dummy_base_model(a="not-an-int", b="hi")

    assert isinstance(d, dummy_base_model)
    assert len(d.warnings) > 0  # New: check that warnings exist

    # Check warning content using new API
    all_warnings = list(d.warnings)  # Get all ModelWarning objects
    warning_messages = [w.message.lower() for w in all_warnings]
    assert any("valid integer" in msg for msg in warning_messages)


def test_missing_required_field_becomes_warning(dummy_base_model):
    """Test that missing required fields become warnings."""
    d = dummy_base_model(a=5)

    assert isinstance(d, dummy_base_model)
    assert len(d.warnings) > 0

    # Check for field required warning
    all_warnings = list(d.warnings)
    warning_messages = [w.message.lower() for w in all_warnings]
    assert any("field required" in msg for msg in warning_messages)


def test_assignment_validation_generates_warning_and_skips_assignment(dummy_base_model):
    """Test that invalid assignments generate warnings and don't change the value."""
    d = dummy_base_model(a=1, b="foo")
    d.warnings.clear()  # Clear any initialization warnings

    # Good assignment should work
    d.a = 42
    assert d.a == 42
    assert len(d.warnings) == 0

    # Bad assignment should generate warning and not change value
    original_b = d.b
    d.b = 123  # Invalid: should be string

    assert d.b == original_b  # Value should not change
    assert len(d.warnings) == 1

    # Check warning content
    b_warnings = d.warnings.get_by_field("b")
    assert len(b_warnings) > 0
    assert any("valid string" in w.message.lower() for w in b_warnings)


def test_merge_submodel_warnings_brings_them_up(dummy_base_model):
    """Test that submodel warnings are properly merged with key attributes."""

    class Child(Base):
        x: int

        def _to_dataframe(self, **kwargs):
            import pandas as pd

            return pd.DataFrame({"x": [self.x]})

    child = Child(x="warning")  # Invalid value will create warnings
    assert len(child.warnings) > 0, "child should have at least one warning"

    parent = dummy_base_model(a=0, b="string")
    parent.warnings.clear()

    parent._merge_submodel_warnings(child, key_attr="x")

    # Check that warnings were merged with proper prefix
    parent_fields = parent.warnings.get_fields_with_warnings()
    assert any("Child(x=warning)" in field for field in parent_fields)

    # Check that the actual warning content is preserved
    assert len(parent.warnings) > 0


def test_show_warnings_no_warnings_prints_no_warnings(capsys, dummy_base_model):
    """Test show_warnings output when no warnings exist."""
    d = dummy_base_model(a=3, b="string")
    d.warnings.clear()  # Ensure no warnings

    d.show_warnings()

    captured = capsys.readouterr()
    assert "No warnings." in captured.out.strip()


def test_show_warnings_with_warnings_prints_formatted_output(capsys, dummy_base_model):
    """Test show_warnings output when warnings exist."""
    d = dummy_base_model(a="invalid", b="string")

    d.show_warnings()

    captured = capsys.readouterr()
    assert "Warnings:" in captured.out
    # Should contain field name and warning indicator
    assert "a:" in captured.out
    assert "[WARNING]" in captured.out or "[ERROR]" in captured.out


def test_merge_submodel_warnings_with_multiple_submodels(dummy_base_model):
    """Test merging warnings from multiple submodels."""

    class Child(Base):
        x: int

        def _to_dataframe(self, **kwargs):
            import pandas as pd

            return pd.DataFrame({"x": [self.x]})

    c1 = Child(x="bad1")
    c2 = Child(x="bad2")

    # Both should have warnings
    assert len(c1.warnings) > 0 and len(c2.warnings) > 0

    parent = dummy_base_model(a=1, b="string")
    parent.warnings.clear()

    # Merge warnings from both children
    parent._merge_submodel_warnings(c1, c2, key_attr="x")

    # Check that both were merged with proper prefixes
    parent_fields = parent.warnings.get_fields_with_warnings()
    assert any("Child(x=bad1)" in field for field in parent_fields)
    assert any("Child(x=bad2)" in field for field in parent_fields)

    # Should have warnings from both children
    assert len(parent.warnings) >= 2


def test_add_warning_manually(dummy_base_model):
    """Test manually adding warnings to a model."""
    d = dummy_base_model(a=1, b="string")
    d.warnings.clear()

    d.add_warning("custom_field", "Custom warning message")

    assert d.warnings.has_warnings("custom_field")
    custom_warnings = d.warnings.get_by_field("custom_field")
    assert len(custom_warnings) == 1
    assert custom_warnings[0].message == "Custom warning message"


def test_add_warning_with_severity(dummy_base_model):
    """Test adding warnings with different severity levels."""
    d = dummy_base_model(a=1, b="string")
    d.warnings.clear()

    d.add_warning("error_field", "Critical error", "error")
    d.add_warning("info_field", "Information", "info")

    error_warnings = d.warnings.get_by_field("error_field")
    info_warnings = d.warnings.get_by_field("info_field")

    assert error_warnings[0].severity == "error"
    assert info_warnings[0].severity == "info"


def test_clear_warnings_for_specific_field(dummy_base_model):
    """Test clearing warnings for a specific field."""
    d = dummy_base_model(a="invalid", b=123)  # Both invalid

    # Should have warnings for both fields
    assert d.warnings.has_warnings("a")
    assert d.warnings.has_warnings("b")

    # Clear warnings for field 'a' only
    d._clear_warnings_for_attr("a")

    assert not d.warnings.has_warnings("a")
    assert d.warnings.has_warnings("b")  # Should still have warnings for b


def test_warnings_property_returns_collector(dummy_base_model):
    """Test that the warnings property returns a WarningCollector."""
    d = dummy_base_model(a=1, b="string")

    from pyetm.models.warnings import WarningCollector

    assert isinstance(d.warnings, WarningCollector)


def test_assignment_clears_previous_warnings(dummy_base_model):
    """Test that valid assignment clears previous warnings for that field."""
    d = dummy_base_model(a=1, b="string")

    # Make invalid assignment to create warning
    d.a = "invalid"  # Should create warning
    assert d.warnings.has_warnings("a")

    # Make valid assignment - should clear warnings
    d.a = 42
    assert d.a == 42
    assert not d.warnings.has_warnings("a")


def test_get_serializable_fields(dummy_base_model):
    """Test _get_serializable_fields method."""
    d = dummy_base_model(a=1, b="string")

    fields = d._get_serializable_fields()

    assert "a" in fields
    assert "b" in fields
    # Should not include private fields
    assert all(not field.startswith("_") for field in fields)


def test_model_construction_with_partial_data(dummy_base_model):
    """Test model construction with missing optional fields."""
    # Assuming 'c' is optional in dummy_base_model
    d = dummy_base_model(a=1, b="string")

    assert d.a == 1
    assert d.b == "string"
    # Optional field 'c' should have default or None


def test_multiple_validation_errors_all_become_warnings(dummy_base_model):
    """Test that multiple validation errors all become warnings."""
    # Create model with multiple invalid fields
    d = dummy_base_model(a="not-int", b=123)  # Both invalid

    assert isinstance(d, dummy_base_model)
    assert len(d.warnings) >= 2  # Should have at least 2 warnings

    # Should have warnings for both fields
    assert d.warnings.has_warnings("a")
    assert d.warnings.has_warnings("b")


# Additional helper test for complex warning merging
def test_nested_warning_merging_preserves_structure(dummy_base_model):
    """Test that nested warning structures are preserved during merging."""
    d = dummy_base_model(a=1, b="string")
    d.warnings.clear()

    # Add complex nested warning structure (simulating legacy behavior)
    complex_warnings = {"sub1": ["Warning 1", "Warning 2"], "sub2": ["Warning 3"]}
    d.add_warning("parent", complex_warnings)

    # Should create nested field names
    fields = d.warnings.get_fields_with_warnings()
    assert "parent.sub1" in fields
    assert "parent.sub2" in fields

    # Check warning counts
    sub1_warnings = d.warnings.get_by_field("parent.sub1")
    sub2_warnings = d.warnings.get_by_field("parent.sub2")

    assert len(sub1_warnings) == 2
    assert len(sub2_warnings) == 1


def test_show_warnings_different_severities(capsys, dummy_base_model):
    """Test show_warnings output with different severity levels."""
    d = dummy_base_model(a=1, b="string")
    d.warnings.clear()

    # Add warnings with different severities
    d.add_warning("field1", "Information message", "info")
    d.add_warning("field2", "Warning message", "warning")
    d.add_warning("field3", "Error message", "error")

    d.show_warnings()

    captured = capsys.readouterr()
    assert "Warnings:" in captured.out
    assert "[INFO]" in captured.out
    assert "[WARNING]" in captured.out
    assert "[ERROR]" in captured.out
    assert "Information message" in captured.out
    assert "Warning message" in captured.out
    assert "Error message" in captured.out


def test_from_dataframe_not_implemented_creates_fallback(dummy_base_model):
    """Test that calling from_dataframe on base class creates fallback with warning."""
    import pandas as pd

    df = pd.DataFrame({"a": [1], "b": ["test"]})

    # The base class should create a fallback instance with warnings since _from_dataframe is not implemented
    instance = dummy_base_model.from_dataframe(df)

    assert isinstance(instance, dummy_base_model)
    assert len(instance.warnings) > 0
    # Check for warnings about the failure - the warning will be on 'from_dataframe' field
    from_dataframe_warnings = instance.warnings.get_by_field("from_dataframe")
    assert len(from_dataframe_warnings) > 0
    assert "must implement _from_dataframe" in from_dataframe_warnings[0].message


def test_from_dataframe_error_handling_creates_fallback_instance(dummy_base_model):
    """Test that from_dataframe creates fallback instance with warnings on error."""
    import pandas as pd

    # Override _from_dataframe to raise an error
    def failing_from_dataframe(cls, df, **kwargs):
        raise ValueError("Intentional test error")

    dummy_base_model._from_dataframe = classmethod(failing_from_dataframe)

    df = pd.DataFrame({"a": [1], "b": ["test"]})

    # Should not raise, but create instance with warnings
    instance = dummy_base_model.from_dataframe(df)

    assert isinstance(instance, dummy_base_model)
    assert len(instance.warnings) > 0
    # Check for warnings about the failure - the warning will be on 'from_dataframe' field
    from_dataframe_warnings = instance.warnings.get_by_field("from_dataframe")
    assert len(from_dataframe_warnings) > 0
    assert "Failed to create from DataFrame" in from_dataframe_warnings[0].message


def test_from_dataframe_successful_delegation():
    """Test that from_dataframe properly delegates to _from_dataframe."""
    import pandas as pd
    from pyetm.models.base import Base

    class TestModel(Base):
        x: int
        y: str

        def _to_dataframe(self, **kwargs):
            return pd.DataFrame({"x": [self.x], "y": [self.y]})

        @classmethod
        def _from_dataframe(cls, df, **kwargs):
            row = df.iloc[0]
            return cls(x=row["x"], y=row["y"])

    # Test the successful path
    df = pd.DataFrame({"x": [42], "y": ["hello"]})
    instance = TestModel.from_dataframe(df)

    assert instance.x == 42
    assert instance.y == "hello"
    assert len(instance.warnings) == 0
