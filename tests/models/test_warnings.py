from datetime import datetime
from pyetm.models.warnings import ModelWarning, WarningCollector

# ----------------ModelWarning----------------


def test_warning_creation():
    """Test basic ModelWarning object creation."""
    warning = ModelWarning(field="test_field", message="Test message")

    assert warning.field == "test_field"
    assert warning.message == "Test message"
    assert warning.severity == "warning"
    assert isinstance(warning.timestamp, datetime)


def test_warning_with_custom_severity():
    """Test ModelWarning creation with custom severity."""
    warning = ModelWarning(
        field="error_field", message="Error message", severity="error"
    )

    assert warning.field == "error_field"
    assert warning.message == "Error message"
    assert warning.severity == "error"


def test_warning_string_representation():
    """Test ModelWarning __str__ method."""
    warning = ModelWarning(field="field1", message="Test message")

    assert str(warning) == "field1: Test message"


def test_warning_repr():
    """Test ModelWarning __repr__ method."""
    warning = ModelWarning(field="field1", message="Test message", severity="error")

    repr_str = repr(warning)
    assert "ModelWarning(" in repr_str
    assert "field='field1'" in repr_str
    assert "message='Test message'" in repr_str
    assert "severity='error'" in repr_str


def test_warning_to_dict():
    """Test ModelWarning serialization to dictionary."""
    warning = ModelWarning(field="field1", message="Test message", severity="info")
    result = warning.to_dict()

    assert result["field"] == "field1"
    assert result["message"] == "Test message"
    assert result["severity"] == "info"
    assert "timestamp" in result


# ----------------WarningCollector----------------


def test_warning_collector_creation():
    """Test basic WarningCollector creation."""
    collector = WarningCollector()

    assert len(collector) == 0
    assert not collector.has_warnings()
    assert collector.get_fields_with_warnings() == []


def test_add_simple_warning():
    """Test adding a simple string warning."""
    collector = WarningCollector()
    collector.add("field1", "Simple warning")

    assert len(collector) == 1
    assert collector.has_warnings("field1")
    assert not collector.has_warnings("field2")

    warnings = collector.get_by_field("field1")
    assert len(warnings) == 1
    assert warnings[0].message == "Simple warning"


def test_add_multiple_warnings_same_field():
    """Test adding multiple warnings to the same field."""
    collector = WarningCollector()

    collector.add("field1", "Warning 1")
    collector.add("field1", "Warning 2")

    assert len(collector) == 2
    warnings = collector.get_by_field("field1")
    assert len(warnings) == 2
    messages = [w.message for w in warnings]
    assert "Warning 1" in messages
    assert "Warning 2" in messages


def test_add_list_of_warnings():
    """Test adding a list of warning messages."""
    collector = WarningCollector()
    collector.add("field1", ["Warning 1", "Warning 2", "Warning 3"])

    assert len(collector) == 3
    warnings = collector.get_by_field("field1")
    messages = [w.message for w in warnings]
    assert "Warning 1" in messages
    assert "Warning 2" in messages
    assert "Warning 3" in messages


def test_add_nested_dict_warnings():
    """Test adding nested dictionary warnings (legacy pattern)."""
    collector = WarningCollector()
    nested_warnings = {
        "subfield1": ["Sub warning 1"],
        "subfield2": ["Sub warning 2", "Sub warning 3"],
    }
    collector.add("parent", nested_warnings)

    assert len(collector) == 3
    assert collector.has_warnings("parent.subfield1")
    assert collector.has_warnings("parent.subfield2")

    sub1_warnings = collector.get_by_field("parent.subfield1")
    assert len(sub1_warnings) == 1
    assert sub1_warnings[0].message == "Sub warning 1"

    sub2_warnings = collector.get_by_field("parent.subfield2")
    assert len(sub2_warnings) == 2


def test_add_warning_with_severity():
    """Test adding warnings with different severities."""
    collector = WarningCollector()

    collector.add("field1", "Info message", "info")
    collector.add("field2", "Warning message", "warning")
    collector.add("field3", "Error message", "error")

    assert len(collector) == 3

    info_warning = collector.get_by_field("field1")[0]
    assert info_warning.severity == "info"

    warning_warning = collector.get_by_field("field2")[0]
    assert warning_warning.severity == "warning"

    error_warning = collector.get_by_field("field3")[0]
    assert error_warning.severity == "error"


def test_clear_all_warnings():
    """Test clearing all warnings."""
    collector = WarningCollector()
    collector.add("field1", "Warning 1")
    collector.add("field2", "Warning 2")

    assert len(collector) == 2

    collector.clear()

    assert len(collector) == 0
    assert not collector.has_warnings()


def test_clear_specific_field():
    """Test clearing warnings for a specific field."""
    collector = WarningCollector()
    collector.add("field1", "Warning 1")
    collector.add("field2", "Warning 2")
    collector.add("field1", "Warning 3")

    assert len(collector) == 3

    collector.clear("field1")

    assert len(collector) == 1
    assert not collector.has_warnings("field1")
    assert collector.has_warnings("field2")


def test_get_fields_with_warnings():
    """Test getting list of fields that have warnings."""
    collector = WarningCollector()
    collector.add("field1", "Warning 1")
    collector.add("field2", "Warning 2")
    collector.add("field1", "Warning 3")

    fields = collector.get_fields_with_warnings()

    assert len(fields) == 2
    assert "field1" in fields
    assert "field2" in fields


def test_to_dict():
    """Test conversion to detailed dictionary format."""
    collector = WarningCollector()
    collector.add("field1", "Warning 1", "error")

    result = collector.to_dict()

    assert "field1" in result
    assert len(result["field1"]) == 1
    warning_dict = result["field1"][0]
    assert warning_dict["field"] == "field1"
    assert warning_dict["message"] == "Warning 1"
    assert warning_dict["severity"] == "error"
    assert "timestamp" in warning_dict


def test_merge_from_another_collector():
    """Test merging warnings from another collector."""
    collector1 = WarningCollector()
    collector1.add("field1", "Main warning")

    collector2 = WarningCollector()
    collector2.add("sub_field", "Sub warning")

    collector1.merge_from(collector2, "SubModel")

    assert len(collector1) == 2
    assert collector1.has_warnings("field1")
    assert collector1.has_warnings("SubModel.sub_field")


def test_merge_from_without_prefix():
    """Test merging warnings without prefix."""
    collector1 = WarningCollector()
    collector1.add("field1", "Warning 1")

    collector2 = WarningCollector()
    collector2.add("field2", "Warning 2")

    collector1.merge_from(collector2)

    assert len(collector1) == 2
    assert collector1.has_warnings("field1")
    assert collector1.has_warnings("field2")


def test_collector_bool_evaluation():
    """Test WarningCollector boolean evaluation."""
    collector = WarningCollector()

    assert not collector  # Empty collector is falsy

    collector.add("field1", "Warning")

    assert collector  # Non-empty collector is truthy


def test_collector_iteration():
    """Test iterating over WarningCollector."""
    collector = WarningCollector()
    collector.add("field1", "Warning 1")
    collector.add("field2", "Warning 2")

    warnings = list(collector)

    assert len(warnings) == 2
    assert all(isinstance(w, ModelWarning) for w in warnings)
    messages = [w.message for w in warnings]
    assert "Warning 1" in messages
    assert "Warning 2" in messages


def test_collector_repr_empty():
    """Test WarningCollector __repr__ when empty."""
    collector = WarningCollector()

    repr_str = repr(collector)

    assert "no warnings" in repr_str


def test_collector_repr_with_warnings():
    """Test WarningCollector __repr__ with warnings."""
    collector = WarningCollector()
    collector.add("field1", "Warning", "warning")
    collector.add("field2", "Error", "error")
    collector.add("field3", "Info", "info")

    repr_str = repr(collector)

    assert "3 warnings" in repr_str
    assert "1 warning" in repr_str
    assert "1 error" in repr_str
    assert "1 info" in repr_str
