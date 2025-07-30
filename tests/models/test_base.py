from pyetm.models.base import Base


def test_valid_initialization_has_no_warnings(dummy_base_model):
    d = dummy_base_model(a=10, b="string", c=3.14)
    assert d.a == 10
    assert d.b == "string"
    assert d.c == 3.14
    assert d.warnings == {}


def test_invalid_initialization_becomes_warning_not_exception(dummy_base_model):
    d = dummy_base_model(a="not-an-int", b="hi")
    assert isinstance(d, dummy_base_model)
    assert any("valid integer" in w.lower() for w_list in d.warnings.values() for w in w_list)


def test_missing_required_field_becomes_warning(dummy_base_model):
    d = dummy_base_model(a=5)
    assert isinstance(d, dummy_base_model)
    assert any("field required" in w.lower() for w_list in d.warnings.values() for w in w_list)


def test_assignment_validation_generates_warning_and_skips_assignment(dummy_base_model):
    d = dummy_base_model(a=1, b="foo")
    d.warnings.clear()

    # good assignment
    d.a = 42
    assert d.a == 42
    assert d.warnings == {}

    # bad assignment
    d.b = 123
    assert d.b == "foo"
    assert len(d.warnings) == 1
    # actual message contains "valid string"
    assert any("valid string" in w.lower() for w_list in d.warnings.values() for w in w_list)


def test_merge_submodel_warnings_brings_them_up(dummy_base_model):
    class Child(Base):
        x: int

    child = Child(x="warning")
    assert child.warnings, "child should have at least one warning"

    parent = dummy_base_model(a=0, b="string")
    parent.warnings.clear()

    parent._merge_submodel_warnings(child, key_attr='x')

    assert parent.warnings[f'Child(x=warning)'] == child.warnings


def test_show_warnings_no_warnings_prints_no_warnings(capsys, dummy_base_model):
    d = dummy_base_model(a=3, b="string")
    # ensure no warnings
    d.warnings.clear()
    d.show_warnings()
    captured = capsys.readouterr()
    assert "No warnings." in captured.out.strip()


def test_merge_submodel_warnings_with_list(dummy_base_model):
    # Create two children
    class Child(Base):
        x: int

    c1 = Child(x="bad1")
    c2 = Child(x="bad2")
    # both have warnings
    assert c1.warnings and c2.warnings

    parent = dummy_base_model(a=1, b="string")
    parent.warnings.clear()

    # pass list of submodels
    parent._merge_submodel_warnings(c1, c2, key_attr='x')
    # expect two warnings, in order
    expected = {
        'Child(x=bad1)': {
            'x': ['Input should be a valid integer, unable to parse string as an integer']
        },
        'Child(x=bad2)': {
            'x': ['Input should be a valid integer, unable to parse string as an integer']
        }
    }
    assert parent.warnings == expected


def test_load_safe_always_constructs_and_warns(dummy_base_model):
    # load_safe should never raise, even if data is invalid
    data = {"a": "not-int", "b": 123}
    d = dummy_base_model.load_safe(**data)
    assert isinstance(d, dummy_base_model)
    # Contains both warnings
    msgs = [w.lower() for w_list in d.warnings.values() for w in w_list]
    assert any("valid integer" in m for m in msgs)
    assert any("valid string" in m or "field required" in m for m in msgs)
