from pyetm.models.base import Base


# TODO: consider extracting Dummy to a fixture or using another model fixture in the long run
class Dummy(Base):
    a: int
    b: str
    c: float = 1.23  # default value


def test_valid_initialization_has_no_warnings():
    d = Dummy(a=10, b="string", c=3.14)
    assert d.a == 10
    assert d.b == "string"
    assert d.c == 3.14
    assert d.warnings == []


def test_invalid_initialization_becomes_warning_not_exception():
    d = Dummy(a="not-an-int", b="hi")
    assert isinstance(d, Dummy)
    assert any("valid integer" in w.lower() for w in d.warnings)


def test_missing_required_field_becomes_warning():
    d = Dummy(a=5)
    assert isinstance(d, Dummy)
    assert any("field required" in w.lower() for w in d.warnings)


def test_assignment_validation_generates_warning_and_skips_assignment():
    d = Dummy(a=1, b="foo")
    d.warnings.clear()

    # good assignment
    d.a = 42
    assert d.a == 42
    assert d.warnings == []

    # bad assignment
    d.b = 123
    assert d.b == "foo"
    assert len(d.warnings) == 1
    # actual message contains “valid string”
    assert any("valid string" in w.lower() for w in d.warnings)


def test_merge_submodel_warnings_brings_them_up():
    # child that will warn on init
    class Child(Base):
        x: int

    child = Child(x="warning")
    assert child.warnings, "child should have at least one warning"

    parent = Dummy(a=0, b="string")
    parent.warnings.clear()

    parent._merge_submodel_warnings(child)
    assert parent.warnings == [f"Child: {child.warnings[0]}"]


def test_show_warnings_no_warnings_prints_no_warnings(capsys):
    d = Dummy(a=3, b="string")
    # ensure no warnings
    d.warnings.clear()
    d.show_warnings()
    captured = capsys.readouterr()
    assert "No warnings." in captured.out.strip()


def test_merge_submodel_warnings_with_list():
    # Create two children
    class Child(Base):
        x: int

    c1 = Child(x="bad1")
    c2 = Child(x="bad2")
    # both have warnings
    assert c1.warnings and c2.warnings

    parent = Dummy(a=1, b="string")
    parent.warnings.clear()

    # pass list of submodels
    parent._merge_submodel_warnings([c1, c2])
    # expect two warnings, in order
    expected = [f"Child: {c1.warnings[0]}", f"Child: {c2.warnings[0]}"]
    assert parent.warnings == expected


def test_load_safe_always_constructs_and_warns():
    # load_safe should never raise, even if data is invalid
    data = {"a": "not-int", "b": 123}
    d = Dummy.load_safe(**data)
    assert isinstance(d, Dummy)
    # Contains both warnings
    msgs = [w.lower() for w in d.warnings]
    assert any("valid integer" in m for m in msgs)
    assert any("valid string" in m or "field required" in m for m in msgs)
