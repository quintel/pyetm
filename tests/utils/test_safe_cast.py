from pyetm.utils.safe_cast import cast_bool, cast_int

def test_cast_bool():
    """Test cast_bool method"""
    na = float("nan")
    assert cast_bool(None) is None
    assert cast_bool(na) is None
    assert cast_bool(True) is True
    assert cast_bool(False) is False
    assert cast_bool(1) is True
    assert cast_bool(0.0) is False
    assert cast_bool("yes") is True
    assert cast_bool("No") is False
    assert cast_bool("1") is True
    assert cast_bool("maybe") is None

def test_cast_int():
    """Test cast_int method"""
    na = float("nan")
    assert cast_int(None) is None
    assert cast_int(na) is None
    assert cast_int(5) == 5
    assert cast_int(5.9) == 5
    assert cast_int("7") == 7
    assert cast_int("abc") is None
