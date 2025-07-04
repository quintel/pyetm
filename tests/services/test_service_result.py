import re
from pyetm.services.service_result import ServiceResult


def test_ok_default_errors():
    # ok with data and no errors
    result = ServiceResult.ok(data=123)
    assert result.success is True
    assert result.data == 123
    assert isinstance(result.errors, list)
    assert result.errors == []


def test_ok_with_warnings():
    # ok with data and warnings
    warnings = ["warn1", "warn2"]
    result = ServiceResult.ok(data={"key": "value"}, errors=warnings)
    assert result.success is True
    assert result.data == {"key": "value"}
    assert result.errors == warnings
    # modifying input warnings list should not affect result.errors
    warnings.append("warn3")
    assert result.errors == ["warn1", "warn2"]


def test_fail():
    # fail with errors
    errs = ["err1", "err2"]
    result = ServiceResult.fail(errs)
    assert result.success is False
    assert result.data is None
    assert result.errors == errs
    # modifying input errs list should not affect result.errors
    errs.append("err3")
    assert result.errors == ["err1", "err2"]


def test_repr_with_data():
    # repr shows '…' when data is not None
    result = ServiceResult.ok(data=[1, 2, 3])
    r = repr(result)
    assert "success=True" in r
    assert "errors=[]" in r
    # Since data is not None, '…' should appear
    assert re.search(r"data=…", r)


def test_repr_without_data():
    # repr shows None when data is None
    result = ServiceResult.fail(["error"])
    r = repr(result)
    assert "success=False" in r
    assert "errors=['error']" in r
    # Since data is None, repr should contain 'data=None'
    assert "data=None" in r
