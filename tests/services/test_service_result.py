import pytest
from typing import Dict

from pyetm.services.service_result import (
    ServiceResult,
    RequestsSessionError,
    AuthenticationError,
    GenericError,
)

def test_service_result_minimal_success():
    """Minimal success result has success=True and no data/errors/status."""
    result = ServiceResult(success=True)
    assert result.success is True
    assert result.data is None
    assert result.errors is None
    assert result.status_code is None

def test_service_result_success_with_data_and_status():
    """You can supply data and an HTTP status code on success."""
    payload = {"foo": "bar"}
    result = ServiceResult[Dict[str, str]](
        success=True,
        data=payload,
        status_code=200
    )
    assert result.success is True
    assert result.data == payload
    assert result.status_code == 200
    assert result.errors is None

def test_service_result_failure_with_errors_and_status():
    """On failure, errors and status_code should be propagated; data stays None."""
    errs = ["Bad gateway", "Try again later"]
    result = ServiceResult(
        success=False,
        errors=errs,
        status_code=502
    )
    assert result.success is False
    assert result.errors == errs
    assert result.status_code == 502
    assert result.data is None

def test_service_result_genericity_works():
    """ServiceResult should accept different type parameters without runtime issues."""
    # Even though Python erases generics at runtime, instantiation should still succeed
    int_result = ServiceResult[int](success=True, data=7)
    str_result = ServiceResult[str](success=True, data="seven")
    assert isinstance(int_result.data, int)
    assert isinstance(str_result.data, str)

def test_exception_hierarchy():
    """AuthenticationError and GenericError should subclass RequestsSessionError."""
    auth = AuthenticationError("401 Unauthorized")
    gen  = GenericError("HTTP 500: boom")

    assert isinstance(auth, RequestsSessionError)
    assert isinstance(gen, RequestsSessionError)

    assert issubclass(AuthenticationError, RequestsSessionError)
    assert issubclass(GenericError, RequestsSessionError)

def test_catching_specific_errors():
    """You can catch GenericError and AuthenticationError separately."""
    with pytest.raises(AuthenticationError):
        raise AuthenticationError("denied")

    with pytest.raises(GenericError):
        raise GenericError("something else")
