from dataclasses import dataclass
from typing import Generic, TypeVar, Optional, List

T = TypeVar('T')

@dataclass
class ServiceResult(Generic[T]):
    """
    Generic wrapper for service operations.
    """
    success: bool
    data: Optional[T] = None
    errors: Optional[List[str]] = None
    status_code: Optional[int] = None


class RequestsSessionError(Exception):
    """Base class for all ETM HTTP session errors."""


class AuthenticationError(RequestsSessionError):
    """Raised when the API returns HTTP 401 Unauthorized."""


class GenericError(RequestsSessionError):
    """Raised when the API returns any other 4xx or 5xx response."""
