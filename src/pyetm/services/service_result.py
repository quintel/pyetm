from __future__ import annotations
from dataclasses import dataclass, field
from typing import Generic, TypeVar, Optional

T = TypeVar("T")


@dataclass
class ServiceResult(Generic[T]):
    """
    A uniform result object for all service runners.
      - success=False means a breaking error: `data` will be None
      - success=True but non-empty `errors` means warnings only
      - data can be any type
    """

    success: bool
    data: Optional[T] = None
    errors: list[str] = field(default_factory=list)

    @classmethod
    def ok(cls, data: T, errors: Optional[list[str]] = None) -> ServiceResult[T]:
        """Use when your runner fetched data; pass warnings in `errors` if any."""
        # copy warnings list to avoid external mutations
        err_copy: list[str] = list(errors) if errors else []
        return cls(success=True, data=data, errors=err_copy)

    @classmethod
    def fail(cls, errors: list[str]) -> ServiceResult[None]:
        """Use when a critical error happened and you cannot proceed."""
        err_copy: list[str] = list(errors)
        return cls(success=False, data=None, errors=err_copy)

    def __repr__(self) -> str:
        # represent presence of data with an ellipsis, absence with None
        data_repr = "…" if self.data is not None else "None"
        return f"<ServiceResult success={self.success!r} errors={self.errors!r} data={data_repr}>"
