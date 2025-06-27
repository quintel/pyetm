from __future__ import annotations
from typing import Any, Type, TypeVar
from pydantic import BaseModel, PrivateAttr, ValidationError, ConfigDict

T = TypeVar("T", bound="Base")


class Base(BaseModel):
    """
    Custom base model that:
      - Collects non-breaking validation or runtime warnings
      - Fails fast on critical errors
      - Catches validation errors and converts them into warnings
      - Validates on assignment, converting assignment errors into warnings
    """

    # Enable assignment validation
    model_config = ConfigDict(validate_assignment=True)

    # Internal list of warnings (not part of serialized schema)
    _warnings: list[str] = PrivateAttr(default_factory=list)

    def __init__(self, **data: Any) -> None:
        # Ensure private warnings list exists before any validation
        object.__setattr__(self, "_warnings", [])
        try:
            super().__init__(**data)
        except ValidationError as e:
            # Construct without validation to preserve fields
            inst = self.__class__.model_construct(**data)
            # Copy field data
            object.__setattr__(self, "__dict__", inst.__dict__.copy())
            # Ensure warnings list on this instance
            if not hasattr(self, "_warnings"):
                object.__setattr__(self, "_warnings", [])
            # Convert each validation error into a warning
            for err in e.errors():
                loc = ".".join(str(x) for x in err.get("loc", []))
                msg = err.get("msg", "")
                self.add_warning(f"{loc}: {msg}")

    def __setattr__(self, name: str, value: Any) -> None:
        # Intercept assignment-time validation errors
        try:
            super().__setattr__(name, value)
        except ValidationError as e:
            # Add warning instead of raising
            for err in e.errors():
                loc = ".".join(str(x) for x in err.get("loc", []))
                msg = err.get("msg", "")
                self.add_warning(f"Assignment {loc}: {msg}")
            # Do not assign invalid value

    def add_warning(self, message: str) -> None:
        """Append a warning message to this model."""
        self._warnings.append(message)

    @property
    def warnings(self) -> list[str]:
        """Return a copy of the warnings list."""
        return list(self._warnings)

    def show_warnings(self) -> None:
        """Print all warnings to the console."""
        if not self._warnings:
            print("No warnings.")
            return
        print("Warnings:")
        for i, w in enumerate(self._warnings, start=1):
            print(f" {i}. {w}")

    def _merge_submodel_warnings(self, submodel: Any) -> None:
        """
        Bring warnings from a nested Base (or list thereof)
        into this model's warnings list.
        """
        from typing import Iterable

        def _collect(wm: Base):
            for w in wm.warnings:
                self.add_warning(f"{wm.__class__.__name__}: {w}")

        if isinstance(submodel, Base):
            _collect(submodel)
        elif isinstance(submodel, Iterable):
            for item in submodel:
                if isinstance(item, Base):
                    _collect(item)

    @classmethod
    def load_safe(cls: Type[T], **data: Any) -> T:
        """
        Alternate constructor that always returns an instance,
        converting all validation errors into warnings.
        """
        return cls(**data)
