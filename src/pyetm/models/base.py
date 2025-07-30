from __future__ import annotations
from typing import Any, Type, TypeVar
from pydantic import BaseModel, PrivateAttr, ValidationError, ConfigDict
from pydantic_core import InitErrorDetails, PydanticCustomError
import pandas as pd

T = TypeVar("T", bound="Base")


class Base(BaseModel):
    """
    Custom base model that:
      - Collects non-breaking validation or runtime warnings
      - Fails fast on critical errors
      - Catches validation errors and converts them into warnings
      - Validates on assignment, converting assignment errors into warnings
      - Provides serialization to DataFrame
    """

    # Enable assignment validation
    model_config = ConfigDict(validate_assignment=True)

    # Internal list of warnings (not part of serialized schema)
    _warnings: dict[str, list[str]] = PrivateAttr(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        # Ensure private warnings list exists before any validation
        object.__setattr__(self, "_warnings", {})
        try:
            super().__init__(**data)
        except ValidationError as e:
            # Construct without validation to preserve fields
            inst = self.__class__.model_construct(**data)
            # Copy field data
            object.__setattr__(self, "__dict__", inst.__dict__.copy())
            # Ensure warnings list on this instance
            if not hasattr(self, "_warnings"):
                object.__setattr__(self, "_warnings", {})
            # Convert each validation error into a warning
            for err in e.errors():
                loc = ".".join(str(x) for x in err.get("loc", []))
                msg = err.get("msg", "")
                self.add_warning(loc, msg)

    def __setattr__(self, name: str, value: Any) -> None:
        """Abuses the fact that init does not return on valdiation errors"""
        # Intercept assignment-time validation errors
        if name in self.__class__.model_fields:
            try:
                self._clear_warnings_for_attr(name)
                current_data = self.model_dump()
                current_data[name] = value
                obj = self.__class__.model_validate(current_data)
                if name in obj.warnings:
                    self.add_warning(name, obj.warnings[name])
                    # Do not assign invalid value
                    return
            except ValidationError as e:
                for err in e.errors():
                    if err.get("loc") == (name,):
                        msg = err.get("msg", "")
                        self.add_warning(name, msg)
                # Do not assign invalid value
                return

        super().__setattr__(name, value)

    def add_warning(self, key: str, message: str) -> None:
        """Append a warning message to this model."""
        if key in self._warnings:
            self._warnings[key].append(message)
        else:
            self._warnings[key] = [message]

    @property
    def warnings(self) -> dict[str, list[str]]:
        """Return a copy of the warnings list."""
        return self._warnings

    def show_warnings(self) -> None:
        """Print all warnings to the console."""
        if not self._warnings:
            print("No warnings.")
            return
        print("Warnings:")
        for i, w in enumerate(self._warnings, start=1):
            print(f" {i}. {w}")

    def _clear_warnings_for_attr(self, key):
        """
        Remove a key from the warnings.
        """
        self._warnings.pop(key, None)

    def _merge_submodel_warnings(self, submodel: Any) -> None:
        """
        Bring warnings from a nested Base (or list thereof)
        into this model's warnings list.
        """
        from typing import Iterable

        def _collect(wm: Base):
            for w in wm.warnings:
                self.add_warning(wm.__class__.__name__, w)

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

    def _get_serializable_fields(self) -> list[str]:
        """
        Parse and return column names for serialization.
        Override this method in subclasses if you need custom field selection logic.
        """
        return [
            field_name
            for field_name in self.model_fields.keys()
            if not field_name.startswith("_")
        ]

    def _raise_exception_on_loc(self, err: str, type: str, loc: str, msg: str):
        """
        Nice and convoluted way to raise validation errors on custom locs.
        Used in model validators
        """
        raise ValidationError.from_exception_data(
            err,
            [
                InitErrorDetails(
                    type=PydanticCustomError(
                        type,
                        msg,
                    ),
                    loc=(loc,),
                    input=self,
                ),
            ],
        )

    def _to_dataframe(self, **kwargs) -> pd.DataFrame:
        """
        Private method to be implemented by each subclass for specific serialization logic.
        This method should contain the actual DataFrame creation logic.

        Returns:
            pd.DataFrame: The serialized DataFrame
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _to_dataframe() method"
        )

    def to_df(self, **kwargs) -> pd.DataFrame:
        """
        Public method that handles common serialization logic and delegates to _to_dataframe().

        Returns:
            pd.DataFrame: Serialized DataFrame with class name as index level
        """
        columns = self._get_serializable_fields()
        kwargs.setdefault("available_columns", columns)

        # Get DataFrame with unified error handling
        try:
            df = self._to_dataframe(**kwargs)
            if not isinstance(df, pd.DataFrame):
                raise ValueError(f"Expected DataFrame, got {type(df)}")
        except Exception as e:
            self.add_warning(
                f"{self.__class__.__name__}._to_dataframe()", f"failed: {e}"
            )
            df = pd.DataFrame()

        # Set index name if not already set
        if df.index.name is None:
            df.index.name = self.__class__.__name__.lower()

        return df
