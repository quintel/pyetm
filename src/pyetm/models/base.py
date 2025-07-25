from __future__ import annotations
from typing import Any, Type, TypeVar
from pydantic import BaseModel, PrivateAttr, ValidationError, ConfigDict
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
            self.add_warning(f"{self.__class__.__name__}._to_dataframe() failed: {e}")
            df = pd.DataFrame()

        # Set index name if not already set
        if df.index.name is None:
            df.index.name = self.__class__.__name__.lower()

        return df

    @classmethod
    def _from_dataframe(cls: Type[T], df: pd.DataFrame, **kwargs) -> T:
        """
        Private method to be implemented by each subclass for specific deserialization logic.
        This method should contain the actual model creation logic from DataFrame.

        Returns:
            Instance of the model class
        """
        raise NotImplementedError(
            f"{cls.__name__} must implement _from_dataframe() class method"
        )

    @classmethod
    def from_df(cls: Type[T], df: pd.DataFrame, **kwargs) -> T:
        """
        Public class method that handles common deserialization logic and delegates to _from_dataframe().

        Returns:
            Instance of the model class (with warnings)
        """
        try:
            instance = cls._from_dataframe(df, **kwargs)
            if not isinstance(instance, cls):
                raise ValueError(
                    f"Expected {cls.__name__} instance, got {type(instance)}"
                )
            return instance
        except Exception as e:
            try:
                instance = cls.model_construct()
            except Exception:
                instance = object.__new__(cls)
                object.__setattr__(instance, "_warnings", [])

            if not hasattr(instance, "_warnings"):
                object.__setattr__(instance, "_warnings", [])

            instance.add_warning(f"{cls.__name__}._from_dataframe() failed: {e}")
            return instance
