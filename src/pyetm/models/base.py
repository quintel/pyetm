from __future__ import annotations
from typing import Any, Type, TypeVar, Union, List, Dict
from pydantic import BaseModel, PrivateAttr, ValidationError, ConfigDict
from pydantic_core import InitErrorDetails, PydanticCustomError
import pandas as pd
from pyetm.models.warnings import WarningCollector

T = TypeVar("T", bound="Base")


class Base(BaseModel):
    """
    Custom base model that:
      - Collects non-breaking validation or runtime warnings using WarningCollector
      - Fails fast on critical errors
      - Catches validation errors and converts them into warnings
      - Validates on assignment, converting assignment errors into warnings
      - Provides serialization to DataFrame
    """

    # Enable assignment validation
    model_config = ConfigDict(validate_assignment=True)
    _warning_collector: WarningCollector = PrivateAttr(default_factory=WarningCollector)

    def __init__(self, **data: Any) -> None:
        """
        Initialize the model, converting validation errors to warnings.
        """
        object.__setattr__(self, "_warning_collector", WarningCollector())

        try:
            super().__init__(**data)
        except ValidationError as e:
            # If validation fails, create model without validation and collect warnings
            # Use model_construct to bypass validation
            temp_instance = self.__class__.model_construct(**data)

            # Copy the constructed data to this instance
            for field_name, field_value in temp_instance.__dict__.items():
                if not field_name.startswith("_"):
                    object.__setattr__(self, field_name, field_value)

            # Convert validation errors to warnings
            for error in e.errors():
                field_path = ".".join(str(part) for part in error.get("loc", []))
                message = error.get("msg", "Validation failed")
                self._warning_collector.add(field_path, message, "error")

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Handle assignment with validation error capture.
        """
        # Skip validation for private attributes, methods/functions, or existing methods
        if (
            name.startswith("_")
            or name not in self.__class__.model_fields
            or callable(value)
            or hasattr(self.__class__, name)
        ):
            # Use object.__setattr__ to bypass Pydantic for these cases
            object.__setattr__(self, name, value)
            return

        # Clear existing warnings for this field
        self._warning_collector.clear(name)

        try:
            # Try to validate the new value by creating a copy with the update
            current_data = self.model_dump()
            current_data[name] = value

            # Test validation with a temporary instance
            test_instance = self.__class__.model_validate(current_data)

            # If validation succeeds, set the value
            super().__setattr__(name, value)

        except ValidationError as e:
            # If validation fails, add warnings but don't set the value
            for error in e.errors():
                if error.get("loc") == (name,):
                    message = error.get("msg", "Validation failed")
                    self._warning_collector.add(name, message, "warning")
            return

    def add_warning(
        self,
        field: str,
        message: Union[str, List[str], Dict[str, Any]],
        severity: str = "warning",
    ) -> None:
        """Add a warning to this model instance."""
        self._warning_collector.add(field, message, severity)

    @property
    def warnings(self) -> Union[WarningCollector, Dict[str, List[str]]]:
        """
        Return warnings.
        """
        return self._warning_collector

    def show_warnings(self) -> None:
        """Print all warnings to the console."""
        self._warning_collector.show_warnings()

    def log_warnings(
        self, logger, level: str = "warning", prefix: str | None = None
    ) -> None:
        """
        Log all collected warnings using the provided logger.
        """
        try:
            collector = getattr(self, "warnings", None)
            if collector is None or len(collector) == 0:
                return
            log_fn = getattr(logger, level, getattr(logger, "warning", None))
            if log_fn is None:
                return
            for w in collector:
                field = getattr(w, "field", "")
                msg = getattr(w, "message", str(w))
                if prefix:
                    log_fn(f"{prefix} [{field}]: {msg}")
                else:
                    log_fn(f"[{field}]: {msg}")
        except Exception:
            pass

    def _clear_warnings_for_attr(self, field: str) -> None:
        """Remove warnings for a specific field."""
        self._warning_collector.clear(field)

    def _merge_submodel_warnings(self, *submodels: Base, key_attr: str = None) -> None:
        """
        Merge warnings from nested Base models.
        """
        self._warning_collector.merge_submodel_warnings(*submodels, key_attr=key_attr)

    @classmethod
    def from_dataframe(cls: Type[T], df: pd.DataFrame, **kwargs) -> T:
        """
        Create an instance from a pandas DataFrame.
        """
        try:
            return cls._from_dataframe(df, **kwargs)
        except Exception as e:
            # Create a fallback instance with warnings
            instance = cls.model_construct()
            instance.add_warning(
                "from_dataframe", f"Failed to create from DataFrame: {e}"
            )
            return instance

    @classmethod
    def _from_dataframe(cls, df: pd.DataFrame, **kwargs):
        """
        Private method to be implemented by each subclass for specific deserialization logic.
        """
        raise NotImplementedError(
            f"{cls.__name__} must implement _from_dataframe() class method"
        )

    def _get_serializable_fields(self) -> List[str]:
        """
        Parse and return column names for serialization.
        Override this method in subclasses if you need custom field selection logic.
        """
        return [
            field_name
            for field_name in self.__class__.model_fields.keys()
            if not field_name.startswith("_")
        ]

    def _raise_exception_on_loc(self, err: str, type: str, loc: str, msg: str):
        """
        Raise validation errors on custom locations.
        Used in model validators.
        """
        raise ValidationError.from_exception_data(
            err,
            [
                InitErrorDetails(
                    type=PydanticCustomError(type, msg),
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

    def to_dataframe(self, **kwargs) -> pd.DataFrame:
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
        except Exception as e:
            self.add_warning(
                f"{self.__class__.__name__}._to_dataframe()", f"failed: {e}"
            )
            df = pd.DataFrame()

        return df
