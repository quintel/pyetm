"""Base models and shared functionality."""

from __future__ import annotations
from typing import Any, Type, TypeVar, Union, List, Dict, Optional, Callable, cast, Literal
from pydantic import BaseModel, PrivateAttr, ValidationError, ConfigDict
from pydantic_core import InitErrorDetails, PydanticCustomError
import pandas as pd
from pyetm.models.warnings import WarningCollector
from pyetm.models.error_policy import get_error_policy

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
    _bulk_operation_context: bool = PrivateAttr(default=False)

    def __init__(self, **data: Any) -> None:
        """
        Initialize the model, converting validation errors to warnings.
        """
        super(BaseModel, self).__setattr__("__pydantic_private__", {})

        # Initialize all private attributes with their defaults
        private_dict: Dict[str, Any] = self.__pydantic_private__  # type: ignore[assignment]
        for attr_name, attr_info in self.__class__.__private_attributes__.items():
            if (
                hasattr(attr_info, "default_factory")
                and attr_info.default_factory is not None
            ):
                # Call factory - signature varies between pydantic versions
                private_dict[attr_name] = attr_info.default_factory()
            elif hasattr(attr_info, "default"):
                private_dict[attr_name] = attr_info.default
            else:
                private_dict[attr_name] = None

        try:
            super().__init__(**data)
        except ValidationError as e:
            # Check if data is None or empty - this indicates API error
            if not data or data is None:
                # Re-raise with clearer message
                raise ValueError(
                    f"Cannot create {self.__class__.__name__} with empty data. "
                    "This usually indicates an authentication or API error."
                ) from e

            # If validation fails, create model without validation and collect warnings
            # Use model_construct to bypass validation
            temp_instance = self.__class__.model_construct(**data)

            # Copy the constructed data to this instance
            for field_name, field_value in temp_instance.__dict__.items():
                if not field_name.startswith("_"):
                    object.__setattr__(self, field_name, field_value)

            # Ensure required Pydantic slot attributes exist to prevent AttributeError
            for slot in ("__pydantic_fields_set__", "__pydantic_extra__"):
                try:
                    value = object.__getattribute__(temp_instance, slot)
                    object.__setattr__(self, slot, value)
                except AttributeError:
                    # Initialize missing slot attributes with defaults
                    if slot == "__pydantic_extra__":
                        object.__setattr__(self, slot, {})
                    elif slot == "__pydantic_fields_set__":
                        object.__setattr__(self, slot, set())

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
        severity: Literal["info", "warning", "error"] = "warning",
    ) -> None:
        """Add a warning to this model instance.

        Args:
            field: Field name where warning occurred
            message: Warning message (str, list of strings, or dict)
            severity: Warning severity level (info/warning/error)

        Raises:
            RuntimeError: If error policy determines warning should raise an exception
        """
        # Add warning to collector
        self._warning_collector.add(field, message, severity)

        # Check if we should raise based on error policy
        policy = get_error_policy()
        if policy.should_raise(severity, self._bulk_operation_context):
            # Format message for exception
            if isinstance(message, str):
                error_msg = message
            elif isinstance(message, list):
                error_msg = "; ".join(str(m) for m in message)
            elif isinstance(message, dict):
                error_msg = "; ".join(f"{k}: {v}" for k, v in message.items())
            else:
                error_msg = str(message)

            raise RuntimeError(
                f"{self.__class__.__name__} validation failed on field '{field}': {error_msg}"
            )

    @property
    def warnings(self) -> WarningCollector:
        """
        Return warnings.
        """
        return self._warning_collector

    def set_bulk_context(self, is_bulk: bool = True) -> None:
        """Set whether this model is being processed in a bulk operation context.

        Args:
            is_bulk: True if in bulk operation, False for single operation

        Note:
            This affects error handling behavior. In bulk operations, errors are
            collected as warnings to allow partial success. In single operations,
            errors may raise exceptions based on the error_mode setting.
        """
        self._bulk_operation_context = is_bulk

    def show_warnings(self) -> None:
        """Print all warnings to the console."""
        self._warning_collector.show_warnings()

    def auto_show_warnings(self, context: str = "") -> None:
        """
        Automatically display warnings if any exist, with contextual information.

        Args:
            context: Additional context to display (e.g., "SavedScenario #123")
        """
        if len(self.warnings) > 0:
            if context:
                print(f"\n=== Warnings for {context} ===")
            self.show_warnings()

    def log_warnings(
        self, logger: Any, level: str = "warning", prefix: str | None = None
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

    def _merge_submodel_warnings(
        self, *submodels: Base, key_attr: Optional[str] = None
    ) -> None:
        """
        Merge warnings from nested Base models.
        """
        self._warning_collector.merge_submodel_warnings(*submodels, key_attr=key_attr)

    @classmethod
    def from_dataframe(cls: Type[T], df: pd.DataFrame, **kwargs: Any) -> T:
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
    def _from_dataframe(cls: Type[T], df: pd.DataFrame, **kwargs: Any) -> T:
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

    def _raise_exception_on_loc(self, err: str, type: str, loc: str, msg: str) -> None:
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

    def _to_dataframe(self, **kwargs: Any) -> pd.DataFrame:
        """
        Private method to be implemented by each subclass for specific serialization logic.
        This method should contain the actual DataFrame creation logic.

        Returns:
            pd.DataFrame: The serialized DataFrame
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _to_dataframe() method"
        )

    def to_dataframe(self, **kwargs: Any) -> pd.DataFrame:
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
