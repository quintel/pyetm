"""Input parameter models and validation."""

from __future__ import annotations
from typing import Any, Dict, Optional, Union
from pydantic import field_validator, model_validator
import pandas as pd
from pyetm.models.warnings import WarningCollector
from pyetm.models.base import Base


class InputError(Exception):
    """Base input error"""


class Input(Base):
    """
    Represents a slider in the front end. Inputs have default values, units and keys and may
    have a user value or be disabled by a coupling setting.
    """

    key: str
    unit: str
    default: Optional[Union[float, str]] = None
    user: Optional[Union[float, str]] = None
    disabled: Optional[bool] = False
    disabled_by: Optional[str] = None

    def is_valid_update(self, value: Any) -> WarningCollector:
        """
        Returns a WarningCollector with validation warnings without updating the current object.
        """
        new_obj_dict = self.model_dump()
        new_obj_dict["user"] = value

        try:
            warnings_obj = self.__class__(**new_obj_dict)
            if isinstance(warnings_obj.warnings, WarningCollector):
                return warnings_obj.warnings
            return WarningCollector()
        except Exception:
            return WarningCollector()

    @classmethod
    def from_json(cls, data: tuple[str, Dict[str, Any]]) -> "Input":
        """
        Initialize an Input from a JSON-like tuple coming from .items()
        """
        key, payload = data
        payload["key"] = key

        try:
            klass = cls.class_type(payload["unit"])
            input_instance = klass.model_validate(payload)
            return input_instance
        except Exception as e:
            # Create a basic Input with warning attached
            basic_input = cls.model_construct(**payload)  # Bypass validation
            basic_input.add_warning(key, f"Failed to create specialized input: {e}")
            return basic_input

    @staticmethod
    def class_type(unit: str) -> type[Input]:
        """Return the appropriate Input subclass for the given unit"""
        if unit == "bool":
            return BoolInput
        elif unit == "enum":
            return EnumInput
        else:
            return FloatInput

    @field_validator("user", mode="before")
    @classmethod
    def check_reset(cls, value: Any) -> Any:
        """If a reset value is sent, treat it as setting the user value to None"""
        if isinstance(value, str) and value == "reset":
            return None
        else:
            return value

    @property
    def merged_value(self) -> Optional[Union[float, str]]:
        """
        Returns the merged value: user value if set, otherwise default value.
        Raises a warning if both user and default are None/NaN.
        """
        if not pd.isna(self.user):
            return self.user
        if not pd.isna(self.default):
            return self.default

        # Both are None/NaN - add warning
        self.add_warning(
            self.key,
            f"Both user and default values are None/NaN for input '{self.key}'",
        )
        return None


class BoolInput(Input):
    """
    Input representing a boolean.
    Uses floats to represent bools (1.0 true, 0.0 false)
    """

    user: Optional[float] = None
    default: Optional[float] = None

    @field_validator("user", mode="after")
    @classmethod
    def is_bool_float(cls, value: Optional[float]) -> Optional[float]:
        if pd.isna(value) or value in [1.0, 0.0]:
            return value
        raise ValueError(
            f"{value} should be 1.0 or 0.0 representing True/False, or On/Off"
        )

    @field_validator("user", mode="before")
    @classmethod
    def coerce_bool(cls, value: Any) -> Any:
        if value is None:
            return None

        truth_map: Dict[str, float] = {
            "true": 1.0,
            "t": 1.0,
            "1": 1.0,
            "yes": 1.0,
            "y": 1.0,
            "on": 1.0,
            "false": 0.0,
            "f": 0.0,
            "0": 0.0,
            "no": 0.0,
            "n": 0.0,
            "off": 0.0,
        }

        if isinstance(value, str):
            return truth_map.get(value.strip().lower(), value)

        if isinstance(value, bool):
            return 1.0 if value else 0.0

        if isinstance(value, (int, float)):
            return 1.0 if value != 0 else 0.0

        return value


class EnumInput(Input):
    """Input representing an enumeration"""

    permitted_values: list[str]
    default: Optional[str] = None
    user: Optional[str] = None

    def _get_serializable_fields(self) -> list[str]:
        """Include permitted_values in serialization for EnumInput"""
        base_fields = super()._get_serializable_fields()
        if "permitted_values" not in base_fields:
            base_fields.append("permitted_values")
        return base_fields

    @model_validator(mode="after")
    def check_permitted(self) -> "EnumInput":
        if pd.isna(self.user) or self.user in self.permitted_values:
            return self
        self._raise_exception_on_loc(
            "ValueError",
            type="inclusion",
            loc="user",
            msg=f"Value error, {self.user} should be in {self.permitted_values}",
        )
        return self

    @field_validator("user", mode="before")
    @classmethod
    def coerce_enum(cls, value: Any) -> Any:
        if pd.isna(value):
            return None
        return pd.Series([value]).astype(str).str.strip().iloc[0]


class FloatInput(Input):
    """Input representing a float"""

    user: Optional[float] = None
    min: float
    max: float
    default: Optional[float] = None
    share_group: Optional[str] = None
    step: Optional[float] = None

    def _get_serializable_fields(self) -> list[str]:
        """Include min/max in serialization for FloatInput"""
        base_fields = super()._get_serializable_fields()
        for field in ["min", "max", "step", "share_group"]:
            if field not in base_fields:
                base_fields.append(field)
        return base_fields

    @model_validator(mode="after")
    def check_min_max(self) -> "FloatInput":
        if not isinstance(self.user, float):
            return self
        if self.user is None or (self.user <= self.max and self.user >= self.min):
            return self
        self._raise_exception_on_loc(
            "ValueError",
            type="out_of_bounds",
            loc="user",
            msg=f"Value error, {self.user} should be between {self.min} and {self.max}",
        )
        return self

    @field_validator("user", mode="before")
    @classmethod
    def coerce_float(cls, value: Any) -> Any:
        if pd.isna(value):
            return None
        try:
            if isinstance(value, str):
                return pd.to_numeric(
                    pd.Series([value]).str.strip(), errors="raise"
                ).iloc[0]
            elif isinstance(value, (int, float)):
                return pd.Series([value], dtype=float).iloc[0]
        except (ValueError, pd.errors.ParserError):
            pass

        return value


class Inputs(Base):
    """
    A collection of Inputs.
    """

    inputs: list[Input]

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    def __len__(self) -> int:
        return len(self.inputs)

    def __iter__(self) -> Any:
        yield from iter(self.inputs)

    def __getitem__(self, key: str) -> Input:
        result = self.get_input_by_key(key)
        if result is None:
            raise KeyError(f"Input '{key}' not found")
        return result

    def keys(self) -> list[str]:
        return [input.key for input in self.inputs]

    def get_input_by_key(self, key: str) -> Optional[Input]:
        """Get input by its key"""
        for input_obj in self.inputs:
            if input_obj.key == key:
                return input_obj
        return None

    def is_valid_update(self, key_vals: Dict[str, Any]) -> Dict[str, WarningCollector]:
        """
        Returns a dict mapping input keys to their WarningCollectors when errors were found.
        """
        warnings: Dict[str, WarningCollector] = {}
        input_map = {inp.key: inp for inp in self.inputs}

        for key, value in key_vals.items():
            input_obj = input_map.get(key)
            if input_obj is None:
                warnings[key] = WarningCollector.with_warning(key, "Key does not exist")
                continue

            input_warnings = input_obj.is_valid_update(value)
            if len(input_warnings) > 0:
                warnings[key] = input_warnings

        return warnings

    def update(self, key_vals: Dict[str, Any]) -> None:
        """
        Update the values of certain inputs.
        """
        for input_obj in self.inputs:
            if input_obj.key in key_vals:
                input_obj.user = key_vals[input_obj.key]

    def _to_dataframe(
        self, fields: Union[str, list[str]] = "value", **kwargs: Any
    ) -> pd.DataFrame:
        """
        Serialize the Inputs collection to DataFrame.

        Args:
            fields: Field(s) to include. Default is "value" (user value if set, else default).
                   Can be a string or list of strings. Options include:
                   - "value": user value if set, otherwise default (RECOMMENDED)
                   - "user": only user-set values
                   - "default": only default values
                   - "min", "max", "permitted_values", etc.: other input attributes
        """
        if not isinstance(fields, list):
            fields = [fields]
        columns = ["unit"] + fields
        try:
            df = pd.DataFrame.from_dict(
                {
                    input.key: [
                        (
                            input.merged_value
                            if key == "value"
                            else getattr(input, key, None)
                        )
                        for key in columns
                    ]
                    for input in self.inputs
                },
                orient="index",
                columns=columns,
            )
            df.index.name = "input"
            return df.set_index("unit", append=True)
        except Exception as e:
            self.add_warning("_to_dataframe", f"Failed to create DataFrame: {e}")
            return pd.DataFrame()

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Inputs":
        inputs = [Input.from_json(item) for item in data.items()]

        collection = cls.model_validate({"inputs": inputs})
        collection._merge_submodel_warnings(*inputs, key_attr="key")

        return collection
