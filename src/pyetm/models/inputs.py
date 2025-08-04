from __future__ import annotations
from typing import Optional, Union
from pydantic import field_validator, model_validator
import pandas as pd
from pyetm.models.warnings import WarningCollector
from pyetm.models.base import Base


class InputError(Exception):
    """Base input error"""


class Input(Base):
    key: str
    unit: str
    default: Optional[Union[float, str]] = None
    user: Optional[Union[float, str]] = None
    disabled: Optional[bool] = False
    coupling_disabled: Optional[bool] = False
    coupling_groups: Optional[list[str]] = []
    disabled_by: Optional[str] = None

    def is_valid_update(self, value) -> WarningCollector:
        """
        Returns a WarningCollector with validation warnings without updating the current object.
        """
        new_obj_dict = self.model_dump()
        new_obj_dict["user"] = value

        warnings_obj = self.__class__(**new_obj_dict)
        return warnings_obj.warnings

    @classmethod
    def from_json(cls, data: tuple[str, dict]) -> "Input":
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
    def check_reset(cls, value):
        """If a reset value is sent, treat it as setting the user value to None"""
        if isinstance(value, str) and value == "reset":
            return None
        else:
            return value


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
        if value == 1.0 or value == 0.0 or value is None:
            return value
        raise ValueError(
            f"{value} should be 1.0 or 0.0 representing True/False, or On/Off"
        )

    # NOTE: I need a lot of validation, and I need my own update method that
    # will cast true/false into 1.0 and 0.0


class EnumInput(Input):
    """Input representing an enumeration"""

    permitted_values: list[str]
    default: Optional[str] = None
    user: Optional[str] = None

    def _get_serializable_fields(self) -> list[str]:
        """Include permitted_values in serialization for EnumInput"""
        base_fields = super()._get_serializable_fields()
        # Ensure permitted_values is included
        if "permitted_values" not in base_fields:
            base_fields.append("permitted_values")
        return base_fields

    @model_validator(mode="after")
    def check_permitted(self) -> EnumInput:
        if self.user is None or self.user in self.permitted_values:
            return self
        self._raise_exception_on_loc(
            "ValueError",
            type="inclusion",
            loc="user",
            msg=f"Value error, {self.user} should be in {self.permitted_values}",
        )


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
    def check_min_max(self) -> FloatInput:
        if not isinstance(self.user, float):
            # We let pydantic handle the field validation
            return self
        if self.user is None or (self.user <= self.max and self.user >= self.min):
            return self
        self._raise_exception_on_loc(
            "ValueError",
            type="out_of_bounds",
            loc="user",
            msg=f"Value error, {self.user} should be between {self.min} and {self.max}",
        )


class Inputs(Base):
    inputs: list[Input]

    def __len__(self):
        return len(self.inputs)

    def __iter__(self):
        yield from iter(self.inputs)

    def keys(self):
        return [input.key for input in self.inputs]

    # TODO: Check the efficiency of doing this in a loop
    def is_valid_update(self, key_vals: dict) -> dict[str, WarningCollector]:
        """
        Returns a dict mapping input keys to their WarningCollectors when errors were found.
        """
        warnings = {}

        # Check each input that has an update
        for input_obj in self.inputs:
            if input_obj.key in key_vals:
                input_warnings = input_obj.is_valid_update(key_vals[input_obj.key])
                if len(input_warnings) > 0:
                    warnings[input_obj.key] = input_warnings

        # Check for non-existent keys
        non_existent_keys = set(key_vals.keys()) - set(self.keys())
        for key in non_existent_keys:
            warnings[key] = WarningCollector.with_warning(key, "Key does not exist")

        return warnings

    def update(self, key_vals: dict):
        """
        Update the values of certain inputs.
        Uses the new warning system for validation.
        """
        for input_obj in self.inputs:
            if input_obj.key in key_vals:
                # Use assignment which goes through __setattr__ validation
                input_obj.user = key_vals[input_obj.key]

    def _to_dataframe(self, columns="user", **kwargs) -> pd.DataFrame:
        """
        Serialize the Inputs collection to DataFrame.
        """
        if not isinstance(columns, list):
            columns = [columns]
        columns = ["unit"] + columns

        # Create DataFrame from inputs
        df = pd.DataFrame.from_dict(
            {
                input.key: [getattr(input, key, None) for key in columns]
                for input in self.inputs
            },
            orient="index",
            columns=columns,
        )
        df.index.name = "input"
        return df.set_index("unit", append=True)

    @classmethod
    def from_json(cls, data) -> Inputs:
        inputs = [Input.from_json(item) for item in data.items()]

        collection = cls.model_validate({"inputs": inputs})
        collection._merge_submodel_warnings(*inputs, key_attr="key")

        return collection
