from __future__ import annotations
from typing import Optional, Union
import pandas as pd
from pyetm.models.base import Base


class InputError(Exception):
    """Base input error"""


class Input(Base):
    key: str
    unit: str
    default: Optional[Union[float, str, bool]] = None
    user: Optional[Union[float, str, bool]] = None
    disabled: Optional[bool] = False
    coupling_disabled: Optional[bool] = False
    coupling_groups: Optional[list[str]] = []
    disabled_by: Optional[str] = None

    @classmethod
    def from_json(cls, data: tuple[str, dict]) -> Input:
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
            basic_input = cls.model_validate(payload)
            basic_input.add_warning(f"Failed to create specialized input: {e}")
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


class BoolInput(Input):
    """Input representing a boolean"""

    user: Optional[bool] = None
    default: Optional[bool] = None


class EnumInput(Input):
    """Input representing an enumeration"""

    user: Optional[str] = None
    permitted_values: list[str]
    default: Optional[str] = None

    def _get_serializable_fields(self) -> list[str]:
        """Include permitted_values in serialization for EnumInput"""
        base_fields = super()._get_serializable_fields()
        # Ensure permitted_values is included
        if "permitted_values" not in base_fields:
            base_fields.append("permitted_values")
        return base_fields


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


class Inputs(Base):
    inputs: list[Input]

    def __len__(self):
        return len(self.inputs)

    def __iter__(self):
        yield from iter(self.inputs)

    def keys(self):
        return [input.key for input in self.inputs]

    def _to_dataframe(self, values="user", **kwargs) -> pd.DataFrame:
        """
        Serialize the Inputs collection to DataFrame.
        """
        if not isinstance(values, list):
            values = [values]
        columns = ["unit"] + values

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

        # Merge any warnings from individual inputs
        for input_obj in inputs:
            if hasattr(input_obj, "warnings") and input_obj.warnings:
                for warning in input_obj.warnings:
                    collection.add_warning(warning)

        return collection
