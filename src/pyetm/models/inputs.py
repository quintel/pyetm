from __future__ import annotations
from typing import Any, Dict, Optional, Union
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

    @classmethod
    def _from_dataframe_row(cls, key: str, row_data: Dict[str, Any]) -> Input:
        """
        Create an Input instance from a single row of DataFrame data.

        Args:
            key: The input key (from index)
            row_data: Dictionary of column values for this row

        Returns:
            Input instance of appropriate subclass
        """
        # Add the key to the row data
        data = {"key": key, **row_data}

        # Determine the appropriate class based on unit
        unit = data.get("unit", "")
        try:
            klass = cls.class_type(unit)
            return klass.load_safe(**data)
        except Exception as e:
            # Fallback to base Input class
            instance = cls.load_safe(**data)
            instance.add_warning(
                f"Failed to create specialized input for unit '{unit}': {e}"
            )
            return instance


class BoolInput(Input):
    """Input representing a boolean"""

    user: Optional[bool] = None
    default: Optional[bool] = None


class EnumInput(Input):
    """Input representing an enumeration"""

    user: Optional[str] = None
    permitted_values: list[str] = []
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
    min: Optional[float] = None
    max: Optional[float] = None
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
    inputs: list[Input] = []

    def __len__(self):
        return len(self.inputs)

    def __iter__(self):
        yield from iter(self.inputs)

    def keys(self):
        return [input.key for input in self.inputs]

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
    def _from_dataframe(cls, df: pd.DataFrame, **kwargs) -> Inputs:
        """
        Deserialize DataFrame back to Inputs collection.
        Expected DataFrame structure: MultiIndex(['input', 'unit']) with value columns.
        """
        inputs_list = []

        # Verify expected structure
        if not isinstance(df.index, pd.MultiIndex) or df.index.names != [
            "input",
            "unit",
        ]:
            raise ValueError(
                f"Expected MultiIndex with names ['input', 'unit'], got {df.index.names}"
            )

        # Iterate through each row in the DataFrame
        for (input_key, unit), row in df.iterrows():
            try:
                # Convert row to dictionary and add the unit and key
                row_data = row.to_dict()
                row_data["unit"] = unit

                # Create the appropriate Input object
                input_obj = Input._from_dataframe_row(input_key, row_data)
                inputs_list.append(input_obj)

            except Exception as e:
                # Create a minimal input with warning
                input_obj = Input.load_safe(key=input_key, unit=unit, default=None)
                input_obj.add_warning(f"Failed to deserialize input '{input_key}': {e}")
                inputs_list.append(input_obj)

        # Create the Inputs collection
        collection = cls.load_safe(inputs=inputs_list)

        # Merge warnings from individual inputs
        for input_obj in inputs_list:
            if hasattr(input_obj, "warnings") and input_obj.warnings:
                for warning in input_obj.warnings:
                    collection.add_warning(f"Input '{input_obj.key}': {warning}")

        return collection

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
