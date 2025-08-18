from __future__ import annotations
from typing import Optional, Union, Set
from pydantic import field_validator, model_validator, PrivateAttr
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

    def has_coupling_groups(self) -> bool:
        """Check if this input has any coupling groups"""
        return any(
            isinstance(g, str) and g.startswith("external")
            for g in (self.coupling_groups or [])
        )

    def get_coupling_groups(self) -> list[str]:
        """Get the coupling groups for this input which must start with 'external'"""
        groups = self.coupling_groups or []
        return [g for g in groups if isinstance(g, str) and g.startswith("external")]

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
            return self
        if self.user is None or (self.user <= self.max and self.user >= self.min):
            return self
        self._raise_exception_on_loc(
            "ValueError",
            type="out_of_bounds",
            loc="user",
            msg=f"Value error, {self.user} should be between {self.min} and {self.max}",
        )


class CouplingState:
    """Helper class to track coupling state"""

    def __init__(self):
        self.active_groups: Set[str] = set()
        self.inactive_groups: Set[str] = set()

    def activate_group(self, group: str):
        """Activate a coupling group"""
        self.active_groups.add(group)
        self.inactive_groups.discard(group)

    def deactivate_group(self, group: str):
        """Deactivate a coupling group"""
        self.inactive_groups.add(group)
        self.active_groups.discard(group)

    def is_active(self, group: str) -> bool:
        """Check if a coupling group is active"""
        return group in self.active_groups

    def is_inactive(self, group: str) -> bool:
        """Check if a coupling group is inactive"""
        return group in self.inactive_groups


class Inputs(Base):
    inputs: list[Input]
    _coupling_state: CouplingState = PrivateAttr(default_factory=CouplingState)

    def __init__(self, **data):
        super().__init__(**data)

    def __len__(self):
        return len(self.inputs)

    def __iter__(self):
        yield from iter(self.inputs)

    def keys(self):
        return [input.key for input in self.inputs]

    def get_all_coupling_groups(self) -> Set[str]:
        """Get all possible coupling groups from all inputs"""
        all_groups = set()
        for input_obj in self.inputs:
            all_groups.update(input_obj.get_coupling_groups())
        return all_groups

    def get_coupling_groups_for_input(self, key: str) -> list[str]:
        """Get coupling groups for a specific input key"""
        input_obj = self.get_input_by_key(key)
        if input_obj:
            return input_obj.get_coupling_groups()
        return []

    def get_input_by_key(self, key: str) -> Optional[Input]:
        """Get input by its key"""
        for input_obj in self.inputs:
            if input_obj.key == key:
                return input_obj
        return None

    def get_coupling_inputs(self) -> list[Input]:
        """List all coupling inputs whose keys start with 'external_coupling'"""
        return [
            inp
            for inp in self.inputs
            if isinstance(inp.key, str) and inp.key.startswith("external_coupling")
        ]

    def activate_coupling_group(self, group: str):
        """Activate a coupling group"""
        if self._coupling_state:
            self._coupling_state.activate_group(group)

    def deactivate_coupling_group(self, group: str):
        """Deactivate a coupling group"""
        if self._coupling_state:
            self._coupling_state.deactivate_group(group)

    def get_active_coupling_groups(self) -> Set[str]:
        """Get all active coupling groups"""
        return self._coupling_state.active_groups if self._coupling_state else set()

    def get_inactive_coupling_groups(self) -> Set[str]:
        """Get all inactive coupling groups"""
        return self._coupling_state.inactive_groups if self._coupling_state else set()

    def activate_coupling_groups_for_updates(self, key_vals: dict):
        """
        Activate coupling groups based on input updates.
        """
        if not self._coupling_state:
            return

        for key, value in key_vals.items():
            if isinstance(value, str) and value == "reset":
                continue

            groups = self.get_coupling_groups_for_input(key)
            for group in groups:
                if not self._coupling_state.is_inactive(group):
                    self.activate_coupling_group(group)

    def is_valid_update(self, key_vals: dict) -> dict[str, WarningCollector]:
        """
        Returns a dict mapping input keys to their WarningCollectors when errors were found.
        """
        warnings: dict[str, WarningCollector] = {}
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

    def update(self, key_vals: dict):
        """
        Update the values of certain inputs and activate coupling groups.
        """
        self.activate_coupling_groups_for_updates(key_vals)
        for input_obj in self.inputs:
            if input_obj.key in key_vals:
                input_obj.user = key_vals[input_obj.key]

    def force_uncouple_all(self):
        """Force uncouple all coupling groups"""
        if self._coupling_state:
            all_groups = self.get_all_coupling_groups()
            for group in all_groups:
                self.deactivate_coupling_group(group)

    def _to_dataframe(self, columns="user", **kwargs) -> pd.DataFrame:
        """
        Serialize the Inputs collection to DataFrame.
        """
        if not isinstance(columns, list):
            columns = [columns]
        columns = ["unit"] + columns

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
