from __future__ import annotations
from typing import Optional, Union
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

    # Nice to have:
    # TODO: Add validation for input SET - correct units, between min and max


class BoolInput(Input):
    """Input representing a boolean"""

    user: Optional[bool] = None
    default: Optional[bool] = None


class EnumInput(Input):
    """Input representing an enumeration"""

    user: Optional[str] = None
    permitted_values: list[str]
    default: Optional[str] = None


class FloatInput(Input):
    """Input representing a float"""

    user: Optional[float] = None
    min: float
    max: float
    default: Optional[float] = None
    share_group: Optional[str] = None
    step: Optional[float] = None
