from typing import Any, Dict, Optional, Union
from pydantic import BaseModel


class Input(BaseModel):
    key: str
    unit: str
    default: Optional[Union[float, str, bool]] = None
    user: Optional[Union[float, str, bool]] = None
    disabled: Optional[bool] = False
    coupling_disabled: Optional[bool] = False
    coupling_groups: Optional[list[str]] = []
    disabled_by: Optional[str] = None

    @classmethod
    def from_json(cls, data: tuple[str, dict]):
        """
        Initialise an Input from a JSON-like tuple coming from .items()
        """
        key, payload = data
        payload.update({"key": key})
        klass = cls.class_type(payload["unit"])
        return klass(**payload)

    @staticmethod
    def class_type(unit: str):
        if unit == "bool":
            return BoolInput
        elif unit == "enum":
            return EnumInput
        else:
            return FloatInput

    # Nice to have:
    # Add validation for input SET - correct units, between min and max


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
