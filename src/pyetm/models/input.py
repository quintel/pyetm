from typing import Optional
from pydantic import BaseModel

class Input(BaseModel):
    key: str
    unit: str
    user: Optional[float | str | bool] = None
    disabled: Optional[bool] = False
    coupling_disabled: Optional[bool] = False
    coupling_groups: Optional[list[str]] = []
    disabled_by: Optional[str] = None

    @classmethod
    def from_json(cls, data: tuple[str, dict]):
        '''Initialise an Input from a JSON-like tuple object coming from .items()'''
        key, data = data
        data.update({'key': key})
        klass = cls.class_type(data["unit"])
        return klass(**data)

    @staticmethod
    def class_type(unit):
        if unit == "bool":
            return BoolInput
        elif unit == "enum":
            return EnumInput
        else:
            return FloatInput

    # Nice to have:
    # Add validation for input SET - correct units, between min and max


class BoolInput(Input):
    ''' Input representing a boolean '''
    user: Optional[bool] = None


class EnumInput(Input):
    ''' Input representing an enumarable '''
    user: Optional[str] = None
    permitted_values: list[str]


class FloatInput(Input):
    ''' Input representing a float '''
    user: Optional[float] = None
    min: float
    max: float
    share_group: Optional[str] = None
    step: Optional[float] = None
