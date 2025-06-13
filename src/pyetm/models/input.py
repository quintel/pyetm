from typing import Optional
from pydantic import BaseModel

class Input(BaseModel):
    key: str
    # These datatypes depend on Unit! - we might need an extra model for it,
    # that groups unit types with min/max options for validation. For for example
    # "enum" units there is no min or max, but these will have a
    # "permitted_values" field instead.
    # Idea: we need subtypes of Input - like EnumInput and BoolInput and FloatInput
    min: Optional[float] = None
    max: Optional[float] = None
    unit: str
    user: Optional[float | str | bool] = None
    disabled: Optional[bool] = False
    coupling_disabled: Optional[bool] = False
    coupling_groups: Optional[list[str]] = []
    disabled_by: Optional[str] = None
    share_group: Optional[str] = None
    step: Optional[float] = None

    @classmethod
    def from_json(cls, data: tuple[str, dict]):
        '''Initialise an Input from a JSON-like tuple object coming from .items()'''
        key, data = data
        data.update({'key': key})
        return cls(**data)


    # Nice to have:
    # Add validation for input GET - which fields are sometimes must and which are (always) optional?
    # Add validation for input SET - correct units, between min and max


