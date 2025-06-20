from typing import Any, Tuple, Union
from pydantic import BaseModel


class BalancedInput(BaseModel):
    key: str
    value: Union[float, str, bool]

    @classmethod
    def from_json(cls, data: Tuple[str, Any]) -> "BalancedInput":
        key, val = data
        return cls(key=key, value=val)
