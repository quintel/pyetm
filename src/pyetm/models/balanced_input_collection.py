from typing import Any, Dict, List
from pydantic import BaseModel
from .balanced_input import BalancedInput


class BalancedInputCollection(BaseModel):
    inputs: List[BalancedInput]

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "BalancedInputCollection":
        return cls(inputs=[BalancedInput.from_json(item) for item in data.items()])
