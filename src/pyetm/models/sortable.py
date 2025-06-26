from typing import Any, List, Optional, Tuple, Union, Dict
from pydantic import BaseModel


class Sortable(BaseModel):
    """
    Represents one sortable order.
    - If payload is a flat list, returns one Sortable.
    - If payload is a dict (heat_network), returns one Sortable per subtype.
    """

    type: str
    order: List[Any]
    subtype: Optional[str] = None

    @classmethod
    def from_json(
        cls, data: Tuple[str, Union[List[Any], Dict[str, List[Any]]]]
    ) -> List["Sortable"]:
        """
        :param data: (sortable_type, payload)
           - payload list → [Sortable(type, order)]
           - payload dict → [Sortable(type, subtype, order) for each subtype]
        :returns: list of Sortable objects
        """
        sort_type, payload = data
        if isinstance(payload, list):
            return [cls(type=sort_type, order=payload)]
        elif isinstance(payload, dict):
            return [
                cls(type=sort_type, subtype=sub, order=order)
                for sub, order in payload.items()
            ]
        else:
            raise ValueError(f"Unexpected payload for '{sort_type}': {payload!r}")
