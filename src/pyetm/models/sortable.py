from typing import Any, Iterator, Optional, Tuple, Union, Dict
from pydantic import BaseModel


class Sortable(BaseModel):
    """
    Represents one sortable order.
    - If payload is a flat list, yields one Sortable.
    - If payload is a dict (heat_network), yields one Sortable per subtype.
    """

    type: str
    order: list[Any]
    subtype: Optional[str] = None

    @classmethod
    def from_json(
        cls, data: Tuple[str, Union[list[Any], Dict[str, list[Any]]]]
    ) -> Iterator["Sortable"]:
        """
        :param data: (sortable_type, payload)
           - payload list → yield Sortable(type, order)
           - payload dict → yield Sortable(type, subtype, order) for each subtype
        """
        sort_type, payload = data

        if isinstance(payload, list):
            yield cls(type=sort_type, order=payload)

        elif isinstance(payload, dict):
            for sub, order in payload.items():
                yield cls(type=sort_type, subtype=sub, order=order)

        else:
            raise ValueError(f"Unexpected payload for '{sort_type}': {payload!r}")
