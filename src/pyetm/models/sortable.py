from __future__ import annotations
from typing import Any, Iterator, Optional, Tuple, Union, Dict

from pyetm.models.base import Base


class SortableError(Exception):
    """Base sortable error"""


class Sortable(Base):
    """
    Represents one sortable order.
    - If payload is a flat list, yields one Sortable.
    - If payload is a dict (heat_network), yields one Sortable per subtype.
    """

    type: str
    order: list[Any]
    subtype: Optional[str] = None

    def name(self):
        """
        Returns the display name
        """
        if self.subtype:
            return f'{self.type}_{self.subtype}'
        else:
            return self.type

    @classmethod
    def from_json(
        cls, data: Tuple[str, Union[list[Any], Dict[str, list[Any]]]]
    ) -> Iterator[Sortable]:
        """
        :param data: (sortable_type, payload)
           - payload list → yield Sortable(type, order)
           - payload dict → yield Sortable(type, subtype, order) for each subtype
        """
        sort_type, payload = data

        if isinstance(payload, list):
            try:
                sortable = cls.model_validate({"type": sort_type, "order": payload})
                yield sortable
            except Exception as e:
                # Create basic sortable with warning
                sortable = cls.model_validate({"type": sort_type, "order": []})
                sortable.add_warning(f"Failed to create sortable for {sort_type}: {e}")
                yield sortable

        elif isinstance(payload, dict):
            for sub, order in payload.items():
                try:
                    sortable = cls.model_validate(
                        {"type": sort_type, "subtype": sub, "order": order}
                    )
                    yield sortable
                except Exception as e:
                    # Create basic sortable with warning
                    sortable = cls.model_validate(
                        {"type": sort_type, "subtype": sub, "order": []}
                    )
                    sortable.add_warning(
                        f"Failed to create sortable for {sort_type}.{sub}: {e}"
                    )
                    yield sortable

        else:
            # Create basic sortable with warning for unexpected payload
            sortable = cls.model_validate({"type": sort_type, "order": []})
            sortable.add_warning(f"Unexpected payload for '{sort_type}': {payload!r}")
            yield sortable
