from __future__ import annotations
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd

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
            return f"{self.type}_{self.subtype}"
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
                sortable.add_warning('base', f"Failed to create sortable for {sort_type}: {e}")
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
                        'base', f"Failed to create sortable for {sort_type}.{sub}: {e}"
                    )
                    yield sortable

        else:
            # Create basic sortable with warning for unexpected payload
            sortable = cls.model_validate({"type": sort_type, "order": []})
            sortable.add_warning('type', f"Unexpected payload for '{sort_type}': {payload!r}")
            yield sortable


class Sortables(Base):
    """
    A flat collection of Sortable instances,
    regardless of whether the source JSON was nested.
    """

    sortables: List[Sortable]

    def __len__(self) -> int:
        return len(self.sortables)

    def __iter__(self):
        yield from self.sortables

    def keys(self) -> List[str]:
        # will repeat 'heat_network' for each subtype
        return [s.type for s in self.sortables]

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Sortables":
        """
        :param data: the raw JSON dict from
                     GET /api/v3/scenarios/:id/user_sortables
        """
        items: List[Sortable] = []
        for pair in data.items():
            items.extend(Sortable.from_json(pair))

        collection = cls.model_validate({"sortables": items})

        # Merge any warnings from individual sortables
        for sortable in items:
            if hasattr(sortable, "warnings") and sortable.warnings:
                for warning in sortable.warnings:
                    collection.add_warning(warning)

        return collection

    def as_dict(self) -> Dict[str, Any]:
        """
        Return a dict mimicking the index endpoint.
        """
        result: Dict[str, Any] = {}
        for s in self.sortables:
            if s.subtype:
                result.setdefault(s.type, {})[s.subtype] = s.order
            else:
                result[s.type] = s.order
        return result

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(
            {s.name(): s.order for s in self.sortables}, orient="index"
        ).T
