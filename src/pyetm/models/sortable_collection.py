from typing import Any, Dict, List
from pydantic import BaseModel
from .sortable import Sortable


class SortableCollection(BaseModel):
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
    def from_json(cls, data: Dict[str, Any]) -> "SortableCollection":
        """
        :param data: the raw JSON dict from
                     GET /api/v3/scenarios/:id/user_sortables
        """
        items: List[Sortable] = []
        for pair in data.items():
            items.extend(Sortable.from_json(pair))
        return cls(sortables=items)

    def as_dict(self) -> Dict[str, Any]:
        """
        Turn back into the same dict shape the index endpoint returned,
        nesting heat_network subtypes under 'heat_network' and flat lists
        for the rest.
        """
        result: Dict[str, Any] = {}
        for s in self.sortables:
            if s.subtype:
                result.setdefault(s.type, {})[s.subtype] = s.order
            else:
                result[s.type] = s.order
        return result
