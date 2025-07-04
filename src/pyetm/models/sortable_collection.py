import pandas as pd

from typing import Any, Dict, List
from pyetm.models.base import Base
from .sortable import Sortable


class SortableCollection(Base):
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
            { s.name() : s.order for s in self.sortables}, orient='index'
        ).T
