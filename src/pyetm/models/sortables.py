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

    @classmethod
    def _from_dataframe(cls, df: pd.DataFrame, **kwargs) -> Sortable:
        """
        Deserialize DataFrame back to a single Sortable.
        Expected DataFrame structure: single column with sortable name, order as values.
        """
        if df.empty:
            raise ValueError("Cannot create Sortable from empty DataFrame")

        if len(df.columns) != 1:
            raise ValueError(
                f"Expected single column DataFrame, got {len(df.columns)} columns"
            )

        # Get the sortable name from column name
        sortable_name = df.columns[0]

        # Extract order from DataFrame values (drop NaN values)
        order = df[sortable_name].dropna().tolist()

        # Parse type and subtype from name
        if "_" in sortable_name:
            parts = sortable_name.split("_", 1)
            type_name = parts[0]
            subtype = parts[1]
        else:
            type_name = sortable_name
            subtype = None

        return cls.load_safe(type=type_name, subtype=subtype, order=order)


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

    def _to_dataframe(self, **kwargs) -> pd.DataFrame:
        """
        Serialize the Sortables collection to DataFrame.
        Creates a DataFrame where each column represents a sortable's order.
        Structure: sortable names as columns, order positions as index.
        """
        if not self.sortables:
            return pd.DataFrame()

        # Create a dictionary with sortable names as keys and orders as values
        data_dict = {}
        for sortable in self.sortables:
            try:
                data_dict[sortable.name()] = sortable.order
            except Exception as e:
                data_dict[sortable.name()] = []
                self.add_warning(
                    f"Failed to serialize sortable '{sortable.name()}': {e}"
                )

        result_df = pd.DataFrame.from_dict(data_dict, orient="index").T

        return result_df

    @classmethod
    def _from_dataframe(cls, df: pd.DataFrame, **kwargs) -> Sortables:
        """
        Deserialize DataFrame back to Sortables collection.
        Expected DataFrame structure: sortable names as columns, order positions as index.
        """
        sortables_list = []

        if df.empty:
            return cls.load_safe(sortables=[])

        for sortable_name in df.columns:
            try:
                order = df[sortable_name].dropna().tolist()

                # Parse type and subtype from name
                if "_" in sortable_name:
                    parts = sortable_name.split("_", 1)
                    type_name = parts[0]
                    subtype = parts[1]
                else:
                    type_name = sortable_name
                    subtype = None

                # Create the Sortable object
                sortable_obj = Sortable.load_safe(
                    type=type_name, subtype=subtype, order=order
                )
                sortables_list.append(sortable_obj)

            except Exception as e:
                # Create a minimal sortable with warning
                if "_" in sortable_name:
                    parts = sortable_name.split("_", 1)
                    type_name = parts[0]
                    subtype = parts[1]
                else:
                    type_name = sortable_name
                    subtype = None

                sortable_obj = Sortable.load_safe(
                    type=type_name, subtype=subtype, order=[]
                )
                sortable_obj.add_warning(
                    f"Failed to deserialize sortable '{sortable_name}': {e}"
                )
                sortables_list.append(sortable_obj)

        collection = cls.load_safe(sortables=sortables_list)

        for sortable_obj in sortables_list:
            if hasattr(sortable_obj, "warnings") and sortable_obj.warnings:
                for warning in sortable_obj.warnings:
                    collection.add_warning(
                        f"Sortable '{sortable_obj.name()}': {warning}"
                    )

        return collection
