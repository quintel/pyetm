from __future__ import annotations
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd
from pydantic import field_validator, model_validator
from pyetm.models.warnings import WarningCollector

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

    def is_valid_update(self, new_order: list[Any]) -> WarningCollector:
        """
        Returns a WarningCollector with validation warnings without updating the current object
        """
        new_obj_dict = self.model_dump()
        new_obj_dict["order"] = new_order

        warnings_obj = self.__class__(**new_obj_dict)
        return warnings_obj.warnings

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        """Validate that type is a non-empty string"""
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Type must be a non-empty string")
        return value.strip()

    @field_validator("subtype")
    @classmethod
    def validate_subtype(cls, value: Optional[str]) -> Optional[str]:
        """Validate subtype if provided"""
        if value is not None:
            if not isinstance(value, str) or not value.strip():
                raise ValueError("Subtype must be a non-empty string or None")
            return value.strip()
        return value

    @field_validator("order")
    @classmethod
    def validate_order(cls, value: list[Any]) -> list[Any]:
        """Validate that order is a list and check for duplicates"""
        if not isinstance(value, list):
            raise ValueError("Order must be a list")

        # Check for duplicates
        seen = set()
        duplicates = []
        for item in value:
            if item in seen:
                duplicates.append(item)
            seen.add(item)

        if duplicates:
            raise ValueError(f"Order contains duplicate items: {duplicates}")

        return value

    @model_validator(mode="after")
    def validate_sortable_consistency(self) -> "Sortable":
        """Additional validation for the entire sortable"""
        if self.type == "heat_network" and self.subtype is None:
            raise ValueError("heat_network type requires a subtype")

        if len(self.order) > 17:
            raise ValueError("Order cannot contain more than 17 items")

        return self

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
            sortable = cls(type=sort_type, order=payload)
            yield sortable

        elif isinstance(payload, dict):
            for sub, order in payload.items():
                sortable = cls(type=sort_type, subtype=sub, order=order)
                yield sortable

        else:
            # Create basic sortable with warning for unexpected payload
            sortable = cls(type=sort_type, order=[])
            sortable.add_warning(
                "payload", f"Unexpected payload for '{sort_type}': {payload!r}"
            )
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

    def names(self) -> List[str]:
        """Get all sortable names (including subtype suffixes)"""
        return [s.name() for s in self.sortables]

    def is_valid_update(
        self, updates: Dict[str, list[Any]]
    ) -> Dict[str, WarningCollector]:
        """
        Returns a dict mapping sortable names to their WarningCollectors when errors were found

        :param updates: Dict mapping sortable names to new orders
        :return: Dict mapping sortable names to WarningCollectors
        """
        warnings = {}

        # Check each sortable that has an update
        sortable_by_name = {s.name(): s for s in self.sortables}

        for name, new_order in updates.items():
            if name in sortable_by_name:
                sortable = sortable_by_name[name]
                sortable_warnings = sortable.is_valid_update(new_order)
                if len(sortable_warnings) > 0:
                    warnings[name] = sortable_warnings
            else:
                warnings[name] = WarningCollector.with_warning(
                    name, "Sortable does not exist"
                )

        # Check for non-existent sortables
        non_existent_names = set(updates.keys()) - set(self.names())
        for name in non_existent_names:
            if name not in warnings:  # Don't overwrite existing warnings
                warnings[name] = WarningCollector.with_warning(
                    name, "Sortable does not exist"
                )

        return warnings

    def update(self, updates: Dict[str, list[Any]]):
        """
        Update the orders of specified sortables

        :param updates: Dict mapping sortable names to new orders
        """
        sortable_by_name = {s.name(): s for s in self.sortables}

        for name, new_order in updates.items():
            if name in sortable_by_name:
                sortable_by_name[name].order = new_order

    @field_validator("sortables")
    @classmethod
    def validate_sortables_list(cls, value: List[Sortable]) -> List[Sortable]:
        """Validate the list of sortables"""
        if not isinstance(value, list):
            raise ValueError("Sortables must be a list")

        # Check for duplicate names
        names = [s.name() for s in value if isinstance(s, Sortable)]
        duplicates = []
        seen = set()
        for name in names:
            if name in seen:
                duplicates.append(name)
            seen.add(name)

        if duplicates:
            raise ValueError(f"Duplicate sortable names found: {duplicates}")

        return value

    @model_validator(mode="after")
    def validate_sortables_consistency(self) -> "Sortables":
        """Additional validation for the entire sortables collection"""
        heat_network_types = [s for s in self.sortables if s.type == "heat_network"]
        if len(heat_network_types) > 0:
            # All heat_network sortables should have subtypes
            without_subtypes = [s for s in heat_network_types if s.subtype is None]
            if without_subtypes:
                raise ValueError("All heat_network sortables must have subtypes")

        return self

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Sortables":
        """
        :param data: the raw JSON dict from
                     GET /api/v3/scenarios/:id/user_sortables
        """
        items: List[Sortable] = []
        for pair in data.items():
            items.extend(Sortable.from_json(pair))

        # Use Base class constructor that handles validation gracefully
        collection = cls(sortables=items)

        collection._merge_submodel_warnings(*items, key_attr="type")

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
        """
        return pd.DataFrame.from_dict(
            {s.name(): s.order for s in self.sortables}, orient="index"
        ).T

    @classmethod
    def _from_dataframe(cls, df: pd.DataFrame, **kwargs) -> "Sortables":
        if df is None:
            return cls(sortables=[])

        # Ensure DataFrame
        if isinstance(df, pd.Series):
            df = df.to_frame(name=str(df.name))

        def _extract_order(series: pd.Series) -> List[Any]:
            s = series.dropna()
            if s.dtype == object:
                s = s.astype(str).map(lambda v: v.strip()).replace({"": pd.NA}).dropna()
            return s.tolist()

        items: List[Sortable] = []
        for col in df.columns:
            name = str(col)
            order = _extract_order(df[col])
            if not order:
                continue

            if name.startswith("heat_network_"):
                subtype = name[len("heat_network_") :]
                items.append(
                    Sortable(type="heat_network", subtype=subtype, order=order)
                )
            else:
                items.append(Sortable(type=name, order=order))

        return cls(sortables=items)

    def to_updates_dict(self) -> Dict[str, List[Any]]:
        return {s.name(): s.order for s in self.sortables}
