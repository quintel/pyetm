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

    def validate_update(
        self, sortable_type: str, order: List[str], subtype: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate a sortable update and return validation results.

        Args:
            sortable_type: Type of sortable to update
            order: New order to validate
            subtype: Optional subtype for heat_network

        Returns:
            Dict with validation results:
            {
                "valid": bool,
                "errors": List[str],
                "warnings": List[str],
                "target_sortable": Optional[Sortable]
            }
        """
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "target_sortable": None,
        }

        # Find the target sortable
        target_sortable = self.find_sortable(sortable_type, subtype)

        if not target_sortable:
            validation_result["valid"] = False

            # Check if it's a valid sortable type at all
            available_types = list(set(s.type for s in self.sortables))
            if sortable_type not in available_types:
                validation_result["errors"].append(
                    f"Invalid sortable type '{sortable_type}'. Available types: {available_types}"
                )
                return validation_result

            # Check if it's a heat_network subtype issue
            if sortable_type == "heat_network":
                if not subtype:
                    validation_result["errors"].append(
                        "Subtype is required for heat_network sortables"
                    )
                else:
                    available_subtypes = [
                        s.subtype for s in self.sortables if s.type == "heat_network"
                    ]
                    validation_result["errors"].append(
                        f"Invalid heat_network subtype '{subtype}'. Available subtypes: {available_subtypes}"
                    )
            else:
                if subtype:
                    validation_result["errors"].append(
                        f"Subtype '{subtype}' not allowed for sortable type '{sortable_type}'"
                    )
                else:
                    validation_result["errors"].append(
                        f"Could not find sortable '{sortable_type}'"
                    )

            return validation_result

        validation_result["target_sortable"] = target_sortable

        # Validate the order
        current_items = set(target_sortable.order)
        new_items = set(order)

        # Check for invalid items
        invalid_items = new_items - current_items
        if invalid_items:
            validation_result["valid"] = False
            validation_result["errors"].append(
                f"Invalid items for '{target_sortable.name()}': {list(invalid_items)}. "
                f"Available items: {list(current_items)}"
            )

        # Check for duplicates
        if len(order) != len(set(order)):
            duplicates = [item for item in order if order.count(item) > 1]
            validation_result["valid"] = False
            validation_result["errors"].append(
                f"Duplicate items found in order: {list(set(duplicates))}"
            )

        # Add warnings about changes
        missing_items = current_items - new_items
        if missing_items:
            validation_result["warnings"].append(
                f"Items being removed from '{target_sortable.name()}': {list(missing_items)}"
            )

        return validation_result

    def find_sortable(
        self, sortable_type: str, subtype: Optional[str] = None
    ) -> Optional[Sortable]:
        """
        Find a specific sortable by type and optional subtype.

        Args:
            sortable_type: The sortable type to find
            subtype: Optional subtype for heat_network

        Returns:
            The matching Sortable or None if not found
        """
        for sortable in self.sortables:
            if sortable.type == sortable_type:
                if sortable_type == "heat_network":
                    if sortable.subtype == subtype:
                        return sortable
                else:
                    if sortable.subtype is None:
                        return sortable
        return None

    def get_available_items(
        self, sortable_type: str, subtype: Optional[str] = None
    ) -> List[str]:
        """
        Get available items for a specific sortable type.

        Args:
            sortable_type: The sortable type
            subtype: Optional subtype for heat_network

        Returns:
            List of available items

        Raises:
            SortableError: If sortable not found
        """
        sortable = self.find_sortable(sortable_type, subtype)
        if not sortable:
            raise SortableError(
                f"Sortable '{sortable_type}'"
                + (f" with subtype '{subtype}'" if subtype else "")
                + " not found"
            )
        return sortable.order.copy()

    def get_available_types(self) -> List[str]:
        """Get all available sortable types."""
        return list(set(s.type for s in self.sortables))

    def get_available_subtypes(self, sortable_type: str) -> List[str]:
        """Get available subtypes for a sortable type (mainly for heat_network)."""
        return [
            s.subtype
            for s in self.sortables
            if s.type == sortable_type and s.subtype is not None
        ]
