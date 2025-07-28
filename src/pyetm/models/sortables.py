from __future__ import annotations
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd

from pyetm.models.base import Base


class SortableError(Exception):
    """Base sortable error"""


class ValidationResult:
    """Simple validation result container"""

    def __init__(
        self, valid: bool = True, errors: List[str] = None, warnings: List[str] = None
    ):
        self.valid = valid
        self.errors = errors or []
        self.warnings = warnings or []

    def add_error(self, error: str):
        self.errors.append(error)
        self.valid = False

    def add_warning(self, warning: str):
        self.warnings.append(warning)

    def merge(self, other: "ValidationResult"):
        """Merge another validation result into this one"""
        if not other.valid:
            self.valid = False
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)


class Sortable(Base):
    """
    Represents one sortable order.
    """

    type: str
    order: list[Any]
    subtype: Optional[str] = None

    def name(self) -> str:
        """Returns the display name"""
        return f"{self.type}_{self.subtype}" if self.subtype else self.type

    def matches(self, sortable_type: str, subtype: Optional[str] = None) -> bool:
        """Check if this sortable matches the given type and subtype"""
        if self.type != sortable_type:
            return False

        if sortable_type == "heat_network":
            return self.subtype == subtype
        else:
            return self.subtype is None

    def validate_order_update(self, new_order: List[str]) -> ValidationResult:
        """
        Validate a new order for this sortable.

        Args:
            new_order: The proposed new order

        Returns:
            ValidationResult with validation details
        """
        result = ValidationResult()

        current_items = set(self.order)
        new_items = set(new_order)

        # Check for invalid items
        invalid_items = new_items - current_items
        if invalid_items:
            result.add_error(
                f"Invalid items for '{self.name()}': {list(invalid_items)}. "
                f"Available items: {list(current_items)}"
            )

        # Check for duplicates
        if len(new_order) != len(set(new_order)):
            duplicates = [item for item in new_order if new_order.count(item) > 1]
            result.add_error(f"Duplicate items found in order: {list(set(duplicates))}")

        # Add warnings about changes
        missing_items = current_items - new_items
        if missing_items:
            result.add_warning(
                f"Items being removed from '{self.name()}': {list(missing_items)}"
            )

        return result

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
                yield cls.model_validate({"type": sort_type, "order": payload})
            except Exception as e:
                sortable = cls.model_validate({"type": sort_type, "order": []})
                sortable.add_warning(f"Failed to create sortable for {sort_type}: {e}")
                yield sortable

        elif isinstance(payload, dict):
            for sub, order in payload.items():
                try:
                    yield cls.model_validate(
                        {"type": sort_type, "subtype": sub, "order": order}
                    )
                except Exception as e:
                    sortable = cls.model_validate(
                        {"type": sort_type, "subtype": sub, "order": []}
                    )
                    sortable.add_warning(
                        f"Failed to create sortable for {sort_type}.{sub}: {e}"
                    )
                    yield sortable
        else:
            sortable = cls.model_validate({"type": sort_type, "order": []})
            sortable.add_warning(f"Unexpected payload for '{sort_type}': {payload!r}")
            yield sortable


class Sortables(Base):
    """
    A flat collection of Sortable instances.
    """

    sortables: List[Sortable]

    def __len__(self) -> int:
        return len(self.sortables)

    def __iter__(self):
        yield from self.sortables

    def keys(self) -> List[str]:
        return [s.type for s in self.sortables]

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Sortables":
        """Create Sortables from API response"""
        items: List[Sortable] = []
        for pair in data.items():
            items.extend(Sortable.from_json(pair))

        collection = cls.model_validate({"sortables": items})

        # Merge warnings from individual sortables
        for sortable in items:
            if hasattr(sortable, "warnings") and sortable.warnings:
                for warning in sortable.warnings:
                    collection.add_warning(warning)

        return collection

    def find_sortable(
        self, sortable_type: str, subtype: Optional[str] = None
    ) -> Optional[Sortable]:
        """Find a sortable by type and subtype"""
        return next(
            (s for s in self.sortables if s.matches(sortable_type, subtype)), None
        )

    def validate_update(
        self, sortable_type: str, order: List[str], subtype: Optional[str] = None
    ) -> ValidationResult:
        """
        Validate a sortable update.

        Args:
            sortable_type: Type of sortable to update
            order: New order to validate
            subtype: Optional subtype for heat_network

        Returns:
            ValidationResult with validation details
        """
        result = ValidationResult()

        # First, validate that the sortable exists and type/subtype are valid
        sortable_validation = self._validate_sortable_exists(sortable_type, subtype)
        if not sortable_validation.valid:
            return sortable_validation

        # Get the target sortable and validate the order
        target_sortable = self.find_sortable(sortable_type, subtype)
        order_validation = target_sortable.validate_order_update(order)

        # Merge validations
        result.merge(sortable_validation)
        result.merge(order_validation)

        return result

    def _validate_sortable_exists(
        self, sortable_type: str, subtype: Optional[str] = None
    ) -> ValidationResult:
        """Validate that a sortable type and subtype combination exists"""
        result = ValidationResult()

        available_types = self.get_available_types()

        # Check if sortable type exists
        if sortable_type not in available_types:
            result.add_error(
                f"Invalid sortable type '{sortable_type}'. Available types: {available_types}"
            )
            return result

        # Special handling for heat_network
        if sortable_type == "heat_network":
            if not subtype:
                result.add_error("Subtype is required for heat_network sortables")
                return result

            available_subtypes = self.get_available_subtypes(sortable_type)
            if subtype not in available_subtypes:
                result.add_error(
                    f"Invalid heat_network subtype '{subtype}'. Available subtypes: {available_subtypes}"
                )
                return result
        else:
            if subtype:
                result.add_error(
                    f"Subtype '{subtype}' not allowed for sortable type '{sortable_type}'"
                )
                return result

        # Check if the specific combination exists
        if not self.find_sortable(sortable_type, subtype):
            result.add_error(
                f"Could not find sortable '{sortable_type}'"
                + (f" with subtype '{subtype}'" if subtype else "")
            )

        return result

    def get_available_items(
        self, sortable_type: str, subtype: Optional[str] = None
    ) -> List[str]:
        """Get available items for a specific sortable type"""
        sortable = self.find_sortable(sortable_type, subtype)
        if not sortable:
            raise SortableError(
                f"Sortable '{sortable_type}'"
                + (f" with subtype '{subtype}'" if subtype else "")
                + " not found"
            )
        return sortable.order.copy()

    def get_available_types(self) -> List[str]:
        """Get all available sortable types"""
        return list(set(s.type for s in self.sortables))

    def get_available_subtypes(self, sortable_type: str) -> List[str]:
        """Get available subtypes for a sortable type"""
        return [
            s.subtype
            for s in self.sortables
            if s.type == sortable_type and s.subtype is not None
        ]

    def as_dict(self) -> Dict[str, Any]:
        """Return a dict mimicking the index endpoint"""
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
