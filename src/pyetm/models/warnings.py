from __future__ import annotations
from typing import Any, Optional, Dict, List, Union, Literal
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ModelWarning:
    """Individual warning with context and metadata."""

    field: str
    message: str
    severity: Literal["info", "warning", "error"] = "warning"
    timestamp: datetime = field(default_factory=datetime.now)

    def __str__(self) -> str:
        return f"{self.field}: {self.message}"

    def __repr__(self) -> str:
        return f"ModelWarning(field='{self.field}', message='{self.message}', severity='{self.severity}')"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "field": self.field,
            "message": self.message,
            "severity": self.severity,
            "timestamp": self.timestamp.isoformat(),
        }


class WarningCollector:
    """Manages warnings for a model instance with a clean API."""

    def __init__(self):
        self._warnings: List[ModelWarning] = []

    @classmethod
    def with_warning(
        cls, field: str, message: str, severity: str = "warning"
    ) -> "WarningCollector":
        """
        Convenience method to create a WarningCollector with a single warning.
        """
        collector = cls()
        collector.add(field, message, severity)
        return collector

    def add(
        self,
        field: str,
        message: Union[str, List[str], Dict[str, Any]],
        severity: str = "warning",
    ) -> None:
        """
        Add warning(s) to the collection.
        """
        if isinstance(message, str):
            self._warnings.append(ModelWarning(field, message, severity))

        elif isinstance(message, list):
            for msg in message:
                if isinstance(msg, str):
                    self._warnings.append(ModelWarning(field, msg, severity))
                else:
                    self._warnings.append(ModelWarning(field, str(msg), severity))

        elif isinstance(message, dict):
            # Handle nested warning dictionaries (like from submodels)
            for sub_field, sub_messages in message.items():
                nested_field = f"{field}.{sub_field}"
                self.add(nested_field, sub_messages, severity)
        else:
            # Fallback for any other type
            self._warnings.append(ModelWarning(field, str(message), severity))

    def clear(self, field: Optional[str] = None) -> None:
        """Clear warnings. If field is specified, clear only that field."""
        if field is None:
            self._warnings.clear()
        else:
            self._warnings = [w for w in self._warnings if w.field != field]

    def get_by_field(self, field: str) -> List[ModelWarning]:
        """Get all warnings for a specific field."""
        return [w for w in self._warnings if w.field == field]

    def has_warnings(self, field: Optional[str] = None) -> bool:
        """Check if warnings exist. If field specified, check only that field."""
        if field is None:
            return len(self._warnings) > 0
        return any(w.field == field for w in self._warnings)

    def get_fields_with_warnings(self) -> List[str]:
        """Get list of all fields that have warnings."""
        return list(set(w.field for w in self._warnings))

    def to_dict(self) -> Dict[str, List[Dict[str, Any]]]:
        """Convert to dictionary grouped by field."""
        result = {}
        for warning in self._warnings:
            if warning.field not in result:
                result[warning.field] = []
            result[warning.field].append(warning.to_dict())
        return result

    def merge_from(self, other: "WarningCollector", prefix: str = "") -> None:
        """
        Merge warnings from another collector, optionally with a field prefix.
        """
        for warning in other._warnings:
            field = f"{prefix}.{warning.field}" if prefix else warning.field
            self._warnings.append(
                ModelWarning(
                    field=field,
                    message=warning.message,
                    severity=warning.severity,
                    timestamp=warning.timestamp,
                )
            )

    def merge_submodel_warnings(
        self, *submodels, key_attr: Optional[str] = None
    ) -> None:
        """
        Merge warnings from Base model instances.
        """
        for submodel in submodels:
            if hasattr(submodel, "_warning_collector"):
                # Determine prefix for nested warnings
                prefix = submodel.__class__.__name__
                if key_attr and hasattr(submodel, key_attr):
                    key_value = getattr(submodel, key_attr)
                    prefix = f"{prefix}({key_attr}={key_value})"

                self.merge_from(submodel._warning_collector, prefix)

    def show_warnings(self) -> None:
        """Print all warnings to console in a readable format."""
        if not self._warnings:
            print("No warnings.")
            return

        print("Warnings:")
        grouped = {}
        for warning in self._warnings:
            if warning.field not in grouped:
                grouped[warning.field] = []
            grouped[warning.field].append(warning)

        for field, warnings in grouped.items():
            print(f"  {field}:")
            for warning in warnings:
                severity_indicator = (
                    "[WARNING]"
                    if warning.severity == "warning"
                    else "[ERROR]" if warning.severity == "error" else "[INFO]"
                )
                print(f"    {severity_indicator} {warning.message}")

    def __len__(self) -> int:
        """Return number of warnings."""
        return len(self._warnings)

    def __bool__(self) -> bool:
        """Return True if there are warnings."""
        return len(self._warnings) > 0

    def __iter__(self):
        """Iterate over warnings."""
        return iter(self._warnings)

    def __repr__(self) -> str:
        """Nice string representation showing warning summary."""
        if not self._warnings:
            return "WarningCollector(no warnings)"

        # Group by field for summary
        field_counts = {}
        severity_counts = {"error": 0, "warning": 0, "info": 0}

        for warning in self._warnings:
            field_counts[warning.field] = field_counts.get(warning.field, 0) + 1
            severity_counts[warning.severity] += 1

        # Build summary string
        total = len(self._warnings)
        severity_parts = []
        for sev, count in severity_counts.items():
            if count > 0:
                severity_parts.append(f"{count} {sev}")

        severity_str = ", ".join(severity_parts)
        field_summary = (
            f"{len(field_counts)} fields" if len(field_counts) != 1 else "1 field"
        )

        return (
            f"WarningCollector({total} warnings: {severity_str} across {field_summary})"
        )
