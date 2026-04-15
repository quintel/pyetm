"""
ExportDataCollection: Format-agnostic container for scenario export data.

This module provides a structured way to collect and organize all export data
from scenarios without applying any format-specific transformations. It serves
as an intermediate representation that can be used to export to any file format.
"""

from __future__ import annotations

from typing import Optional, Dict
from pydantic import BaseModel, Field, ConfigDict
import pandas as pd

from pyetm.models.export_config import ExportConfig


class ExportDataCollection(BaseModel):
    """
    Format-agnostic container for scenario export data.

    Stores data as pandas DataFrames/dicts without format-specific transformations.
    See examples/export_data_collection.ipynb for usage examples.
    """

    # Required field - always populated
    main_info: pd.DataFrame = Field(
        ..., description="Scenario metadata (id, title, area, year, etc.)"
    )

    # Optional fields - None when not included in export
    inputs: Optional[pd.DataFrame] = Field(
        None, description="Input parameter values across scenarios"
    )

    sortables: Optional[pd.DataFrame] = Field(
        None, description="Sortable technology data across scenarios"
    )

    custom_curves: Optional[Dict[str, Dict[str, pd.Series]]] = Field(
        None,
        description="Custom curves: {curve_name: {scenario_id: Series}}",
    )

    hourly_output_curves: Optional[Dict[str, Dict[str, pd.DataFrame]]] = Field(
        None,
        description="Hourly output curves: {curve_name: {scenario_id: DataFrame}}",
    )

    annual_exports: Optional[Dict[str, Dict[str, pd.DataFrame]]] = Field(
        None,
        description="Annual exports: {export_name: {scenario_id: DataFrame}}",
    )

    gquery_results: Optional[pd.DataFrame] = Field(
        None, description="Query results across scenarios"
    )

    users: Optional[pd.DataFrame] = Field(
        None, description="User permission data across scenarios"
    )

    # Configuration used to collect this data
    config: ExportConfig = Field(
        ..., description="The ExportConfig used to collect this data"
    )

    model_config = ConfigDict(
        arbitrary_types_allowed=True,  # Allow pandas DataFrames
    )

    def _safe_to_dict(self, obj: pd.DataFrame | pd.Series) -> dict:
        """Convert DataFrame or Series to JSON-serializable dict."""
        if isinstance(obj, pd.Series):
            return self._series_to_dict(obj)
        return self._dataframe_to_dict(obj)

    def _series_to_dict(self, series: pd.Series) -> dict:
        """Convert Series to dict, handling MultiIndex."""
        if isinstance(series.index, pd.MultiIndex):
            return {str(idx): val for idx, val in series.items()}
        return series.to_dict()

    def _dataframe_to_dict(self, df: pd.DataFrame) -> dict:
        """Convert DataFrame to dict, handling MultiIndex."""
        if isinstance(df.columns, pd.MultiIndex):
            return self._multiindex_columns_to_dict(df)
        if isinstance(df.index, pd.MultiIndex):
            return {str(idx): val for idx, val in df.to_dict(orient="index").items()}
        return df.to_dict()

    def _multiindex_columns_to_dict(self, df: pd.DataFrame) -> dict:
        """Convert DataFrame with MultiIndex columns to nested dict."""
        result = {}
        for scenario_id in df.columns.levels[0]:
            scenario_df = df[scenario_id]
            result[str(scenario_id)] = self._safe_to_dict(scenario_df)
        return result

    def to_dict(self) -> dict:
        """Serialize to nested dictionary structure."""
        result = {"main_info": self._safe_to_dict(self.main_info)}

        # Convert simple DataFrames
        for field in ["inputs", "sortables", "gquery_results", "users"]:
            value = getattr(self, field, None)
            if value is not None:
                result[field] = self._safe_to_dict(value)

        # Convert nested dicts
        for field in ["custom_curves", "hourly_output_curves", "annual_exports"]:
            value = getattr(self, field, None)
            if value is not None:
                result[field] = self._convert_nested_dict(value)

        result["config"] = self.config.model_dump()
        return result

    def _convert_nested_dict(self, nested_dict: Dict) -> Dict:
        """Convert nested dict of DataFrames/Series to JSON-serializable structure."""
        return {
            outer_key: {
                inner_key: self._safe_to_dict(data)
                for inner_key, data in inner_dict.items()
            }
            for outer_key, inner_dict in nested_dict.items()
        }

    def __repr__(self) -> str:
        """Human-readable summary of the export data collection."""
        lines = [
            "ExportDataCollection(",
            f"  main_info: {self._format_dataframe_shape(self.main_info)} scenarios)",
        ]

        # Add optional DataFrames
        for field in ["inputs", "sortables", "gquery_results", "users"]:
            lines.extend(self._format_optional_field(field))

        # Add nested dicts
        for field in ["custom_curves", "hourly_output_curves", "annual_exports"]:
            lines.extend(self._format_nested_field(field))

        lines.append(")")
        return "\n".join(lines)

    def _format_dataframe_shape(self, df: pd.DataFrame) -> str:
        """Format DataFrame shape for repr."""
        rows, cols = df.shape
        return f"({rows} fields × {cols}"

    def _format_optional_field(self, field_name: str) -> list[str]:
        """Format optional DataFrame field for repr."""
        value = getattr(self, field_name, None)
        if value is None:
            return []
        rows, cols = value.shape
        label = field_name.replace("_", " ")
        return [f"  {field_name}: ({rows} {label} × {cols} scenarios)"]

    def _format_nested_field(self, field_name: str) -> list[str]:
        """Format nested dict field for repr."""
        value = getattr(self, field_name, None)
        if value is None:
            return []
        num_items = len(value)
        num_scenarios = len(next(iter(value.values()), {}))

        # Map field names to singular item types
        item_type_map = {
            "custom_curves": "curve",
            "hourly_output_curves": "curve",
            "annual_exports": "export",
        }
        item_type = item_type_map.get(field_name, field_name.replace("_", " "))
        plural = f"{item_type}s" if num_items != 1 else item_type

        return [
            f"  {field_name}: {num_items} {plural} across {num_scenarios} scenarios"
        ]
