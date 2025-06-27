from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Optional

from pyetm.clients import BaseClient
from pyetm.models.base import Base
from pyetm.services.scenario_runners.fetch_custom_curves import DownloadCurveRunner
from pyetm.config.settings import get_settings


class CustomCurveError(Exception):
    """Base custom curve error"""


class CustomCurve(Base):
    """
    Wrapper around a single custom curve.
    Curves are getting saved to the filesystem, as bulk processing of scenarios
    could end up with several 100 MBs of curves, which we don't want to keep in
    memory.
    """

    key: str
    type: str
    file_path: Optional[Path] = None

    def available(self) -> bool:
        return bool(self.file_path)

    def retrieve(self, client, scenario) -> Optional[pd.DataFrame]:
        """Process curve from client, save to file, set file_path"""
        try:
            result = DownloadCurveRunner.run(client, scenario, self.key)

            if result.success:
                try:
                    curve = pd.read_csv(
                        result.data, header=None, index_col=False, dtype=float
                    ).squeeze('columns').dropna(how='all')

                    self.file_path = (
                        get_settings().path_to_tmp(str(scenario.id)) / f"{self.key}.csv"
                    )
                    curve.to_csv(self.file_path, index=False)
                    return curve.rename(self.key)
                except Exception as e:
                    # File processing error - add warning and return None
                    self.add_warning(
                        f"Failed to process curve data for {self.key}: {e}"
                    )
                    return None
            else:
                # API call failed - add warning for each error
                for error in result.errors:
                    self.add_warning(f"Failed to retrieve curve {self.key}: {error}")
                return None

        except Exception as e:
            # Unexpected error - add warning
            self.add_warning(f"Unexpected error retrieving curve {self.key}: {e}")
            return None

    def contents(self) -> Optional[pd.Series]:
        """Open file from path and return contents"""
        if not self.available():
            self.add_warning(f"Curve {self.key} not available - no file path set")
            return None

        try:
            return (
                pd.read_csv(self.file_path, header=None, index_col=False, dtype=float)
                .squeeze("columns")
                .dropna(how="all")
                .rename(self.key)
            )
        except Exception as e:
            self.add_warning(f"Failed to read curve file for {self.key}: {e}")
            return None

    def remove(self) -> bool:
        """Remove file and clear path"""
        if not self.available():
            return True

        try:
            self.file_path.unlink(missing_ok=True)
            self.file_path = None
            return True
        except Exception as e:
            self.add_warning(f"Failed to remove curve file for {self.key}: {e}")
            return False

    @classmethod
    def from_json(cls, data: dict) -> CustomCurve:
        """
        Initialize a CustomCurve from JSON data
        """
        try:
            curve = cls.model_validate(data)
            return curve
        except Exception as e:
            # Create basic curve with warning attached
            basic_data = {
                "key": data.get("key", "unknown"),
                "type": data.get("type", "unknown"),
            }
            curve = cls.model_validate(basic_data)
            curve.add_warning(f"Failed to create curve from data: {e}")
            return curve


class CustomCurves(Base):
    curves: list[CustomCurve]

    def __len__(self) -> int:
        return len(self.curves)

    def __iter__(self):
        yield from iter(self.curves)

    def is_attached(self, curve_name: str) -> bool:
        ''' Returns true if that curve is attached '''
        return any((curve_name == key for key in self.attached_keys()))

    def attached_keys(self):
        """Returns the keys of attached curves"""
        yield from (curve.key for curve in self.curves)

    def get_contents(
        self, scenario, curve_name: str
    ) -> Optional[pd.DataFrame | pd.Series]:
        curve = self._find(curve_name)

        if not curve:
            self.add_warning(f"Curve {curve_name} not found in collection")
            return None

        if not curve.available():
            # Try to retrieve it
            result = curve.retrieve(BaseClient(), scenario)
            # Merge any warnings from the curve retrieval
            self._merge_submodel_warnings(curve)
            return result
        else:
            contents = curve.contents()
            # Merge any warnings from reading contents
            self._merge_submodel_warnings(curve)
            return contents

    def _find(self, curve_name: str) -> Optional[CustomCurve]:
        return next((c for c in self.curves if c.key == curve_name), None)

    @classmethod
    def from_json(cls, data: list[dict]) -> CustomCurves:
        """
        Initialize CustomCurves collection from JSON data
        """
        curves = []
        collection_warnings = []

        for curve_data in data:
            try:
                curve = CustomCurve.from_json(curve_data)
                curves.append(curve)
            except Exception as e:
                # Log the problematic curve but continue processing
                collection_warnings.append(f"Skipped invalid curve data: {e}")

        collection = cls.model_validate({"curves": curves})

        # Add any collection-level warnings
        for warning in collection_warnings:
            collection.add_warning(warning)

        # Merge warnings from individual curves
        for curve in curves:
            collection._merge_submodel_warnings(curve)

        return collection
