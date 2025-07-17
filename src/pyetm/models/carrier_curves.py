from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Optional
from pyetm.clients import BaseClient
from pyetm.models.base import Base
from pyetm.config.settings import get_settings
from pyetm.services.scenario_runners.fetch_carrier_curves import (
    DownloadCarrierCurveRunner,
)


class CarrierCurveError(Exception):
    """Base carrier curve error"""


class CarrierCurve(Base):
    """
    Wrapper around a single carrier curve (output curve).
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
        file_path = (
            get_settings().path_to_tmp(str(scenario.id))
            / f"{self.key.replace('/','-')}.csv"
        )

        # TODO: Examine the caching situation in the future if time permits: could be particularly
        # relevant for bulk processing
        # if file_path.is_file():
        #     self.file_path = file_path
        #     return self.contents()
        try:
            result = DownloadCarrierCurveRunner.run(client, scenario, self.key)
            if result.success:
                try:
                    result.data.seek(0)
                    df = pd.read_csv(result.data, index_col=0)
                    df_clean = df.dropna(how="all")
                    df_clean.index = range(len(df_clean))
                    self.file_path = file_path
                    df_clean.to_csv(self.file_path, index=True)
                    return df_clean

                except Exception as e:
                    self.add_warning(
                        f"Failed to process curve data for {self.key}: {e}"
                    )
                    return None

        except Exception as e:
            # Unexpected error - add warning
            self.add_warning(f"Unexpected error retrieving curve {self.key}: {e}")
            return None

    def contents(self) -> Optional[pd.DataFrame]:
        """Open file from path and return contents"""
        if not self.available():
            self.add_warning(f"Curve {self.key} not available - no file path set")
            return None

        try:
            df = pd.read_csv(self.file_path, index_col=0)
            return df.dropna(how="all")
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
    def from_json(cls, data: dict) -> CarrierCurve:
        """
        Initialize a CarrierCurve from JSON data
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


class CarrierCurves(Base):
    curves: list[CarrierCurve]

    def __len__(self) -> int:
        return len(self.curves)

    def __iter__(self):
        yield from iter(self.curves)

    def is_attached(self, curve_name: str) -> bool:
        """Returns true if that curve is attached"""
        return any((curve_name == key for key in self.attached_keys()))

    def attached_keys(self):
        """Returns the keys of attached curves"""
        yield from (curve.key for curve in self.curves)

    def get_contents(self, scenario, curve_name: str) -> Optional[pd.DataFrame]:
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

    def get_curves_by_carrier_type(
        self, scenario, carrier_type: str
    ) -> dict[str, pd.DataFrame]:
        """
        Get all curves for a specific carrier type.

        Args:
            scenario: The scenario object
            carrier_type: One of 'electricity', 'heat', 'hydrogen', 'methane'

        Returns:
            Dictionary mapping curve names to DataFrames
        """
        carrier_mapping = {
            "electricity": ["merit_order", "electricity_price", "residual_load"],
            "heat": [
                "heat_network",
                "agriculture_heat",
                "household_heat",
                "buildings_heat",
            ],
            "hydrogen": ["hydrogen", "hydrogen_integral_cost"],
            "methane": ["network_gas"],
        }

        if carrier_type not in carrier_mapping:
            valid_types = ", ".join(carrier_mapping.keys())
            self.add_warning(
                f"Invalid carrier type '{carrier_type}'. Valid types: {valid_types}"
            )
            return {}

        results = {}
        for curve_name in carrier_mapping[carrier_type]:
            curve_data = self.get_contents(scenario, curve_name)
            if curve_data is not None:
                results[curve_name] = curve_data

        return results

    def _find(self, curve_name: str) -> Optional[CarrierCurve]:
        return next((c for c in self.curves if c.key == curve_name), None)

    @classmethod
    def from_json(cls, data: list[dict]) -> CarrierCurves:
        """
        Initialize CarrierCurves collection from JSON data
        """
        curves = []
        collection_warnings = []

        for curve_data in data:
            try:
                curve = CarrierCurve.from_json(curve_data)
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

    @classmethod
    def from_service_result(
        cls, service_result, scenario, cache_curves: bool = True
    ) -> "CarrierCurves":
        """Create CarrierCurves instance from service result"""
        if not service_result.success or not service_result.data:
            empty_curves = cls(curves=[])
            for error in service_result.errors:
                empty_curves.add_warning(f"Service error: {error}")
            return empty_curves

        curves_list = []

        if cache_curves:
            cache_dir = get_settings().path_to_tmp(str(scenario.id))
            cache_dir.mkdir(parents=True, exist_ok=True)

        for curve_name, curve_data in service_result.data.items():
            try:
                curve = CarrierCurve.model_validate(
                    {"key": curve_name, "type": cls._infer_curve_type(curve_name)}
                )

                if cache_curves:
                    curve_file = cache_dir / f"{curve_name.replace('/', '-')}.csv"
                    curve_data.seek(0)
                    df = pd.read_csv(curve_data, index_col=0)
                    df_clean = df.dropna(how="all")
                    df_clean.to_csv(curve_file, index=True)
                    curve.file_path = curve_file

                curves_list.append(curve)

            except Exception as e:
                curves_list.append(
                    CarrierCurve.model_validate({"key": curve_name, "type": "unknown"})
                )
                curves_list[-1].add_warning(f"Failed to process curve data: {e}")

        curves_collection = cls(curves=curves_list)

        for error in service_result.errors:
            curves_collection.add_warning(f"Download warning: {error}")

        for curve in curves_list:
            curves_collection._merge_submodel_warnings(curve)

        return curves_collection

    @staticmethod
    def _infer_curve_type(curve_name: str) -> str:
        """Infer curve type from curve name."""
        type_mapping = {
            "electricity_price": "price_curve",
            "merit_order": "merit_curve",
            "heat_network": "load_curve",
            "agriculture_heat": "merit_curve",
            "household_heat": "fever_curve",
            "buildings_heat": "fever_curve",
            "hydrogen": "reconciliation_curve",
            "network_gas": "reconciliation_curve",
            "residual_load": "query_curve",
            "hydrogen_integral_cost": "query_curve",
        }
        return type_mapping.get(curve_name, "carrier_curve")

    @classmethod
    def fetch_all(cls, scenario, cache_curves: bool = True) -> "CarrierCurves":
        """
        Convenience method to fetch all carrier curves for a scenario.
        """
        from pyetm.services.scenario_runners.fetch_carrier_curves import (
            FetchAllCarrierCurvesRunner,
        )

        service_result = FetchAllCarrierCurvesRunner.run(BaseClient(), scenario)
        return cls.from_service_result(service_result, scenario, cache_curves)

    @classmethod
    def create_empty_collection(cls) -> "CarrierCurves":
        """
        Create a collection with all known carrier curve types but no data.
        This allows is_attached() to work before data is retrieved.
        """
        from pyetm.services.scenario_runners.fetch_carrier_curves import (
            FetchAllCarrierCurvesRunner,
        )

        curves_list = []
        for curve_name in FetchAllCarrierCurvesRunner.CURVE_TYPES:
            curve = CarrierCurve.model_validate(
                {"key": curve_name, "type": cls._infer_curve_type(curve_name)}
            )
            curves_list.append(curve)

        return cls(curves=curves_list)
