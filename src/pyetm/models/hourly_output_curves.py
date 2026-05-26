"""Hourly output curve data models."""

from __future__ import annotations
from functools import lru_cache
import logging
import pandas as pd
from pathlib import Path
from typing import Any, Optional, cast
import os

import yaml

logger = logging.getLogger(__name__)
from pyetm.clients import BaseClient, get_client
from pyetm.models.base import Base
from pyetm.models.warnings import WarningCollector
from pyetm.config.settings import get_settings
from pyetm.services.scenario_runners.fetch_hourly_output_curves import (
    DownloadHourlyOutputCurveRunner,
    FetchAllHourlyOutputCurvesRunner,
)


# Small LRU cache for reading CSVs from disk. Uses mtime to invalidate when file changes.
def _read_csv_cached(path: Path) -> pd.DataFrame:
    return _read_csv_cached_impl(str(path), os.path.getmtime(path))


# TODO determine appropriate maxsize
@lru_cache(maxsize=64)
def _read_csv_cached_impl(path_str: str, mtime: float) -> pd.DataFrame:
    df = pd.read_csv(path_str, index_col=0)
    return df.dropna(how="all")


class HourlyOutputCurveError(Exception):
    """Base hourly output curve error"""


class HourlyOutputCurve(Base):
    """
    Wrapper around a single hourly output curve (export in the front end).
    Curves are getting saved to the filesystem, as bulk processing of scenarios
    could end up with several 100 MBs of curves, which we don't want to keep in
    memory.
    """

    key: str
    type: str
    file_path: Optional[Path] = None

    @staticmethod
    def _normalize_to_session(scenario: Any) -> Any:
        """
        Normalize scenario parameter to Session object to extract ETEngine session ID.

        Args:
            scenario: Either a Session object or a Scenario (SavedScenario) object

        Returns:
            Session object with the ETEngine session ID
        """
        # Import at function level to avoid circular imports
        from pyetm.models.scenario import Scenario

        # If it's a SavedScenario, extract the underlying Session
        if isinstance(scenario, Scenario):
            return scenario.session

        # Otherwise assume it's already a Session
        return scenario

    def available(self) -> bool:
        return bool(self.file_path)

    def retrieve(
        self, client: Any, scenario: Any, force_refresh: bool = False
    ) -> Optional[pd.DataFrame]:
        """Process curve from client, save to file, set file_path"""
        # Normalize to Session to get ETEngine session ID
        session = self._normalize_to_session(scenario)

        file_path = (
            get_settings().path_to_tmp(str(session.id))
            / f"{self.key.replace('/', '-')}.csv"
        )

        # Reuse a cached file if present unless explicitly refreshing.
        if not force_refresh and file_path.is_file():
            self.file_path = file_path
            if self.file_path is not None:
                try:
                    return _read_csv_cached(self.file_path)
                except Exception as e:
                    # Fall through to re-download on cache read failure
                    self.add_warning(
                        "file_path",
                        f"Failed to read cached curve file for {self.key}: {e}; refetching",
                    )
        try:
            result = DownloadHourlyOutputCurveRunner.run(client, session, self.key)
            if result.success and result.data is not None:
                try:
                    result.data.seek(0)
                    df = pd.read_csv(result.data, index_col=0)
                    df_clean = df.dropna(how="all")
                    df_clean.index = pd.RangeIndex(len(df_clean))
                    # Create parent directory before saving
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    df_clean.to_csv(file_path, index=True)
                    # Only set file_path after successful save
                    self.file_path = file_path
                    return df_clean

                except Exception as e:
                    error_msg = f"Failed to process curve data for {self.key}: {e}"
                    self.add_warning("data", error_msg)
                    logger.exception(error_msg)
                    return None
            else:
                # API call failed or returned no data
                errors = (
                    result.errors if hasattr(result, "errors") else ["Unknown error"]
                )
                error_msg = f"Failed to download curve {self.key}: {errors}"
                self.add_warning("download", error_msg)
                logger.error(error_msg)
                return None

        except Exception as e:
            error_msg = f"Unexpected error retrieving curve {self.key}: {e}"
            self.add_warning("base", error_msg)
            logger.exception(error_msg)
            return None

    def contents(self) -> Optional[pd.DataFrame]:
        """Open file from path and return contents"""
        if not self.available():
            self.add_warning(
                "file_path", f"Curve {self.key} not available - no file path set"
            )
            return None

        try:
            if self.file_path is None:
                self.add_warning("file_path", f"Curve {self.key} has no file path set")
                return None
            return _read_csv_cached(self.file_path)
        except Exception as e:
            self.add_warning(
                "file_path", f"Failed to read curve file for {self.key}: {e}"
            )
            return None

    def remove(self) -> bool:
        """Remove file and clear path"""
        if not self.available() or self.file_path is None:
            return True

        try:
            self.file_path.unlink(missing_ok=True)
            self.file_path = None
            return True
        except Exception as e:
            self.add_warning(
                "file_path", f"Failed to remove curve file for {self.key}: {e}"
            )
            return False

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "HourlyOutputCurve":
        """
        Initialize a HourlyOutputCurve from JSON data
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
            curve = cls.model_construct(**basic_data)
            curve.add_warning("base", f"Failed to create curve from data: {e}")
            return curve


class HourlyOutputCurves(Base):
    """
    Collection of Hourly Output Curves.
    """

    curves: list[HourlyOutputCurve]

    @staticmethod
    def _normalize_to_session(scenario: Any) -> Any:
        """
        Normalize scenario parameter to Session object to extract ETEngine session ID.

        Args:
            scenario: Either a Session object or a Scenario (SavedScenario) object

        Returns:
            Session object with the ETEngine session ID
        """
        # Import at function level to avoid circular imports
        from pyetm.models.scenario import Scenario

        # If it's a SavedScenario, extract the underlying Session
        if isinstance(scenario, Scenario):
            return scenario.session

        # Otherwise assume it's already a Session
        return scenario

    def __len__(self) -> int:
        return len(self.curves)

    def __iter__(self) -> Any:
        yield from iter(self.curves)

    def is_attached(self, curve_name: str) -> bool:
        """Returns true if that curve is attached"""
        return any((curve_name == key for key in self.attached_keys()))

    def attached_keys(self) -> list[str]:
        """Returns the keys of attached curves."""
        return [curve.key for curve in self.curves]

    def get_contents(self, scenario: Any, curve_name: str) -> Optional[pd.DataFrame]:
        # Normalize to Session to get ETEngine session ID
        session = self._normalize_to_session(scenario)

        curve = self._find(curve_name)

        if not curve:
            self.add_warning("curves", f"Curve {curve_name} not found in collection")
            return None

        if not curve.available():
            # Try to attach a cached file from disk first
            expected_path = (
                get_settings().path_to_tmp(str(session.id))
                / f"{curve.key.replace('/', '-')}.csv"
            )
            if expected_path.is_file():
                curve.file_path = expected_path
                contents = curve.contents()
                self._merge_submodel_warnings(curve, key_attr="key")
                return contents

            result = curve.retrieve(get_client(), session)
            self._merge_submodel_warnings(curve, key_attr="key")
            return result
        else:
            contents = curve.contents()
            self._merge_submodel_warnings(curve, key_attr="key")
            return contents

    @staticmethod
    @lru_cache(maxsize=1)
    def _load_carrier_mappings() -> dict[str, list[str]]:
        """Load carrier mappings from YAML config file"""
        config_path = (
            Path(__file__).parent.parent / "config" / "hourly_output_curve_mappings.yml"
        )
        try:
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)
                if config is not None and isinstance(config, dict):
                    return cast(
                        dict[str, list[str]], config.get("carrier_mappings", {})
                    )
                return {}
        except (FileNotFoundError, yaml.YAMLError):
            # Fallback to hardcoded mappings
            result: dict[str, list[str]] = {
                "electricity": ["merit_order"],
                "heat": ["heat_network"],
                "hydrogen": ["hydrogen"],
                "methane": ["network_gas"],
            }
            return result

    def get_curves_by_carrier_type(
        self, scenario: Any, carrier_type: str
    ) -> dict[str, pd.DataFrame]:
        """
        Get all curves for a specific carrier type.

        Args:
            scenario: The scenario object
            carrier_type: One of 'electricity', 'heat', 'hydrogen', 'methane'

        Returns:
            Dictionary mapping curve names to DataFrames
        """
        carrier_mapping = self._load_carrier_mappings()

        if carrier_type not in carrier_mapping:
            valid_types = ", ".join(carrier_mapping.keys())
            self.add_warning(
                "carrier_type",
                f"Invalid carrier type '{carrier_type}'. Valid types: {valid_types}",
            )
            return {}

        results = {}
        for curve_name in carrier_mapping[carrier_type]:
            curve_data = self.get_contents(scenario, curve_name)
            if curve_data is not None:
                results[curve_name] = curve_data

        return results

    def _find(self, curve_name: str) -> Optional[HourlyOutputCurve]:
        return next((c for c in self.curves if c.key == curve_name), None)

    @classmethod
    def from_json(cls, data: list[dict[str, Any]]) -> "HourlyOutputCurves":
        """
        Initialize HourlyOutputCurves collection from JSON data
        """
        curves = []

        for curve_data in data:
            try:
                curve = HourlyOutputCurve.from_json(curve_data)
                curves.append(curve)
            except Exception as e:
                # Create a basic curve and continue processing
                key = curve_data.get("key", "unknown")
                basic_curve = HourlyOutputCurve.model_construct(key=key, type="unknown")
                basic_curve.add_warning(key, f"Skipped invalid curve data: {e}")
                curves.append(basic_curve)

        collection = cls.model_validate({"curves": curves})

        # Merge warnings from individual curves
        collection._merge_submodel_warnings(*curves, key_attr="key")

        return collection

    @classmethod
    def from_service_result(
        cls, service_result: Any, scenario: Any, cache_curves: bool = True
    ) -> "HourlyOutputCurves":
        """Create HourlyOutputCurves instance from service result"""
        # Normalize to Session to get ETEngine session ID
        from pyetm.models.scenario import Scenario

        session = scenario.session if isinstance(scenario, Scenario) else scenario

        if not service_result.success or not service_result.data:
            empty_curves = cls(curves=[])
            for error in service_result.errors:
                empty_curves.add_warning("base", f"Service error: {error}")
            return empty_curves

        curves_list = []

        if cache_curves:
            cache_dir = get_settings().path_to_tmp(str(session.id))
            cache_dir.mkdir(parents=True, exist_ok=True)

        for curve_name, curve_data in service_result.data.items():
            try:
                curve = HourlyOutputCurve.model_validate(
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
                basic_curve = HourlyOutputCurve.model_construct(
                    key=curve_name, type="unknown"
                )
                basic_curve.add_warning("base", f"Failed to process curve data: {e}")
                curves_list.append(basic_curve)

        curves_collection = cls(curves=curves_list)

        for error in service_result.errors:
            curves_collection.add_warning("base", f"Download warning: {error}")

        curves_collection._merge_submodel_warnings(*curves_list, key_attr="key")

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
        return type_mapping.get(curve_name, "output_curve")

    @classmethod
    def fetch_all(
        cls, scenario: Any, cache_curves: bool = True
    ) -> "HourlyOutputCurves":
        """
        Convenience method to fetch all carrier curves for a scenario.
        """
        # Normalize to Session to get ETEngine session ID
        from pyetm.models.scenario import Scenario

        session = scenario.session if isinstance(scenario, Scenario) else scenario

        service_result = FetchAllHourlyOutputCurvesRunner.run(get_client(), session)
        return cls.from_service_result(service_result, session, cache_curves)

    def _to_dataframe(
        self, curves: Optional[list[str]] = None, **kwargs: Any
    ) -> pd.DataFrame:
        """
        Serialize HourlyOutputCurves collection to DataFrame with multi-index format.
        Returns a DataFrame with MultiIndex (hour, curve_name) containing curve values.
        """
        if not self.curves:
            return pd.DataFrame(
                index=pd.MultiIndex.from_tuples([], names=["hour", "curve_name"]),
                columns=["value"],
            )

        # Filter curves if specific ones requested
        curves_to_process = self.curves
        if curves is not None:
            curves_to_process = [c for c in self.curves if c.key in curves]

        all_curve_data = []

        for curve in curves_to_process:
            try:
                if not curve.available():
                    # Skip unavailable curves
                    continue

                curve_df = curve.contents()
                if curve_df is not None and not curve_df.empty:
                    for hour, value in curve_df.iloc[:, 0].items():
                        all_curve_data.append(
                            {"hour": hour, "curve_name": curve.key, "value": value}
                        )

            except Exception as e:
                self.add_warning(
                    "curves", f"Failed to serialize curve {curve.key}: {e}"
                )

        if all_curve_data:
            result_df = pd.DataFrame(all_curve_data)
            result_df = result_df.set_index(["hour", "curve_name"])
            return result_df
        else:
            return pd.DataFrame(
                index=pd.MultiIndex.from_tuples([], names=["hour", "curve_name"]),
                columns=["value"],
            )

    @classmethod
    def create_empty_collection(cls) -> "HourlyOutputCurves":
        """
        Create a collection with all known hourly output curve types but no data.
        This allows is_attached() to work before data is retrieved.
        """
        from pyetm.services.scenario_runners.fetch_hourly_output_curves import (
            FetchAllHourlyOutputCurvesRunner,
        )

        curves_list = []
        for curve_name in FetchAllHourlyOutputCurvesRunner.CURVE_TYPES:
            curve = HourlyOutputCurve.model_validate(
                {"key": curve_name, "type": cls._infer_curve_type(curve_name)}
            )
            curves_list.append(curve)

        return cls(curves=curves_list)
