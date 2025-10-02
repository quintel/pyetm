from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Optional, Any
from pyetm.models.warnings import WarningCollector
from pyetm.clients import BaseClient
from pyetm.models.base import Base
from pydantic import PrivateAttr
from pyetm.services.scenario_runners.fetch_custom_curves import (
    DownloadCustomCurveRunner,
)
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

    # TODO: validation of length and read csv are same for this method and content method
    # extract them!
    def retrieve(self, client, scenario) -> Optional[pd.Series]:
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
            result = DownloadCustomCurveRunner.run(client, scenario, self.key)

            if result.success:
                try:
                    curve = (
                        pd.read_csv(
                            result.data, header=None, index_col=False, dtype=float
                        )
                        .squeeze("columns")
                        .dropna(how="all")
                    )
                    if len(curve) != 8760:
                        self.add_warning(
                            self.key,
                            f"Curve length should be 8760, got {len(curve)}; proceeding with current data",
                        )

                    self.file_path = file_path
                    curve.to_csv(self.file_path, index=False, header=False)
                    return curve.rename(self.key)
                except Exception as e:
                    # File processing error - add warning and return None
                    self.add_warning(self.key, f"Failed to process curve data: {e}")
                    return None
            else:
                # API call failed - add warning for each error
                for error in result.errors:
                    self.add_warning(self.key, f"Failed to retrieve curve: {error}")
                return None

        except Exception as e:
            # Unexpected error - add warning
            self.add_warning(self.key, f"Unexpected error retrieving curve: {e}")
            return None

    def contents(self) -> Optional[pd.Series]:
        """Open file from path and return contents"""
        if not self.available():
            self.add_warning(self.key, f"Curve not available - no file path set")
            return None

        try:
            series = (
                pd.read_csv(self.file_path, header=None, index_col=False, dtype=float)
                .squeeze("columns")
                .dropna(how="all")
            )
            if len(series) != 8760:
                self.add_warning(
                    self.key,
                    f"Curve length should be 8760, got {len(series)}; using available data",
                )
            return series.rename(self.key)
        except Exception as e:
            self.add_warning(self.key, f"Failed to read curve file: {e}")
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
            self.add_warning(self.key, f"Failed to remove curve file: {e}")
            return False

    @classmethod
    def from_json(cls, data: dict) -> CustomCurve:
        """
        Initialize a CustomCurve from JSON data
        """
        try:
            curve = cls(**data)
            missing = [k for k in ("key", "type") if k not in data]
            if missing:
                curve.add_warning(
                    "base",
                    f"Failed to create curve from data: missing required fields: {', '.join(missing)}",
                )

            return curve
        except Exception as e:
            basic_data = {
                "key": data.get("key", "unknown"),
                "type": data.get("type", "unknown"),
            }
            curve = cls.model_construct(**basic_data)
            curve.add_warning("base", f"Failed to create curve from data: {e}")
            return curve

    def _to_dataframe(self, **kwargs) -> pd.DataFrame:
        """
        Serialize CustomCurve to DataFrame with time series data.
        """
        curve_data = self.contents()

        if curve_data is None or curve_data.empty:
            # Return empty DataFrame with proper structure
            return pd.DataFrame({self.key: pd.Series(dtype=float)})
        df = pd.DataFrame({self.key: curve_data.values})
        df.index.name = "hour"
        return df

    @classmethod
    def _from_dataframe(
        cls, df: pd.DataFrame, scenario_id: str | int | None = None, **kwargs
    ) -> "CustomCurve":
        """
        Create CustomCurve from DataFrame containing time series data.
        """
        if len(df.columns) != 1:
            raise ValueError(
                f"DataFrame must contain exactly 1 column, got {len(df.columns)}"
            )

        curve_key = df.columns[0]
        curve_data_dict = {
            "key": curve_key,
            "type": "custom",
        }
        curve = cls.model_validate(curve_data_dict)

        if not df.empty:
            curve_data = df.iloc[:, 0].dropna()
            if not curve_data.empty:
                safe_key = str(curve_key).replace("/", "-")
                file_path = (
                    get_settings().path_to_tmp(str(scenario_id)) / f"{safe_key}.csv"
                )
                file_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    curve_data.to_csv(file_path, index=False, header=False)
                    curve.file_path = file_path
                except Exception as e:
                    curve.add_warning(
                        curve_key, f"Failed to save curve data to file: {e}"
                    )
        return curve


class CustomCurves(Base):
    """
    Wrapper around a collection of custom curves.
    """

    curves: list[CustomCurve]
    _scenario: Any = PrivateAttr(default=None)

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

    def get_contents(
        self, scenario, curve_name: str
    ) -> Optional[pd.DataFrame | pd.Series]:
        curve = self._find(curve_name)

        if not curve:
            self.add_warning("curves", f"Curve {curve_name} not found in collection")
            return None

        if not curve.available():
            # Try to retrieve it
            result = curve.retrieve(BaseClient(), scenario)
            self._merge_submodel_warnings(curve, key_attr="key")
            return result
        else:
            contents = curve.contents()
            self._merge_submodel_warnings(curve, key_attr="key")
            return contents

    def _find(self, curve_name: str) -> Optional[CustomCurve]:
        return next((c for c in self.curves if c.key == curve_name), None)

    @classmethod
    def from_json(cls, data: list[dict]) -> CustomCurves:
        """
        Initialize CustomCurves collection from JSON data
        """
        curves = []

        for curve_data in data:
            try:
                curve = CustomCurve.from_json(curve_data)
                curves.append(curve)
            except Exception as e:
                # Create a basic curve and continue processing
                key = curve_data.get("key", "unknown")
                basic_curve = CustomCurve.model_construct(key=key, type="unknown")
                basic_curve.add_warning(key, f"Skipped invalid curve data: {e}")
                curves.append(basic_curve)

        collection = cls.model_validate({"curves": curves})
        collection._merge_submodel_warnings(*curves, key_attr="key")
        return collection

    def _to_dataframe(self, **kwargs) -> pd.DataFrame:
        """
        Serialize CustomCurves collection to DataFrame with time series data.
        """
        if not self.curves:
            return pd.DataFrame(index=pd.Index([], name="hour"))

        curve_columns = {}

        for curve in self.curves:
            try:
                if (
                    not curve.available()
                    and getattr(self, "_scenario", None) is not None
                ):
                    try:
                        curve.retrieve(BaseClient(), self._scenario)
                    except Exception:
                        pass
                curve_df = curve._to_dataframe(**kwargs)
                if not curve_df.empty:
                    # Get the curve data as a Series
                    curve_series = curve_df.iloc[:, 0]  # First (and only) column
                    curve_columns[curve.key] = curve_series
                else:
                    # TODO: Should we add empty series for curves with no data? currently yes
                    curve_columns[curve.key] = pd.Series(dtype=float, name=curve.key)

            except Exception as e:
                curve_columns[curve.key] = pd.Series(dtype=float, name=curve.key)
                self.add_warning(
                    "curves", f"Failed to serialize curve {curve.key}: {e}"
                )

        if curve_columns:
            # Combine all curves into a single DataFrame
            result_df = pd.DataFrame(curve_columns)
            result_df.index.name = "hour"
            return result_df
        else:
            return pd.DataFrame(index=pd.Index([], name="hour"))

    @classmethod
    def _from_dataframe(
        cls, df: pd.DataFrame, scenario_id: str | int | None = None, **kwargs
    ) -> "CustomCurves":
        """
        Create CustomCurves collection from DataFrame with time series data.
        """
        curves = []
        if len(df.columns) == 0:
            return cls.model_validate({"curves": curves})
        for column_name in df.columns:
            try:
                curve_df = df[[column_name]]
                curve = CustomCurve._from_dataframe(
                    curve_df, scenario_id=scenario_id, **kwargs
                )
                curves.append(curve)
            except Exception as e:
                basic_curve = CustomCurve.model_construct(
                    key=column_name, type="custom"
                )
                basic_curve.add_warning(
                    "base", f"Failed to create curve from column {column_name}: {e}"
                )
                curves.append(basic_curve)
        collection = cls.model_validate({"curves": curves})
        collection._merge_submodel_warnings(*curves, key_attr="key")
        return collection

    # TODO: curves should validate themselves when created from dataframe
    def validate_for_upload(self) -> dict[str, WarningCollector]:
        """
        Validate all curves for upload
        """
        validation_errors = {}

        for curve in self.curves:
            curve_warnings = WarningCollector()

            if not curve.available():
                curve_warnings.add(curve.key, "Curve has no data available")
                validation_errors[curve.key] = curve_warnings
                continue

            try:
                try:
                    # Read without dtype conversion to preserve non-numeric values
                    raw_data = pd.read_csv(
                        curve.file_path, header=None, index_col=False
                    )
                    if raw_data.empty:
                        curve_warnings.add(curve.key, "Curve contains no data")
                        validation_errors[curve.key] = curve_warnings
                        continue

                    # Check length first
                    if len(raw_data) != 8760:
                        curve_warnings.add(
                            curve.key,
                            f"Curve must contain exactly 8,760 values, found {len(raw_data)}",
                        )
                    else:
                        try:
                            # Try to convert to numeric, this will raise if there are non-numeric values
                            pd.to_numeric(raw_data.iloc[:, 0], errors="raise")
                        except (ValueError, TypeError):
                            curve_warnings.add(
                                curve.key, "Curve contains non-numeric values"
                            )

                except pd.errors.EmptyDataError:
                    curve_warnings.add(curve.key, "Curve contains no data")
                except Exception as e:
                    curve_warnings.add(curve.key, f"Error reading curve data: {str(e)}")

            except Exception as e:
                curve_warnings.add(curve.key, f"Error reading curve data: {str(e)}")

            if len(curve_warnings) > 0:
                validation_errors[curve.key] = curve_warnings

        return validation_errors
