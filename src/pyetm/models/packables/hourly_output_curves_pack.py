"""Hourly output curves packing utilities."""

import logging
from typing import ClassVar, Any, Optional, Sequence, Tuple, cast
from xlsxwriter import Workbook  # type: ignore[import-untyped]
from pyetm.models.hourly_output_curves import HourlyOutputCurves
import pandas as pd
from pandas import Series
from pyetm.models.packables.packable import Packable
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class HourlyOutputCurvesPack(Packable):
    """
    A packable for managing hourly output curves across scenarios.

    HourlyOutputCurvesPack handles the extraction and export of hourly output curves from
    scenarios.

    The class supports carrier-based organization of hourly output curves, allowing
    users to export specific carriers or all available carriers.
    """

    key: ClassVar[str] = "hourly_output_curves"
    sheet_name: ClassVar[str] = "HOURLY_OUTPUT_CURVES"

    def _build_dataframe_for_scenario(
        self,
        scenario: Any,
        columns: str = "",
        curves: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame | None:
        """
        Build a DataFrame for one scenario by delegating to the model's to_dataframe() method.

        Transforms the model's multi-index format into column-based format for concatenation.
        """
        try:
            # Fetch the requested curves from the API
            # If curves is specified, fetch only those curves
            # If curves is None, attempt to fetch all curves in the collection
            if curves is not None:
                curves_to_fetch = curves
            else:
                # Try to get all attached curves, but handle case where this isn't available
                try:
                    attached = scenario.hourly_output_curves.attached_keys()
                    # Validate it's iterable
                    curves_to_fetch = list(attached) if attached else []
                except (AttributeError, TypeError):
                    # If attached_keys() doesn't exist or fails, skip fetching
                    curves_to_fetch = []

            # Fetch each curve (will cache for later use by to_dataframe)
            if hasattr(scenario, "get_output_curve") and curves_to_fetch:
                try:
                    for curve_name in curves_to_fetch:
                        try:
                            scenario.get_output_curve(curve_name)
                        except Exception as e:
                            logger.warning(
                                "Failed to fetch curve %s for scenario %s: %s",
                                curve_name,
                                scenario.identifier(),
                                e,
                            )
                except (TypeError, AttributeError):
                    # Handle case where curves_to_fetch isn't iterable
                    pass

            # Delegate to the model's to_dataframe() method
            df = scenario.hourly_output_curves.to_dataframe(curves=curves, **kwargs)

            self.log_scenario_warnings(scenario, "hourly_output_curves", "Hourly output curves")

            if df.empty:
                return None

            # Transform from row-based multi-index to column-based format
            # The model returns either (hour, curve_name) or (hour, curve_name, column) multi-index
            # We need to unstack to get columns

            # Check the number of index levels
            if df.index.nlevels == 2:
                # Simple case: (hour, curve_name) -> unstack curve_name
                df_unstacked = df.unstack(level="curve_name")
                # Remove the 'value' level from column multi-index
                if isinstance(df_unstacked.columns, pd.MultiIndex):
                    df_unstacked.columns = df_unstacked.columns.droplevel(0)
                df_unstacked.columns.name = "curve_type"
            elif df.index.nlevels == 3:
                # hour, curve_name, column -> unstack both curve_name and column
                df_unstacked = df.unstack(level=["curve_name", "column"])
                # Remove the 'value' level
                if (
                    isinstance(df_unstacked.columns, pd.MultiIndex)
                    and df_unstacked.columns.nlevels > 2
                ):
                    df_unstacked.columns = df_unstacked.columns.droplevel(0)
                # Set the names
                if isinstance(df_unstacked.columns, pd.MultiIndex):
                    df_unstacked.columns.names = ["curve_type", None]
            else:
                # Unexpected format, return as-is
                df_unstacked = df

            return cast(pd.DataFrame, df_unstacked)

        except Exception as e:
            logger.warning(
                "Failed extracting hourly output curves for %s: %s",
                scenario.identifier(),
                e,
            )
            return None

    def to_dataframe(
        self, columns: str = "", curves: Optional[Sequence[str]] = None, **kwargs: Any
    ) -> pd.DataFrame:
        """
        Build a DataFrame with optional curve filtering.
        """
        if len(self.scenarios) == 0:
            return pd.DataFrame()

        df = self._to_dataframe(columns=columns, curves=curves, **kwargs)

        if df.empty:
            return df

        # Filter curves if specified
        if curves is not None and df.columns.nlevels >= 2:
            # Get available curve types from the MultiIndex
            available_curves = df.columns.get_level_values(1).unique()
            # Filter to only requested curves that exist
            valid_curves = [c for c in curves if c in available_curves]
            if valid_curves:
                # Select columns where level 1 matches
                mask = df.columns.get_level_values(1).isin(valid_curves)
                df = df.loc[:, mask]
            else:
                # No valid curves found, return empty
                return pd.DataFrame()

        return df

    def _to_dataframe(
        self, columns: str = "", curves: Optional[Sequence[str]] = None, **kwargs: Any
    ) -> pd.DataFrame:
        return self.build_pack_dataframe(columns=columns, curves=curves, **kwargs)

    def build_pack_dataframe(
        self, columns: str = "", curves: Optional[Sequence[str]] = None, **kwargs: Any
    ) -> pd.DataFrame:
        """
        Override base build_pack_dataframe to pass curves filter to _build_dataframe_for_scenario.
        """
        frames: list[pd.DataFrame] = []
        keys: list[Any] = []
        for scenario in self.scenarios:
            try:
                df = self._build_dataframe_for_scenario(
                    scenario, columns=columns, curves=curves, **kwargs
                )
                self.log_scenario_warnings(scenario, self.key, self.sheet_name)
            except Exception as e:
                logger.warning(
                    "Failed building frame for scenario %s in %s: %s",
                    scenario.identifier(),
                    self.__class__.__name__,
                    e,
                )
                continue
            if df is None or df.empty:
                continue
            frames.append(df)
            keys.append(self._key_for(scenario))
        return self._concat_frames(frames, keys)

    def to_dict_per_curve(
        self, curves: Optional[Sequence[str]] = None
    ) -> dict[str, dict[str, pd.DataFrame]]:
        """
        Build a dict organized by curve type, then by scenario.

        Returns dict[curve_name][scenario_id] = DataFrame for that curve and scenario.
        """
        result: dict[str, dict[str, pd.DataFrame]] = {}

        for scenario in self.scenarios:
            try:
                scenario_key = self._key_for(scenario)

                # Fetch the requested curves from the API
                # If curves is specified, fetch only those curves
                # If curves is None, attempt to fetch all curves in the collection
                if curves is not None:
                    curves_to_fetch = curves
                else:
                    # Try to get all attached curves, but handle case where this isn't available
                    try:
                        attached = scenario.hourly_output_curves.attached_keys()
                        # Validate it's iterable
                        curves_to_fetch = list(attached) if attached else []
                    except (AttributeError, TypeError):
                        # If attached_keys() doesn't exist or fails, skip fetching
                        curves_to_fetch = []

                # Fetch each curve (will cache for later use by to_dataframe)
                if hasattr(scenario, "get_output_curve") and curves_to_fetch:
                    try:
                        for curve_name in curves_to_fetch:
                            try:
                                scenario.get_output_curve(curve_name)
                            except Exception as e:
                                logger.warning(
                                    "Failed to fetch curve %s for scenario %s: %s",
                                    curve_name,
                                    scenario.identifier(),
                                    e,
                                )
                    except (TypeError, AttributeError):
                        # Handle case where curves_to_fetch isn't iterable
                        pass

                # Get multi-index dataframe from model
                df = scenario.hourly_output_curves.to_dataframe(curves=curves)

                if df.empty:
                    continue

                # Unstack the multi-index to separate by curve_name
                for curve_name in df.index.get_level_values("curve_name").unique():
                    # Extract data for this specific curve
                    curve_df_raw = df.xs(curve_name, level="curve_name")
                    # Ensure it's a DataFrame, not a Series
                    curve_df = curve_df_raw if isinstance(curve_df_raw, pd.DataFrame) else curve_df_raw.to_frame()

                    if curve_name not in result:
                        result[curve_name] = {}
                    result[curve_name][scenario_key] = curve_df

                self.log_scenario_warnings(scenario, self.key, self.sheet_name)

            except Exception as e:
                logger.warning(
                    "Failed building curves dict for scenario %s: %s",
                    scenario.identifier(),
                    e,
                )
                continue

        return result

    def to_excel_per_carrier(self, path: str, carriers: Optional[Sequence[str]] = None) -> None:
        """Export hourly output curves to Excel file organized by carrier."""

        # Determine carrier selection
        carrier_map = HourlyOutputCurves._load_carrier_mappings()
        valid_carriers = list(carrier_map.keys())
        selected = list(valid_carriers if carriers is None else carriers)
        selected = [c for c in selected if c in valid_carriers]
        if not selected:
            selected = valid_carriers

        # Nothing to do without scenarios
        if not self.scenarios:
            return

        wrote_any = False
        workbook = None
        try:
            # Sort scenarios for deterministic sheet layout
            scenarios_sorted = sorted(self.scenarios, key=lambda s: s.id)

            for carrier in selected:
                series_entries: list[Tuple[Tuple[str, str], Series[Any]]] = []

                for scenario in scenarios_sorted:
                    # Scenario label
                    try:
                        scenario_name = str(scenario.identifier())
                    except Exception:
                        scenario_name = str(getattr(scenario, "id", "scenario"))

                    # Fetch curves by carrier type using the model's method
                    curves = None
                    try:
                        curves = scenario.hourly_output_curves.get_curves_by_carrier_type(
                            scenario, carrier
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to fetch {carrier} curves for scenario '{scenario_name}': {e}"
                        )
                        curves = None
                    if not isinstance(curves, dict) or not curves:
                        continue

                    for curve_name, df in curves.items():
                        if df is None:
                            continue
                        try:
                            if isinstance(df, pd.Series):
                                s = df.copy()
                                series_entries.append(((scenario_name, curve_name), s))
                            elif isinstance(df, pd.DataFrame):
                                if df.empty:
                                    continue
                                if df.shape[1] == 1:
                                    s = df.iloc[:, 0].copy()
                                    series_entries.append(((scenario_name, curve_name), s))
                                else:
                                    for col in df.columns:
                                        s = df[col].copy()
                                        sub_curve = f"{curve_name}:{col}"
                                        series_entries.append(((scenario_name, sub_curve), s))
                        except Exception:
                            continue

                if not series_entries:
                    continue

                cols: list[Tuple[str, str]] = [key for key, _ in series_entries]
                frames = [s for _, s in series_entries]
                combined = pd.concat(frames, axis=1)
                combined.columns = pd.MultiIndex.from_tuples(cols, names=["Scenario", "Curve"])

                # Lazily create the workbook on first real data
                if workbook is None:
                    workbook = Workbook(str(path))
                excel_utils.add_frame(
                    name=carrier.upper(),
                    frame=combined,
                    workbook=workbook,
                    column_width=18,
                    scenario_styling=True,
                )
                wrote_any = True
        finally:
            if workbook is not None and wrote_any:
                workbook.close()
