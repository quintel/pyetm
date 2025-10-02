import logging
from typing import ClassVar, Any, Optional, Sequence, Tuple
from xlsxwriter import Workbook
from pyetm.models.output_curves import OutputCurves
import pandas as pd
from pyetm.models.packables.packable import Packable
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class OutputCurvesPack(Packable):
    """
    A packable for managing output curves (exports) data across scenarios.

    OutputCurvesPack handles the extraction and export of output curves from
    scenarios.

    The class supports carrier-based organization of output curves, allowing
    users to export specific carriers or all available carriers.
    """

    key: ClassVar[str] = "output_curves"
    sheet_name: ClassVar[str] = "OUTPUT_CURVES"

    def _build_dataframe_for_scenario(self, scenario: Any, columns: str = "", **kwargs):
        try:
            series_list = list(scenario.all_output_curves())
            self.log_scenario_warnings(scenario, "_output_curves", "Output curves")
        except Exception as e:
            logger.warning(
                "Failed extracting output curves for %s: %s", scenario.identifier(), e
            )
            return None
        if not series_list:
            return None
        return pd.concat(series_list, axis=1)

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def to_excel_per_carrier(
        self, path: str, carriers: Optional[Sequence[str]] = None
    ) -> None:
        """Export output curves to Excel file organized by carrier."""

        # Determine carrier selection
        carrier_map = OutputCurves._load_carrier_mappings()
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
                series_entries: list[Tuple[Tuple[str, str], pd.Series]] = []

                for scenario in scenarios_sorted:
                    # Scenario label
                    try:
                        scenario_name = str(scenario.identifier())
                    except Exception:
                        scenario_name = str(getattr(scenario, "id", "scenario"))

                    # Fetch curves mapping safely
                    curves = None
                    if hasattr(scenario, "get_output_curves") and callable(
                        getattr(scenario, "get_output_curves")
                    ):
                        try:
                            curves = scenario.get_output_curves(carrier)
                        except Exception:
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
                                    series_entries.append(
                                        ((scenario_name, curve_name), s)
                                    )
                                else:
                                    for col in df.columns:
                                        s = df[col].copy()
                                        sub_curve = f"{curve_name}:{col}"
                                        series_entries.append(
                                            ((scenario_name, sub_curve), s)
                                        )
                        except Exception:
                            continue

                if not series_entries:
                    continue

                cols: list[Tuple[str, str]] = [key for key, _ in series_entries]
                frames = [s for _, s in series_entries]
                combined = pd.concat(frames, axis=1)
                combined.columns = pd.MultiIndex.from_tuples(
                    cols, names=["Scenario", "Curve"]
                )

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
