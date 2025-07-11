import pandas as pd
from pydantic import BaseModel
from typing import Optional, Dict, List, Any, Set
from xlsxwriter import Workbook

from pyetm.models import Scenario
from pyetm.utils.excel import add_frame


class ScenarioPacker(BaseModel):
    """Packs one or multiple scenarios for export to dataframes or excel"""

    _scenarios: Dict[str, Set["Scenario"]] = {}

    def __init__(self, **data):
        super().__init__(**data)
        self._scenarios = {
            "custom_curves": set(),
            "inputs": set(),
            "sortables": set(),
            "carrier_curves": set(),
        }

    def add(self, *scenarios):
        """Add scenarios to all collections"""
        for collection in self._scenarios.values():
            collection.update(scenarios)

    def add_custom_curves(self, *scenarios):
        self._scenarios["custom_curves"].update(scenarios)

    def add_inputs(self, *scenarios):
        self._scenarios["inputs"].update(scenarios)

    def add_sortables(self, *scenarios):
        self._scenarios["sortables"].update(scenarios)

    def add_carrier_curves(self, *scenarios):
        self._scenarios["carrier_curves"].update(scenarios)

    def _all_scenarios(self) -> Set["Scenario"]:
        """Get all unique scenarios across collections"""
        return set().union(*self._scenarios.values())

    def main_info(self) -> pd.DataFrame:
        """Create main info DataFrame"""
        scenarios = list(self._all_scenarios())
        if not scenarios:
            return pd.DataFrame()

        scenario_data = {s.id: s.to_dataframe().iloc[:, 0] for s in scenarios}
        return pd.concat(scenario_data, axis=1)

    def inputs(self) -> pd.DataFrame:
        """Create inputs DataFrame with clean structure"""
        scenarios = list(self._scenarios["inputs"])
        if not scenarios:
            return pd.DataFrame()

        scenario_dataframes = {}
        all_units = {}

        for scenario in scenarios:
            df = scenario.inputs.to_dataframe()
            scenario_dataframes[scenario.id] = df

            if "unit" in df.columns:
                all_units.update(df["unit"].to_dict())

        all_input_keys = self._get_all_input_keys(scenario_dataframes)
        result_columns = self._build_input_columns(
            scenario_dataframes, all_units, all_input_keys
        )

        result = pd.DataFrame(result_columns)
        result.columns = pd.MultiIndex.from_tuples(
            result.columns, names=["scenario", None]
        )
        result.index.name = "input"

        return result

    def _get_all_input_keys(self, scenario_dataframes):
        all_indices = [set(df.index) for df in scenario_dataframes.values()]
        return sorted(set().union(*all_indices))

    def _build_input_columns(self, scenario_dataframes, all_units, all_input_keys):
        result_columns = {}

        if all_units:
            result_columns[("unit", "")] = pd.Series(
                {inp: all_units.get(inp, "") for inp in all_input_keys}
            )

        for scenario_id, df in scenario_dataframes.items():
            value_column = self._get_value_column(df)

            if value_column:
                result_columns[(scenario_id, "value")] = df[value_column].reindex(
                    all_input_keys
                )

            if "default" in df.columns:
                result_columns[(scenario_id, "default")] = df["default"].reindex(
                    all_input_keys
                )

        return result_columns

    def _get_value_column(self, df):
        if "value" in df.columns:
            return "value"

        non_special_columns = [c for c in df.columns if c not in ["unit", "default"]]
        return non_special_columns[0] if non_special_columns else None

    def gquery_results(self) -> pd.DataFrame:
        """Create gquery results DataFrame"""
        scenarios_with_queries = [
            s for s in self._all_scenarios() if s.queries_requested()
        ]
        if not scenarios_with_queries:
            return pd.DataFrame()

        scenario_results = {}
        units = {}

        for scenario in scenarios_with_queries:
            df = scenario.results()
            scenario_results[scenario.id] = df

            if "unit" in df.columns and not units:
                units = df["unit"].to_dict()

        all_query_keys = self._get_all_query_keys(scenario_results)
        result_data = self._build_query_results(scenario_results, units, all_query_keys)

        result = pd.DataFrame(result_data, index=all_query_keys)
        result.index.name = "gquery"

        return result

    def _get_all_query_keys(self, scenario_results):
        all_indices = [
            set(str(q) for q in df.index) for df in scenario_results.values()
        ]
        return sorted(set().union(*all_indices))

    def _build_query_results(self, scenario_results, units, all_query_keys):
        result_data = {"unit": [units.get(q, "") for q in all_query_keys]}

        for scenario_id, df in scenario_results.items():
            value_column = self._get_query_value_column(df)
            scenario_values = []

            for query in all_query_keys:
                value = self._find_query_value(df, query, value_column)
                scenario_values.append(value)

            result_data[scenario_id] = scenario_values

        return result_data

    def _get_query_value_column(self, df):
        if "future" in df.columns:
            return "future"
        if "present" in df.columns:
            return "present"

        non_unit_columns = [c for c in df.columns if c != "unit"]
        return non_unit_columns[0] if len(non_unit_columns) > 0 else None

    def _find_query_value(self, df, query, value_column):
        if not value_column:
            return ""

        for idx in df.index:
            if str(idx) == query:
                return df.loc[idx, value_column]
        return ""

    def _extract_single_scenario_data(
        self, collection_name: str, extractor_method: str
    ) -> pd.DataFrame:
        """Generic method to extract data from the first scenario in a collection"""
        scenarios = list(self._scenarios[collection_name])
        if not scenarios:
            return pd.DataFrame()

        scenario = scenarios[0]

        if collection_name == "sortables":
            return scenario.sortables.to_dataframe()

        series_list = list(getattr(scenario, extractor_method)())
        if series_list:
            return pd.concat(series_list, axis=1)

        return pd.DataFrame()

    def sortables(self) -> pd.DataFrame:
        return self._extract_single_scenario_data("sortables", None)

    def custom_curves(self) -> pd.DataFrame:
        return self._extract_single_scenario_data(
            "custom_curves", "custom_curves_series"
        )

    def carrier_curves(self) -> pd.DataFrame:
        return self._extract_single_scenario_data(
            "carrier_curves", "carrier_curves_series"
        )

    def to_excel(self, path: str):
        """Export to Excel with simplified approach"""
        if not self._all_scenarios():
            raise ValueError("Packer was empty, nothing to export")

        workbook = Workbook(path, {"nan_inf_to_errors": True})

        sheet_configs = [
            ("MAIN", self.main_info),
            ("PARAMETERS", self.inputs),
            ("GQUERIES_RESULTS", self.gquery_results),
            ("SORTABLES", self.sortables),
            ("CUSTOM_CURVES", self.custom_curves),
            ("CARRIER_CURVES", self.carrier_curves),
        ]

        for sheet_name, data_method in sheet_configs:
            df = data_method()
            if not df.empty:
                add_frame(sheet_name, df, workbook, column_width=18)

        workbook.close()

    def clear(self):
        """Clear all scenarios"""
        for collection in self._scenarios.values():
            collection.clear()

    def remove_scenario(self, scenario: "Scenario"):
        """Remove a specific scenario from all collections"""
        for collection in self._scenarios.values():
            collection.discard(scenario)

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of what's in the packer"""
        summary = {"total_scenarios": len(self._all_scenarios())}

        for name, scenarios in self._scenarios.items():
            summary[f"{name}_count"] = len(scenarios)

        summary["scenario_ids"] = sorted([s.id for s in self._all_scenarios()])

        return summary
