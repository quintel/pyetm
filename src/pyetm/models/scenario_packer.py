import pandas as pd
from pydantic import BaseModel
from typing import Optional, Dict, List, Any, Set, Literal, ClassVar
from xlsxwriter import Workbook

from pyetm.models.base import Base
from pyetm.models import Scenario
from pyetm.utils.excel import add_frame


class Packable(Base):
    scenarios: Optional[set["Scenario"]] = set()
    key: ClassVar[str] = 'base_pack'

    def add(self, *scenarios):
        "Adds one or more scenarios to the packable"
        self.scenarios.update(scenarios)

    def discard(self, scenario):
        "Removes a scenario from the pack"
        self.scenarios.discard(scenario)

    def clear(self):
        self.scenarios = []

    def summary(self) -> dict:
        return {self.key: {'scenario_count': len(self.scenarios)}}

    def to_dataframe(self) -> pd.DataFrame:
        """Convert the pack into a dataframe"""
        if len(self.scenarios) == 0:
            return pd.DataFrame()

        return self._to_dataframe()

    # private

    def _to_dataframe(self) -> pd.DataFrame:
        """Base: kids should implement this method"""
        return pd.DataFrame()


class InputsPack(Packable):
    key: ClassVar[str] = 'inputs'

    def _to_dataframe(self):
        data = pd.concat(
            [scenario.inputs.to_dataframe() for scenario in self.scenarios],
            axis=1,
            keys=[scenario.id for scenario in self.scenarios]
        )

        data.index.name = self.key
        return data


class QueryPack(Packable):
    key: ClassVar[str] = 'gquery'

    def _to_dataframe(self) -> pd.DataFrame:
        # its not so possible -> we should setup the df including units
        # from the first one and just merge in the rest I guess
        data = pd.concat(
            [scenario.results() for scenario in self.scenarios],
            axis=1,
            keys=[scenario.id for scenario in self.scenarios],
            copy=False
        )
        data.index.name = self.key

        # col = [col['unit'] for col in data.columns]
        # print(col)

        # # TODO: they have to be merged!
        # data.insert(
        #     0,
        #     ('unit', ''),
        #     data[(list(self.scenarios())[0].id, 'unit')])
        return data


class SortablePack(Packable):
    key: ClassVar[str] = 'sortable'

    def _to_dataframe(self) -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            return scenario.sortables.to_dataframe()


class CustomCurvesPack(Packable):
    key: ClassVar[str] = 'custom_curves'

    def _to_dataframe(self) -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            series_list = list(scenario.custom_curves_series())
            if len(series_list) == 0:
                next
            return pd.concat(series_list, axis=1)
        return pd.DataFrame()


class OutputCurvesPack(Packable):
    key: ClassVar[str] = 'output_curves'

    def _to_dataframe(self) -> pd.DataFrame:
        """PACKS ONLY FIRST SCENARIO"""
        for scenario in self.scenarios:
            series_list = list(scenario.carrier_curves_series())
            if len(series_list) == 0:
                next
            return pd.concat(series_list, axis=1)
        return pd.DataFrame()


class ScenarioPacker(BaseModel):
    """Packs one or multiple scenarios for export to dataframes or excel"""

    # To avoid keeping all in memory, the packer only remembers which scenarios
    # to pack what info for later
    _custom_curves: "CustomCurvesPack" = CustomCurvesPack()
    _inputs: "InputsPack" = InputsPack()
    _sortables: "SortablePack" = SortablePack()
    _output_curves: "OutputCurvesPack" = OutputCurvesPack()

    # Setting up a packer

    def add(self, *scenarios):
        """
        Shorthand method for adding all extractions for the scenario
        """
        self.add_custom_curves(*scenarios)
        self.add_inputs(*scenarios)
        self.add_sortables(*scenarios)
        self.add_output_curves(*scenarios)

    def add_custom_curves(self, *scenarios):
        self._custom_curves.add(*scenarios)

    def add_inputs(self, *scenarios):
        self._inputs.add(*scenarios)

    def add_sortables(self, *scenarios):
        self._sortables.add(*scenarios)

    def add_output_curves(self, *scenarios):
        self._output_curves.add(*scenarios)

    # DataFrame outputs

    def main_info(self) -> pd.DataFrame:
        """Create main info DataFrame"""
        if len(self._scenarios()) == 0:
            return pd.DataFrame()

        return pd.concat(
            [scenario.to_dataframe() for scenario in self._scenarios()],
            axis=1
            # keys=[scenario.id for scenario in self._scenarios()]
        )

    def inputs(self) -> pd.DataFrame:
        return self._inputs.to_dataframe()

    def gquery_results(self) -> pd.DataFrame:
        return QueryPack(scenarios=self._scenarios()).to_dataframe()

    def sortables(self) -> pd.DataFrame:
        return self._sortables.to_dataframe()

    def custom_curves(self) -> pd.DataFrame:
        return self._custom_curves.to_dataframe()

    def output_curves(self) -> pd.DataFrame:
        return self._output_curves.to_dataframe()

    # def inputs(self) -> pd.DataFrame:
    #     """Create inputs DataFrame with clean structure"""
    #     scenarios = list(self._scenarios["inputs"])
    #     if not scenarios:
    #         return pd.DataFrame()

    #     scenario_dataframes = {}
    #     all_units = {}

    #     for scenario in scenarios:
    #         df = scenario.inputs.to_dataframe()
    #         scenario_dataframes[scenario.id] = df

    #         if "unit" in df.columns:
    #             all_units.update(df["unit"].to_dict())

    #     all_input_keys = self._get_all_input_keys(scenario_dataframes)
    #     result_columns = self._build_input_columns(
    #         scenario_dataframes, all_units, all_input_keys
    #     )

    #     result = pd.DataFrame(result_columns)
    #     result.columns = pd.MultiIndex.from_tuples(
    #         result.columns, names=["scenario", None]
    #     )
    #     result.index.name = "input"

    #     return result

    # def _get_all_input_keys(self, scenario_dataframes):
    #     all_indices = [set(df.index) for df in scenario_dataframes.values()]
    #     return sorted(set().union(*all_indices))

    # def _build_input_columns(self, scenario_dataframes, all_units, all_input_keys):
    #     result_columns = {}

    #     if all_units:
    #         result_columns[("unit", "")] = pd.Series(
    #             {inp: all_units.get(inp, "") for inp in all_input_keys}
    #         )

    #     for scenario_id, df in scenario_dataframes.items():
    #         value_column = self._get_value_column(df)

    #         if value_column:
    #             result_columns[(scenario_id, "value")] = df[value_column].reindex(
    #                 all_input_keys
    #             )

    #         if "default" in df.columns:
    #             result_columns[(scenario_id, "default")] = df["default"].reindex(
    #                 all_input_keys
    #             )

    #     return result_columns

    # def _get_value_column(self, df):
    #     if "value" in df.columns:
    #         return "value"

    #     non_special_columns = [c for c in df.columns if c not in ["unit", "default"]]
    #     return non_special_columns[0] if non_special_columns else None

    # def gquery_results(self) -> pd.DataFrame:
    #     """Create gquery results DataFrame"""
    #     scenarios_with_queries = [
    #         s for s in self._all_scenarios() if s.queries_requested()
    #     ]
    #     if not scenarios_with_queries:
    #         return pd.DataFrame()

    #     scenario_results = {}
    #     units = {}

    #     for scenario in scenarios_with_queries:
    #         df = scenario.results()
    #         scenario_results[scenario.id] = df

    #         if "unit" in df.columns and not units:
    #             units = df["unit"].to_dict()

    #     all_query_keys = self._get_all_query_keys(scenario_results)
    #     result_data = self._build_query_results(scenario_results, units, all_query_keys)

    #     result = pd.DataFrame(result_data, index=all_query_keys)
    #     result.index.name = "gquery"

    #     return result

    # def _get_all_query_keys(self, scenario_results):
    #     all_indices = [
    #         set(str(q) for q in df.index) for df in scenario_results.values()
    #     ]
    #     return sorted(set().union(*all_indices))

    # def _build_query_results(self, scenario_results, units, all_query_keys):
    #     result_data = {"unit": [units.get(q, "") for q in all_query_keys]}

    #     for scenario_id, df in scenario_results.items():
    #         value_column = self._get_query_value_column(df)
    #         scenario_values = []

    #         for query in all_query_keys:
    #             value = self._find_query_value(df, query, value_column)
    #             scenario_values.append(value)

    #         result_data[scenario_id] = scenario_values

    #     return result_data

    # def _get_query_value_column(self, df):
    #     if "future" in df.columns:
    #         return "future"
    #     if "present" in df.columns:
    #         return "present"

    #     non_unit_columns = [c for c in df.columns if c != "unit"]
    #     return non_unit_columns[0] if len(non_unit_columns) > 0 else None

    # def _find_query_value(self, df, query, value_column):
    #     if not value_column:
    #         return ""

    #     for idx in df.index:
    #         if str(idx) == query:
    #             return df.loc[idx, value_column]
    #     return ""

    # def _extract_single_scenario_data(
    #     self, collection_name: str, extractor_method: str
    # ) -> pd.DataFrame:
    #     """Generic method to extract data from the first scenario in a collection"""
    #     scenarios = list(self._scenarios[collection_name])
    #     if not scenarios:
    #         return pd.DataFrame()

    #     scenario = scenarios[0]

    #     if collection_name == "sortables":
    #         return scenario.sortables.to_dataframe()

    #     series_list = list(getattr(scenario, extractor_method)())
    #     if series_list:
    #         return pd.concat(series_list, axis=1)

    #     return pd.DataFrame()

    def to_excel(self, path: str):
        """Export to Excel with simplified approach"""
        if len(self._scenarios()) == 0:
            raise ValueError("Packer was empty, nothing to export")

        workbook = Workbook(path, {"nan_inf_to_errors": True})

        sheet_configs = [
            ("MAIN", self.main_info),
            ("PARAMETERS", self._inputs.to_dataframe),
            ("GQUERIES_RESULTS", self.gquery_results),
            ("SORTABLES", self._sortables.to_dataframe),
            ("CUSTOM_CURVES", self._custom_curves.to_dataframe),
            ("output_CURVES", self._output_curves.to_dataframe),
        ]

        for sheet_name, data_method in sheet_configs:
            df = data_method()
            if not df.empty:
                add_frame(sheet_name, df, workbook, column_width=18)

        workbook.close()

    def _scenarios(self) -> set["Scenario"]:
        """
        All scenarios we are packing info for: for these we need to insert
        their metadata
        """
        return set.union(
            *map(set, (pack.scenarios for pack in self.all_pack_data()))
        )

    def all_pack_data(self):
        """Yields each subpack"""
        # TODO: we can also do this with model dump?
        yield self._inputs
        yield self._sortables
        yield self._custom_curves
        yield self._output_curves

    def clear(self):
        """Clear all scenarios"""
        for pack in self.all_pack_data():
            pack.clear()

    def remove_scenario(self, scenario: "Scenario"):
        """Remove a specific scenario from all collections"""
        for pack in self.all_pack_data():
           pack.discard(scenario)

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of what's in the packer"""
        summary = {"total_scenarios": len(self._scenarios())}

        for pack in self.all_pack_data():
            summary.update(pack.summary())


        summary["scenario_ids"] = sorted([s.id for s in self._scenarios()])

        return summary
