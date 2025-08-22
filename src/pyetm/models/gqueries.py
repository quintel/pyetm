"""
Wraps a dict of queries and answers
"""

import pandas as pd

from pyetm.models.base import Base
from pyetm.services.scenario_runners import GetQueryResultsRunner


class Gqueries(Base):
    """
    We cannot validate yet - as we'd need a service connected to the main
    gquery endpoint
    """

    query_dict: dict

    def query_keys(self) -> list[str]:
        return list(self.query_dict.keys())

    def is_ready(self) -> bool:
        return all((not v is None for v in self.query_dict.values()))

    def update(self, json):
        """
        Updates the values with a JSON response from the API
        """
        self.query_dict.update(json)

    def get(self, key):
        """
        Returns the query value if set, otherwise returns None
        """
        return self.query_dict.get(key, None)

    def add(self, *query_keys):
        """
        Add more queries to be requested
        """
        self.query_dict.update(
            {q: None for q in query_keys if q not in self.query_dict.keys()}
        )

    def execute(self, client, scenario):
        result = GetQueryResultsRunner.run(client, scenario, self.query_keys())

        if result.success:
            self.update(result.data)
        else:
            self.add_warning("results", f"Error retrieving queries: {result.errors}")

    def to_dataframe(self, columns="future"):
        if not self.is_ready():
            return pd.DataFrame()

        if isinstance(columns, str):
            columns = [columns]
        columns = ["unit"] + columns

        normalized = {}
        for k, v in self.query_dict.items():
            if isinstance(v, dict):
                normalized[k] = {col: v.get(col) for col in columns}
            else:
                normalized[k] = {"unit": None}
                for col in columns:
                    if col != "unit":
                        normalized[k][col] = v

        df = pd.DataFrame.from_dict(normalized, orient="index")
        df.index.name = "gquery"
        return df

    def _to_dataframe(self, **kwargs) -> pd.DataFrame:
        """
        Implementation required by Base class.
        Uses to_dataframe with default parameters.
        """
        return self.to_dataframe()

    @classmethod
    def from_list(cls, query_list: list[str]):
        return cls(query_dict={q: None for q in query_list})
