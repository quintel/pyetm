"""
Wraps a dict of queries and answers
"""

from typing import Any

import pandas as pd

from pyetm.models.base import Base
from pyetm.services.scenario_runners import GetQueryResultsRunner


class Gqueries(Base):
    """
    We cannot validate yet - as we'd need a service connected to the main
    gquery endpoint
    """

    query_dict: dict[str, Any]

    def query_keys(self) -> list[str]:
        return list(self.query_dict.keys())

    def is_ready(self) -> bool:
        return all((not v is None for v in self.query_dict.values()))

    def update(self, json: dict[str, Any]) -> None:
        """
        Updates the values with a JSON response from the API.
        """
        processed_json: dict[str, Any] = {}

        for key, value in json.items():
            processed_json[key] = value

        self.query_dict.update(processed_json)

    def get(self, key: str) -> Any:
        """
        Returns the query value if set, otherwise returns None
        """
        return self.query_dict.get(key, None)

    def add(self, *query_keys: str) -> None:
        """
        Add more queries to be requested
        """
        self.query_dict.update({q: None for q in query_keys if q not in self.query_dict.keys()})

    def execute(self, client: Any, scenario: Any) -> None:
        result = GetQueryResultsRunner.run(client, scenario, self.query_keys())

        if result.success and result.data is not None:
            self.update(result.data)
        else:
            self.add_warning("results", f"Error retrieving queries: {result.errors}")

    def _to_dataframe(self, columns: str | list[str] = "future", **kwargs: Any) -> pd.DataFrame:
        """
        Implementation required by Base class.
        Uses to_dataframe with default parameters.
        """
        if not self.is_ready():
            return pd.DataFrame()

        if isinstance(columns, str):
            columns = [columns]

        df = pd.DataFrame.from_dict(self.query_dict).reindex(["unit"] + columns).T
        df.index.name = "gquery"
        return df.set_index("unit", append=True)

    @classmethod
    def from_list(cls, query_list: list[str]) -> "Gqueries":
        return cls(query_dict={q: None for q in query_list})
