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

    def remove(self, *query_keys: str) -> None:
        """
        Remove specific query keys from the collection.

        Invalid query keys are rejected with warnings.
        Warnings are automatically displayed for non-existent keys.
        Warnings from previous removals are cleared to show only current operation issues.

        Args:
            query_keys: Query keys to remove from the collection
        """
        # Auto-clear stale warnings from previous removals
        self.warnings.clear()

        for key in query_keys:
            if key in self.query_dict:
                del self.query_dict[key]
            else:
                self.add_warning(key, f"Query '{key}' not found in collection")

        # Auto-display warnings if any exist
        if len(self.warnings) > 0:
            self.auto_show_warnings()

    def clear(self) -> None:
        """
        Remove all queries from the collection.
        Also clears any accumulated warnings.
        """
        self.query_dict.clear()
        self.warnings.clear()

    def execute(self, client: Any, scenario: Any) -> None:
        # Clear previous warnings to prevent accumulation across multiple executions
        self._clear_warnings_for_attr("results")

        result = GetQueryResultsRunner.run(client, scenario, self.query_keys())

        if result.success and result.data is not None:
            # Success - update with query results
            self.update(result.data)
        else:
            # Failure - parse which queries are invalid and retry with valid ones
            invalid_queries = self._extract_invalid_query_names(result.errors)

            # Add warnings for invalid queries
            for error in result.errors:
                # Remove status code prefix (e.g., "422: ")
                if isinstance(error, str) and ": " in error:
                    error = error.split(": ", 1)[1]
                self.add_warning("results", error)

            # Retry with only valid queries if any remain
            valid_queries = [q for q in self.query_keys() if q not in invalid_queries]
            if valid_queries:
                retry_result = GetQueryResultsRunner.run(client, scenario, valid_queries)
                if retry_result.success and retry_result.data is not None:
                    self.update(retry_result.data)

        # Auto-display warnings if any exist
        if len(self.warnings) > 0:
            self.auto_show_warnings()

    def _extract_invalid_query_names(self, errors: list[str]) -> set[str]:
        """
        Extract query names from error messages like:
        - "422: Gquery another_query does not exist"
        - "Gquery another_query does not exist"
        """
        invalid_queries = set()
        for error in errors:
            if isinstance(error, str):
                # Remove status code prefix if present
                if ": " in error:
                    error = error.split(": ", 1)[1]

                # Parse "Gquery <name> does not exist" pattern
                if "Gquery" in error and "does not exist" in error:
                    # Extract the query name between "Gquery " and " does not exist"
                    parts = error.split("Gquery ", 1)
                    if len(parts) > 1:
                        name_part = parts[1].split(" does not exist")[0].strip()
                        invalid_queries.add(name_part)

        return invalid_queries

    def _to_dataframe(self, columns: str | list[str] = "future", **kwargs: Any) -> pd.DataFrame:
        """
        Implementation required by Base class.
        Uses to_dataframe with default parameters.
        Returns partial results even if some queries failed.
        """
        # Filter out queries that have no data (None values)
        ready_queries = {k: v for k, v in self.query_dict.items() if v is not None}

        if not ready_queries:
            return pd.DataFrame()

        if isinstance(columns, str):
            columns = [columns]

        df = pd.DataFrame.from_dict(ready_queries).reindex(["unit"] + columns).T
        df.index.name = "gquery"
        return df.set_index("unit", append=True)

    @classmethod
    def from_list(cls, query_list: list[str]) -> "Gqueries":
        return cls(query_dict={q: None for q in query_list})
