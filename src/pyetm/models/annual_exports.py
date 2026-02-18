from __future__ import annotations
import pandas as pd
from typing import Optional
from pydantic import ConfigDict

from pyetm.clients import BaseClient
from pyetm.models.base import Base
from pyetm.services.scenario_runners.fetch_annual_exports import (
    DownloadAnnualExportRunner,
)


# Available annual export types - see https://docs.energytransitionmodel.com/api/exports
ANNUAL_EXPORT_TYPES = [
    "production_parameters",
    "energy_flow",
    "energy_flow_present",
    "molecule_flow",
    "sankey",
    "storage_parameters",
    "costs_parameters",
]


class AnnualExportError(Exception):
    """Base annual export error"""


class AnnualExport(Base):
    """
    Single annual export.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    data: Optional[pd.DataFrame] = None

    def available(self) -> bool:
        """Check if export data has been retrieved"""
        return self.data is not None

    def retrieve(
        self, client: BaseClient, scenario, force_refresh: bool = False
    ) -> Optional[pd.DataFrame]:
        """Fetch export data from API"""
        # Return cached data unless explicitly refreshing
        if not force_refresh and self.available():
            return self.data

        try:
            result = DownloadAnnualExportRunner.run(client, scenario, self.name)
            if result.success and result.data:
                try:
                    result.data.seek(0)
                    df = pd.read_csv(result.data)
                    # Clean up any completely empty rows
                    df_clean = df.dropna(how="all")
                    self.data = df_clean
                    return df_clean
                except Exception as e:
                    self.add_warning(
                        "data", f"Failed to process export data for {self.name}: {e}"
                    )
                    return None
            else:
                for error in result.errors or []:
                    self.add_warning("api", f"API error: {error}")
                return None
        except Exception as e:
            self.add_warning(
                "base", f"Unexpected error retrieving export {self.name}: {e}"
            )
            return None

    def contents(self) -> Optional[pd.DataFrame]:
        """Get export contents (returns cached data if available)"""
        if not self.available():
            self.add_warning(
                "data", f"Export {self.name} not available - data not yet retrieved"
            )
            return None
        return self.data

    @classmethod
    def from_name(cls, name: str) -> "AnnualExport":
        """Create an AnnualExport instance for a given export type"""
        if name not in ANNUAL_EXPORT_TYPES:
            export = cls(name=name)
            valid_types = ", ".join(ANNUAL_EXPORT_TYPES)
            export.add_warning(
                "name",
                f"Unknown export type '{name}'. Valid types: {valid_types}",
            )
            return export
        return cls(name=name)


class AnnualExports(Base):
    """
    Collection of Annual Exports.
    """

    exports: dict[str, AnnualExport] = {}

    def __len__(self) -> int:
        return len(self.exports)

    def __iter__(self):
        """Iterate over export objects"""
        yield from self.exports.values()

    def __contains__(self, name: str) -> bool:
        """Check if export name exists in collection"""
        return name in self.exports

    @property
    def names(self) -> list[str]:
        """Get list of all export names in collection"""
        return list(self.exports.keys())

    def get(self, name: str) -> Optional[pd.DataFrame]:
        """
        Get contents of a specific export by name.
        If not yet retrieved, returns None.
        """
        if name not in self.exports:
            self.add_warning("exports", f"Export {name} not found in collection")
            return None

        export = self.exports[name]
        contents = export.contents()
        self._merge_submodel_warnings(export, key_attr="name")
        return contents

    def retrieve(
        self, client: BaseClient, scenario, name: str, force_refresh: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve a specific export by name.
        """
        if name not in self.exports:
            self.exports[name] = AnnualExport.from_name(name)

        export = self.exports[name]
        result = export.retrieve(client, scenario, force_refresh)
        self._merge_submodel_warnings(export, key_attr="name")
        return result

    def retrieve_multiple(
        self,
        client: BaseClient,
        scenario,
        names: list[str],
        force_refresh: bool = False,
    ) -> dict[str, pd.DataFrame]:
        """
        Retrieve multiple exports by name.
        """
        results = {}
        for name in names:
            data = self.retrieve(client, scenario, name, force_refresh)
            if data is not None:
                results[name] = data
        return results

    @classmethod
    def create_empty_collection(cls) -> "AnnualExports":
        exports = {name: AnnualExport.from_name(name) for name in ANNUAL_EXPORT_TYPES}
        return cls(exports=exports)
