"""parameters object"""
from __future__ import annotations
import functools

from typing import overload, Literal, Any

import numpy as np
import pandas as pd

from pyetm.logger import get_modulelogger

from .session import SessionMethods

logger = get_modulelogger(__name__)


class ParameterMethods(SessionMethods):
    """collector class for parameter objects"""

    ## Inputs ##

    @property
    def input_parameters(self) -> pd.Series[Any]:
        """scenario input parameters"""
        return self.get_input_parameters(False, False, False)

    @input_parameters.setter
    def input_parameters(
        self, inputs: dict[str, str | float] | pd.Series[Any] | None
    ) -> None:
        self.set_input_parameters(inputs)

    @functools.lru_cache(maxsize=1)
    def _get_input_parameters(self) -> pd.DataFrame:
        """cached configuration"""

        # make request
        url = self.make_endpoint_url(endpoint="inputs")
        records = self.session.get(url, content_type="application/json")

        # convert records to frame
        parameters = pd.DataFrame.from_records(records).T
        parameters = parameters.drop(columns="cache_error", errors="ignore")

        # infer dtypes
        parameters = parameters.infer_objects()

        # add user to column when absent
        if "user" not in parameters.columns:
            parameters.insert(loc=5, column="user", value=np.nan)

        return parameters

    @overload
    def get_input_parameters(
        self,
        user_only: bool = False,
        include_disabled: bool = False,
        detailed: Literal[False] = False,
        share_group: str | None = None,
    ) -> pd.Series[str | float]:
        pass

    @overload
    def get_input_parameters(
        self,
        user_only: bool = False,
        include_disabled: bool = False,
        detailed: Literal[True] = True,
        share_group: str | None = None,
    ) -> pd.DataFrame:
        pass

    def get_input_parameters(
        self,
        user_only: bool = False,
        include_disabled: bool = False,
        detailed: bool = False,
        share_group: str | None = None,
    ) -> pd.Series[str | float] | pd.DataFrame:
        """Get the scenario input parameters from the ETM server.

        Parameters
        ----------
        user_only: boolean, default False
            Exclude parameters not set by the user in the returned results.
        include_disabled: boolean, default False
            Include disabled parameters in returned results.
        detailed: boolean, default False
            Include additional information for each parameter in the
            returned result, e.g. the parameter bounds.
        share_group: optional string
            Only return results for the specified share group.

        Return
        ------
        parameters: Series or DataFrame
            The scenario's input parameters. Returns a series by default
            and returns a DataFrame when detailed is set to True."""

        # exclude parameters without unit (seem to be irrelivant and disabled)
        parameters = self._get_input_parameters()
        parameters = parameters.loc[~parameters["unit"].isna()]

        # drop disabled
        if not include_disabled:
            parameters = parameters.loc[~parameters["disabled"]]

        # drop non-user configured parameters
        if user_only:
            user = ~parameters["user"].isna()
            parameters = parameters.loc[user]

        # subset share group
        if share_group is not None:
            # check share group
            if share_group not in parameters["share_group"].unique():
                raise ValueError(f"share group does not exist: {share_group}")

            # subset share group
            parameters = parameters[parameters["share_group"] == share_group]

        # show all details
        if detailed:
            return parameters

        # set missing defaults
        parameters.loc[:, "user"] = parameters.loc[:, "user"].fillna(
            parameters["default"]
        )

        # subset user set inputs
        user = parameters["user"]
        user.name = "inputs"

        return user

    def set_input_parameters(
        self, inputs: dict[str, str | float] | pd.Series[Any] | pd.DataFrame | None
    ) -> None:
        """set scenario input parameters,
        resets all other user specified parameters"""

        # convert None to dict
        if inputs is None:
            inputs = {}

        # subset series from df
        if isinstance(inputs, pd.DataFrame):
            inputs = inputs["user"]

        # drop nans
        inputs = pd.Series(inputs, name="user").dropna()

        # reset user configured parameters
        _inputs = self.get_input_parameters(user_only=True)
        _inputs = pd.Series("reset", index=_inputs.index, name="user")

        # combine series and upload values
        inputs.combine_first(_inputs)
        self.upload_input_parameters(inputs)

    def upload_input_parameters(
        self, inputs: dict[str, str | float] | pd.Series[Any] | pd.DataFrame | None
    ) -> None:
        """upload scenario input parameters,
        appends parameters to already uploaded parameters"""

        # convert None to dict
        if inputs is None:
            inputs = {}

        # subset series from df
        if isinstance(inputs, pd.DataFrame):
            inputs = inputs["user"]

        # drop nans
        inputs = pd.Series(inputs, name="user").dropna()

        # prepare request
        headers = {"content-type": "application/json"}
        data = {"scenario": {"user_values": dict(inputs)}, "detailed": True}

        # make request
        url = self.make_endpoint_url(endpoint="scenario_id")
        self.session.put(url, json=data, headers=headers)

        # reset cached parameters
        self._reset_cache()

    ## Orders ##

    @property
    def heat_network_order(self) -> list[str]:
        """heat network order"""

        # make url
        extra = "heat_network_order"
        url = self.make_endpoint_url(endpoint="scenario_id", extra=extra)

        # make request
        order = self.session.get(url, content_type="application/json")

        return order["order"]

    @heat_network_order.setter
    def heat_network_order(self, order: list[str]):
        # check items in order
        for item in order:
            if item not in self.heat_network_order:
                raise ValueError(f"Invalid heat network order item: '{item}'")

        # request parameters
        data = {"order": order}
        headers = {"content-type": "application/json"}

        # make url
        extra = "heat_network_order"
        url = self.make_endpoint_url(endpoint="scenario_id", extra=extra)

        # make request
        self.session.put(url, json=data, headers=headers)

        # reset cached items
        self._reset_cache()

    @property
    def forecast_storage_order(self) -> list[str]:
        """forecast storage order"""

        # make url
        extra = "forecast_storage_order"
        url = self.make_endpoint_url(endpoint="scenario_id", extra=extra)

        # make request
        order = self.session.get(url, content_type="application/json")

        return order["order"]

    @forecast_storage_order.setter
    def forecast_storage_order(self, order: list[str]) -> None:
        # check items in order
        for item in order:
            if item not in self.forecast_storage_order:
                raise ValueError(f"Invalid forecast storage order item: '{item}'")

        # request parameters
        data = {"order": order}
        headers = {"content-type": "application/json"}

        # make url
        extra = "forecast_storage_order"
        url = self.make_endpoint_url(endpoint="scenario_id", extra=extra)

        # make request
        self.session.put(url, json=data, headers=headers)

        # reset cached items
        self._reset_cache()

    ## MISC ##

    @functools.lru_cache(maxsize=1)
    def get_application_demands(self) -> pd.DataFrame:
        """get the application demands"""

        # make url
        extra = "application_demands"
        url = self.make_endpoint_url(endpoint="scenario_id", extra=extra)

        # make request and convert to frame
        buffer = self.session.get(url, content_type="text/csv")
        demands = pd.read_csv(buffer, index_col="key")

        return demands

    @functools.lru_cache(maxsize=1)
    def get_storage_parameters(self) -> pd.DataFrame:
        """get the storage parameter data"""

        # make request
        extra = "storage_parameters"
        url = self.make_endpoint_url(endpoint="scenario_id", extra=extra)

        # make request
        buffer = self.session.get(url, content_type="text/csv")

        # convert to frame
        cols = ["group", "carrier", "key", "parameter"]
        parameters = pd.read_csv(buffer, index_col=cols)

        return parameters

    @functools.lru_cache(maxsize=1)
    def get_production_parameters(self) -> pd.DataFrame:
        """get the production parameters"""

        # make url
        extra = "production_parameters"
        url = self.make_endpoint_url(endpoint="scenario_id", extra=extra)

        # make request and convert to frame
        buffer = self.session.get(url, content_type="text/csv")
        parameters = pd.read_csv(buffer)

        return parameters

    @functools.lru_cache(maxsize=1)
    def get_energy_flows(self) -> pd.DataFrame:
        """get the energy flows"""

        # make request
        url = self.make_endpoint_url(endpoint="scenario_id", extra="energy_flow")
        buffer = self.session.get(url, content_type="text/csv")

        return pd.read_csv(buffer, index_col="key")

    @functools.lru_cache(maxsize=1)
    def get_sankey(self) -> pd.DataFrame:
        """get the sankey data"""

        # make request
        url = self.make_endpoint_url(endpoint="scenario_id", extra="sankey")
        buffer = self.session.get(url, content_type="text/csv")

        # convert to frame
        cols = ["group", "carrier", "category", "type"]
        sankey = pd.read_csv(buffer, index_col=cols)

        return sankey
