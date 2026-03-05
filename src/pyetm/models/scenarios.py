from __future__ import annotations
import logging
from os import PathLike
from pathlib import Path
from typing import (
    Iterable,
    Iterator,
    List,
    Union,
    TypedDict,
    Optional,
    Any,
    Sequence,
    TYPE_CHECKING,
)
from pydantic import Field, PrivateAttr
from pyetm.models.session import Session
from pyetm.models.base import Base
from pyetm.clients import BaseClient
from pyetm.clients.base_client import AsyncBatchRunner
from pyetm.types import AnnualExportType, HourlyCurveType, CarrierType
from .scenario import Scenario, SavedScenarioError
import pandas as pd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyetm.models.scenario_packer import ScenarioPacker


class ScenarioCreationParams(TypedDict, total=False):
    """
    Type definition for create_many parameter dicts.
    """

    title: Optional[str]
    scenario_id: Optional[int]
    template_id: Optional[int]
    area_code: Optional[str]
    end_year: Optional[int]
    private: Optional[bool]
    user_values: Optional[dict[str, Any]]
    custom_curves: Optional[dict[str, Any]]
    sortables: Optional[dict[str, list[Any]]]


class Scenarios(Base):
    """
    A collection of SavedScenario and/or Session objects.

    Can hold both Scenario and Session objects to support
    mixed collections loaded from Excel or other sources.
    """

    items: List[Union[Scenario, Session]] = Field(default_factory=list)
    data_warnings: List[str] = Field(default_factory=list)
    _packer: Optional["ScenarioPacker"] = PrivateAttr(default=None)

    def __iter__(self) -> Iterator[Union[Scenario, Session]]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> Union[Scenario, Session]:
        return self.items[index]

    def add(self, *scenarios: Union[Scenario, Session]) -> None:
        self.items.extend(scenarios)

    def extend(self, scenarios: Iterable[Union[Scenario, Session]]) -> None:
        self.items.extend(list(scenarios))

    @property
    def sessions(self) -> List["Session"]:
        """
        Get the underlying ETEngine Session objects from all items.
        """
        return [
            item.session if isinstance(item, Scenario) else item for item in self.items
        ]

    @property
    def combine(self) -> "ScenarioPacker":
        """
        Helps users with quick access to a packer. The combine keyword makes
        sense when spelling out method calls to scenarios.

        E.g. scenarios.combine.inputs.to_dataframe()
        or   scenarios.combine.to_excel()
        """
        from pyetm.models.scenario_packer import ScenarioPacker

        if self._packer is not None:
            return self._packer

        self._packer = ScenarioPacker()
        self._packer.add(*self.items)

        return self._packer

    def get_hourly_output_curves(
        self,
        carrier_type: CarrierType,
    ) -> dict[str, dict[str, pd.DataFrame]]:
        """
        Get hourly output curves for all scenarios by carrier type.
        """
        # Pre-fetch curves for all scenarios
        self._ensure_hourly_curves_fetched(carrier_type)

        # Now delegate to packer for organization
        return self.combine.hourly_output_curves(carrier_type)

    def _ensure_hourly_curves_fetched(self, carrier_type: CarrierType):
        """
        Ensure all scenarios have fetched their hourly output curves.
        Args:
            carrier_type: The carrier type to fetch curves for
        """
        for item in self.items:
            try:
                item.get_hourly_output_curves(carrier_type)
            except Exception as e:
                # Log warning but continue with other scenarios
                logger.warning(
                    "Failed to fetch %s curves for scenario %s: %s",
                    carrier_type,
                    item.identifier(),
                    e,
                )

    def get_annual_exports(
        self,
        exports: Optional[AnnualExportType | Sequence[AnnualExportType]] = None,
    ) -> dict[str, dict[str, pd.DataFrame]]:
        """
        Get annual exports for all scenarios, organized by export type.

        Returns:
            Dict mapping export names to dicts of {scenario_title: DataFrame}
        """
        return self.combine.annual_exports(exports)

    @classmethod
    def load_many(cls, saved_scenario_ids: Iterable[int]) -> "Scenarios":
        """
        Load multiple SavedScenario objects by their MyETM saved scenario IDs.

        Args:
            saved_scenario_ids: Iterable of MyETM saved scenario IDs to load

        Returns:
            SavedScenarios collection containing the loaded SavedScenario objects
        """
        saved_scenarios = []
        for ssid in saved_scenario_ids:
            try:
                saved_scenarios.append(Scenario.load(ssid))
            except SavedScenarioError as e:
                print(f"Could not load saved scenario {ssid}: {e}")
        return cls(items=saved_scenarios)

    @classmethod
    def create_many(
        cls,
        scenario_params: Iterable[ScenarioCreationParams],
        area_code: str | None = None,
        end_year: int | None = None,
        client: BaseClient | None = None,
        raise_on_data_errors: bool = False,
    ) -> "Scenarios":
        """
        Create multiple SavedScenario objects from parameter dicts.

        If scenario_id is not provided in params, creates a new Session first.

        Args:
            scenario_params: Iterable of ScenarioCreationParams dicts, each containing:
                - title: Title for the saved scenario (required)
                - scenario_id: (Optional) ETEngine scenario ID to save. If not provided,
                  a new Session will be created using area_code and end_year
                - area_code: (Optional if using defaults) Area code for new session
                - end_year: (Optional if using defaults) End year for new session
                - user_values: (Optional) Dict of user input values to apply after creation
                - custom_curves: (Optional) Dict of custom curves to upload after creation
                - sortables: (Optional) Dict of sortables to apply after creation
            area_code: Default area_code for all scenarios (if not in params)
            end_year: Default end_year for all scenarios (if not in params)
            client: Optional BaseClient instance (shared across all creates)
            raise_on_data_errors: If True, raise exception when data application fails.
                                  If False (default), store failures in data_warnings list.
        """
        if client is None:
            client = BaseClient()

        # Separate data parameters from creation parameters
        DATA_PARAMS = ["user_values", "custom_curves", "sortables"]
        creation_params_list = []
        data_to_apply = []  # List of (scenario_index, data_dict)

        for idx, params in enumerate(scenario_params):
            # Make a copy to avoid modifying original
            params_copy = dict(params)

            # Extract all data params declaratively
            data = {key: params_copy.pop(key, None) for key in DATA_PARAMS}

            creation_params_list.append(params_copy)

            # Only add to data_to_apply if at least one data param is present
            if any(data.values()):
                data_to_apply.append((idx, data))

        # Create scenarios sequentially
        saved_scenarios = []
        for params in creation_params_list:
            title = params.get("title")

            if title is None:
                print(
                    f"Could not create saved scenario with {params}: "
                    "Missing required 'title'"
                )
                continue

            try:
                # If scenario_id is provided, use existing session
                if "scenario_id" in params:
                    saved_scenarios.append(Scenario.create(params, client=client))
                else:
                    # Create new session first
                    area = params.get("area_code") or area_code
                    year = params.get("end_year") or end_year

                    # Check if template_id is provided (allows inheriting area_code/end_year)
                    has_template = "template_id" in params

                    if not has_template and (area is None or year is None):
                        print(
                            f"Could not create saved scenario with {params}: "
                            "Missing area_code or end_year. Provide them in each dict or as defaults."
                        )
                        continue

                    # Extract session creation params
                    session_params = {
                        k: v
                        for k, v in params.items()
                        if k
                        not in (
                            "title",
                            "private",
                            "area_code",
                            "end_year",
                        )
                    }

                    # Create the session
                    session = Session.new(area, year, **session_params)

                    # Extract SavedScenario-specific params
                    saved_scenario_params = {
                        k: v for k, v in params.items() if k == "private"
                    }

                    # Create SavedScenario from the session
                    saved_scenarios.append(
                        Scenario.from_scenario(
                            session, title, client=client, **saved_scenario_params
                        )
                    )

            except (SavedScenarioError, Exception) as e:
                print(f"Could not create saved scenario with {params}: {e}")

        # Apply data parameters concurrently after all scenarios are created
        scenarios = cls(items=saved_scenarios)
        if data_to_apply and saved_scenarios:
            failure_warnings = cls._apply_data_concurrently(
                saved_scenarios, data_to_apply, client
            )

            # Store failures as warnings or raise exception based on parameter
            if failure_warnings:
                if raise_on_data_errors:
                    error_summary = "\n".join(failure_warnings)
                    raise RuntimeError(
                        f"Failed to apply data to scenarios:\n{error_summary}"
                    )
                else:
                    scenarios.data_warnings.extend(failure_warnings)

        return scenarios

    def to_excel(self, path: PathLike | str, **export_options) -> None:
        """
        Export all scenarios to Excel.

        Note: This exports the underlying session data from each SavedScenario.
        The scenario_id column will contain Scenario IDs (MyETM).
        """
        from pyetm.utils.scenario_excel_service import ScenarioExcelService

        if not self.items:
            raise ValueError("No scenarios to export")

        resolved_path = Path(path).expanduser().resolve()
        ScenarioExcelService.export_to_excel(
            self.items, str(resolved_path), **export_options
        )

    @classmethod
    def from_excel(
        cls, xlsx_path: PathLike | str, update: bool | List[str] = False
    ) -> "Scenarios":
        """
        Import all scenarios from Excel file.

        Loads all scenarios from the Excel file, including both:
        - SavedScenarios (where 'session' column is False or missing)
        - Sessions (where 'session' column is True)

        Returns:
            Scenarios collection containing mixed Scenario and Session objects
        """
        from pyetm.models.scenario_packer import ScenarioPacker

        resolved_path = Path(xlsx_path).expanduser().resolve()

        packer = ScenarioPacker.from_excel(str(resolved_path), update=update)
        all_scenarios = list(packer._scenarios())

        if not all_scenarios:
            print(f"No scenarios found in Excel file: {resolved_path}")

        all_scenarios.sort(key=lambda s: s.id if hasattr(s, "id") else 0)

        scenarios = cls(items=all_scenarios)
        scenarios._packer = packer
        return scenarios

    @staticmethod
    def _get_session(item: Union[Scenario, Session]) -> Session:
        """
        Safely extract Session from either a Scenario or Session object.
        """
        return item.session if isinstance(item, Scenario) else item

    @staticmethod
    def _format_data_error(metadata: tuple, result) -> str:
        """
        Format error message based on request metadata and result.

        Args:
            metadata: Tuple of (req_type, scenario_idx, scenario_title[, curve_key])
            result: ServiceResult with errors

        Returns:
            Formatted error message string
        """
        req_type = metadata[0]
        scenario_title = metadata[2]
        error_msg = ", ".join(result.errors) if result.errors else "Unknown error"

        if req_type == "custom_curve" and len(metadata) > 3:
            curve_key = metadata[3]
            return f"Failed to upload curve '{curve_key}' for scenario '{scenario_title}': {error_msg}"
        else:
            return f"Failed to update {req_type} for scenario '{scenario_title}': {error_msg}"

    @staticmethod
    def _apply_data_concurrently(
        scenarios: List[Scenario], data_to_apply: List[tuple], client: BaseClient
    ) -> List[str]:
        """
        Apply user_values/curves/sortables to scenarios concurrently using runners.

        Args:
            scenarios: List of scenarios
            data_to_apply: List of (scenario_index, data_dict) tuples
            client: BaseClient instance for making requests

        Returns:
            List of warning messages for failed data applications
        """
        from pyetm.services.scenario_runners.update_inputs import UpdateInputsRunner
        from pyetm.services.scenario_runners.update_custom_curves import (
            UpdateCustomCurvesRunner,
        )
        from pyetm.services.scenario_runners.update_sortables import (
            UpdateSortablesRunner,
        )

        requests = []
        request_metadata = []  # Track what each request is for
        warnings = []  # Collect warning messages

        for scenario_idx, data in data_to_apply:
            if scenario_idx >= len(scenarios):
                continue  # Skip if scenario creation failed

            scenario = scenarios[scenario_idx]
            session = Scenarios._get_session(scenario)

            # Use UpdateInputsRunner to build user_values requests
            if data.get("user_values"):
                try:
                    request = UpdateInputsRunner.build_request(session, data["user_values"])
                    requests.append(request)
                    request_metadata.append(("user_values", scenario_idx, scenario.title))
                except Exception as e:
                    warnings.append(
                        f"Failed to build user_values request for scenario '{scenario.title}': {e}"
                    )

            # Use UpdateCustomCurvesRunner to build curve requests
            if data.get("custom_curves"):
                try:
                    curve_requests = UpdateCustomCurvesRunner.build_requests(
                        session, data["custom_curves"]
                    )
                    requests.extend(curve_requests)
                    for curve_key in data["custom_curves"].keys():
                        request_metadata.append(
                            ("custom_curve", scenario_idx, scenario.title, curve_key)
                        )
                except Exception as e:
                    warnings.append(
                        f"Failed to build curve requests for scenario '{scenario.title}': {e}"
                    )

            # Use UpdateSortablesRunner to build sortables requests
            if data.get("sortables"):
                for sortable_type, order in data["sortables"].items():
                    try:
                        request = UpdateSortablesRunner.build_request(
                            session, sortable_type, order
                        )
                        requests.append(request)
                        request_metadata.append(
                            ("sortables", scenario_idx, scenario.title)
                        )
                    except Exception as e:
                        warnings.append(
                            f"Failed to build sortables request for scenario '{scenario.title}': {e}"
                        )

        if requests:
            formatted_requests = []
            for req in requests:
                formatted = {
                    "method": req["method"],
                    "url": req["path"],
                    "kwargs": {**req.get("kwargs", {})},
                }
                # Move payload to kwargs.json if present
                if req.get("payload"):
                    formatted["kwargs"]["json"] = req["payload"]
                formatted_requests.append(formatted)

            results = AsyncBatchRunner.batch_requests_sync(
                client.session, formatted_requests
            )

            # Collect errors from failed updates
            for metadata, result in zip(request_metadata, results):
                if not result.success:
                    warnings.append(Scenarios._format_data_error(metadata, result))

        return warnings
