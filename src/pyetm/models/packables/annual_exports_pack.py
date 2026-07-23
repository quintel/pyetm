"""Annual exports packing utilities."""

import logging
from typing import ClassVar, Any, Optional, Sequence, List, Tuple
import pandas as pd
from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class AnnualExportsPack(Packable):
    """
    A packable for managing annual exports across scenarios.

    Each export type becomes a separate worksheet in Excel exports.
    """

    key: ClassVar[str] = "annual_exports"
    sheet_name: ClassVar[str] = "ANNUAL_EXPORTS"

    @classmethod
    def validate_export_types(
        cls, export_types: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate annual export type names.

        Args:
            export_types: List of export type names to validate

        Returns:
            Tuple of (valid_types, warnings)
            - valid_types: List of validated export type names
            - warnings: List of warning messages for invalid entries
        """
        if not export_types:
            return [], []

        # Get valid export types from metadata service
        from pyetm.services.curve_metadata_service import CurveMetadataService

        valid_export_types = set(CurveMetadataService.get_export_names())

        # Validate each entry
        valid_types = []
        warnings = []

        for export_type in export_types:
            export_type_str = str(export_type).strip()
            if not export_type_str:
                continue

            if export_type_str in valid_export_types:
                valid_types.append(export_type_str)
            else:
                # Invalid entry - create helpful warning message
                warning = (
                    f"Invalid annual export type '{export_type_str}'. "
                    f"Valid types: {', '.join(sorted(valid_export_types))}."
                )
                warnings.append(warning)

        return valid_types, warnings

    def _build_dataframe_for_scenario(
        self,
        scenario: Any,
        columns: str = "",
        exports: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> Optional[pd.DataFrame]:
        """
        Build a DataFrame for one scenario by delegating to the model's to_dataframe() method.

        Returns a DataFrame with export_name as part of the multi-index.
        If no exports specified, returns None (opt-in only).
        """
        try:
            # If not specified, don't fetch any (opt-in only)
            if exports is None:
                return None

            # Fetch the requested exports from the API
            if hasattr(scenario, "get_annual_export") and exports:
                try:
                    for export_name in exports:
                        try:
                            scenario.get_annual_export(export_name)
                        except Exception as e:
                            logger.warning(
                                "Failed to fetch export %s for scenario %s: %s",
                                export_name,
                                scenario.identifier(),
                                e,
                            )
                except (TypeError, AttributeError):
                    # Handle case where exports isn't iterable
                    pass

            # Delegate to the model's to_dataframe() method
            df = scenario.annual_exports.to_dataframe(exports=exports, **kwargs)

            self.log_scenario_warnings(scenario, "annual_exports", "Annual exports")

            return df if not df.empty else None

        except Exception as e:
            logger.warning(
                "Failed extracting annual exports for %s: %s",
                scenario.identifier(),
                e,
            )
            return None

    def to_dict_per_export(
        self, exports: Optional[Sequence[str]] = None
    ) -> dict[str, dict[str, pd.DataFrame]]:
        """
        Build a dict organized by export type, then by scenario.

        Returns:
            Dictionary with structure: dict\\[export_name\\]\\[scenario_id\\] = DataFrame,
            where export_name is the export type and scenario_id is the scenario identifier.
        """
        # First pass: collect data with temporary keys
        temp_result: dict[str, dict[str, tuple[Any, pd.DataFrame]]] = {}
        scenario_keys: dict[str, list[Any]] = {}  # Maps key -> list of scenarios

        if exports:
            logger.debug(
                "Requesting annual exports %s for %d scenarios",
                exports,
                len(self.scenarios),
            )

        fetch_stats = {"requested": 0, "succeeded": 0, "failed": 0}

        for scenario in self.scenarios:
            try:
                scenario_key = self._key_for(scenario)

                # Fetch the requested exports from the API
                if hasattr(scenario, "get_annual_export") and exports:
                    try:
                        for export_name in exports:
                            fetch_stats["requested"] += 1
                            try:
                                result_df = scenario.get_annual_export(export_name)
                                if result_df is not None and not result_df.empty:
                                    fetch_stats["succeeded"] += 1
                                    logger.debug(
                                        "Retrieved export '%s' for scenario %s: %d rows",
                                        export_name,
                                        scenario.identifier(),
                                        len(result_df),
                                    )
                                else:
                                    fetch_stats["failed"] += 1
                                    logger.info(
                                        "Export '%s' returned no data for scenario %s - this export may not be available for this scenario",
                                        export_name,
                                        scenario.identifier(),
                                    )
                            except Exception as e:
                                fetch_stats["failed"] += 1
                                logger.warning(
                                    "Failed to fetch export '%s' for scenario %s: %s. Check if the scenario has this export available.",
                                    export_name,
                                    scenario.identifier(),
                                    e,
                                )
                    except (TypeError, AttributeError):
                        # Handle case where exports isn't iterable
                        pass

                # Directly access export contents instead of using to_dataframe()
                # to_dataframe() concatenates all exports with outer join, causing NaN proliferation
                exports_to_process = (
                    exports if exports else scenario.annual_exports.exports.keys()
                )

                # Track scenarios by their keys for duplicate detection
                if scenario_key not in scenario_keys:
                    scenario_keys[scenario_key] = []
                scenario_keys[scenario_key].append(scenario)

                for export_name in exports_to_process:
                    if export_name in scenario.annual_exports.exports:
                        export_obj = scenario.annual_exports.exports[export_name]
                        export_df = export_obj.contents()

                        if export_df is not None and not export_df.empty:
                            if export_name not in temp_result:
                                temp_result[export_name] = {}
                            # Store both scenario object and dataframe for deduplication
                            temp_result[export_name][scenario_key] = (scenario, export_df)

            except Exception as e:
                logger.warning(
                    "Failed building exports for scenario %s: %s",
                    scenario.identifier(),
                    e,
                )
                continue

        # Second pass: deduplicate scenario keys and build final result
        result: dict[str, dict[str, pd.DataFrame]] = {}

        # Build mapping of old keys to new deduplicated keys
        key_mapping: dict[str, dict[Any, str]] = {}  # Maps original_key -> {scenario: deduplicated_key}

        for original_key, scenarios_list in scenario_keys.items():
            if len(scenarios_list) == 1:
                # No duplicates, use original key
                key_mapping[original_key] = {scenarios_list[0]: original_key}
            else:
                # Duplicates found, append IDs
                key_mapping[original_key] = {}
                for scen in scenarios_list:
                    deduplicated_key = f"{original_key} ({scen.id})"
                    key_mapping[original_key][scen] = deduplicated_key
                    logger.debug(
                        "Duplicate scenario key '%s' renamed to '%s' for scenario %s",
                        original_key,
                        deduplicated_key,
                        scen.identifier(),
                    )

        # Apply deduplication to temp_result
        for export_name, scenarios_data in temp_result.items():
            result[export_name] = {}
            for original_key, (scenario, df) in scenarios_data.items():
                deduplicated_key = key_mapping[original_key][scenario]
                result[export_name][deduplicated_key] = df

        # Log summary
        if exports:
            logger.info(
                "Annual exports fetch complete: %d/%d succeeded, %d failed",
                fetch_stats["succeeded"],
                fetch_stats["requested"],
                fetch_stats["failed"],
            )

            if (
                fetch_stats["failed"] == fetch_stats["requested"]
                and fetch_stats["requested"] > 0
            ):
                logger.warning(
                    "All annual export retrievals failed. Possible causes: "
                    "(1) Scenarios don't have export data available, "
                    "(2) API connection issues, "
                    "(3) Invalid export names requested"
                )

        if not result and exports:
            logger.warning(
                "No annual export data collected despite requesting %s. "
                "Check if the scenarios have this data available in the ETM.",
                exports,
            )

        return result

    def to_excel(self, path: str, exports: Optional[Sequence[str]] = None) -> None:
        """
        Export annual exports to Excel file.

        Each export type becomes a separate worksheet.
        Within each worksheet, scenarios are concatenated with a scenario identifier column.
        """
        from xlsxwriter import Workbook  # type: ignore[import-untyped]
        from pyetm.utils import excel_utils

        if not self.scenarios:
            logger.info("No scenarios to export")
            return

        # Get data organized by export type
        export_dict = self.to_dict_per_export(exports=exports)

        if not export_dict:
            logger.info("No export data available")
            return

        workbook = None
        try:
            workbook = Workbook(str(path))

            # Create a worksheet for each export type
            for export_name, scenarios_data in sorted(export_dict.items()):
                if not scenarios_data:
                    continue

                # Combine all scenarios for this export into one DataFrame
                frames = []
                for scenario_key, df in scenarios_data.items():
                    # Add scenario identifier column
                    df_copy = df.copy()
                    df_copy.insert(0, "scenario", scenario_key)
                    frames.append(df_copy)

                if frames:
                    combined = pd.concat(frames, ignore_index=True)

                    # Create worksheet with export name (truncate if too long)
                    sheet_name = export_name.upper()[:31]  # Excel limit

                    excel_utils.add_frame(
                        name=sheet_name,
                        frame=combined,
                        workbook=workbook,
                        column_width=18,
                        scenario_styling=False,
                    )

        finally:
            if workbook is not None:
                workbook.close()
