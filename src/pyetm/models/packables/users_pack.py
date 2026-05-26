"""User data packing utilities."""

import logging
from typing import ClassVar, Dict, Any, List, Optional
from openpyxl import Workbook  # type: ignore[import-untyped]
import pandas as pd
import numpy as np
from pydantic import Field
from pyetm.models.packables.packable import Packable
from pyetm.services.scenario_runners.scenario_users_index import (
    ScenarioUsersIndexRunner,
)

logger = logging.getLogger(__name__)


class UsersPack(Packable):
    """
    UsersPack handles the import, export, and management of scenario user roles,
    using a grid format similar to SLIDER_SETTINGS.
    """

    key: ClassVar[str] = "users"
    sheet_name: ClassVar[str] = "USERS"

    def to_dataframe(self, columns: Any = None) -> pd.DataFrame:
        """
        Export user roles to a DataFrame in grid format.

        Returns:
            DataFrame with user emails as index, scenarios as columns
        """
        if not self.scenarios:
            return pd.DataFrame()

        # Collect all users across all scenarios
        user_roles: Dict[str, Dict[Any, str]] = {}  # email -> {scenario_key: role}

        for scenario in self.scenarios:
            scenario_key = self._get_scenario_display_key(scenario)

            try:
                result = ScenarioUsersIndexRunner.run(
                    client=scenario.client, scenario_id=scenario.id  # type: ignore[attr-defined]
                )

                if not result.success:
                    logger.warning(
                        "Failed to fetch users for scenario '%s': %s",
                        scenario.identifier(),
                        result.errors,
                    )
                    continue

                users = result.data or []
                for user in users:
                    email = user.get("email")
                    role = user.get("role")

                    if not email or not role:
                        continue

                    # Convert API role to short name for display
                    short_role = self._api_role_to_short(role)

                    if email not in user_roles:
                        user_roles[email] = {}
                    user_roles[email][scenario_key] = short_role

            except Exception as e:
                logger.warning(
                    "Error fetching users for scenario '%s': %s",
                    scenario.identifier(),
                    e,
                )

        if not user_roles:
            return pd.DataFrame()

        # Build DataFrame with multi-level columns
        frames = []
        labels = []

        for scenario in self.scenarios:
            scenario_key = self._get_scenario_display_key(scenario)
            # Create a series with all emails and their roles for this scenario
            scenario_roles = {
                email: roles.get(scenario_key, np.nan) for email, roles in user_roles.items()
            }
            df = pd.DataFrame({"role": scenario_roles})
            frames.append(df)
            labels.append(scenario_key)

        return (
            pd.concat(frames, axis=1, keys=labels, names=["scenario", "field"])
            if frames
            else pd.DataFrame()
        )

    def _api_role_to_short(self, api_role: str) -> str:
        """Convert API role name to short display name."""
        role_map = {
            "scenario_owner": "owner",
            "scenario_collaborator": "collaborator",
            "scenario_viewer": "viewer",
        }
        return role_map.get(api_role, api_role)

    def from_dataframe(self, df: Any, update_set: Optional[set[str]] = None) -> None:
        """
        Import user roles from DataFrame and apply to scenarios.

        Args:
            df: DataFrame in grid format (emails × scenarios)
            update_set: Set of update types to apply (checks for "users")
        """
        if df is None or getattr(df, "empty", False):
            return

        # Check if we should upload to API or just load locally
        skip_upload = not self._should_include_upload(update_set)

        try:
            # Extract grid data using base class helper
            data_df = self._extract_grid_data(df)
            if data_df is None:
                return

            # Prepare grid data using base class helper
            data_df, user_emails, scenario_columns = self._prepare_grid_data(data_df)

            for column_name in scenario_columns:
                # Resolve scenario with automatic warning
                scenario = self._resolve_scenario_with_warning(column_name, "USERS sheet")
                if scenario is None:
                    continue

                # Process each user/role pair for this scenario
                for email in user_emails:
                    role_value = data_df.loc[email, column_name]

                    if pd.isna(role_value):
                        continue

                    try:
                        scenario.update_users(email, role_value, skip_upload=skip_upload)
                        if not skip_upload:
                            logger.info(
                                "Updated user '%s' with role '%s' for scenario '%s'",
                                email,
                                role_value,
                                scenario.identifier(),
                            )
                    except Exception as e:
                        logger.warning(
                            "Failed to apply role for user '%s' in scenario '%s': %s",
                            email,
                            scenario.identifier(),
                            e,
                        )

        except Exception as e:
            logger.warning("Failed to parse USERS sheet: %s", e)
