"""User data packing utilities."""

import logging
from typing import ClassVar, Dict, Any, Optional
import pandas as pd
import numpy as np
from pyetm.models.packables.packable import Packable
from pyetm.models.scenario import Scenario

logger = logging.getLogger(__name__)

UNSAVED_SESSION_WARNING = (
    "Session '%s' is not saved to MyETM, so its users cannot be %s. "
    "Access is managed on the saved Scenario."
)


class UsersPack(Packable):
    """
    UsersPack handles the import, export, and management of scenario user roles,
    using a grid format similar to SLIDER_SETTINGS.
    """

    key: ClassVar[str] = "users"
    sheet_name: ClassVar[str] = "USERS"

    def to_dataframe(self, columns: Any = None) -> pd.DataFrame:
        """
        Export each saved Scenario's members to a DataFrame in grid format.

        Returns:
            DataFrame with user emails as index, scenarios as columns
        """
        roles_by_key = {
            self._get_scenario_display_key(s): self._roles_for(s) for s in self.scenarios
        }
        emails = list(dict.fromkeys(e for roles in roles_by_key.values() for e in roles))
        if not emails:
            return pd.DataFrame()

        frames = [
            pd.DataFrame({"role": [roles.get(e, np.nan) for e in emails]}, index=emails)
            for roles in roles_by_key.values()
        ]
        return pd.concat(frames, axis=1, keys=list(roles_by_key), names=["scenario", "field"])

    def _roles_for(self, scenario: Any) -> Dict[str, str]:
        """Map each member's email to their short role, e.g. 'owner'."""
        if not isinstance(scenario, Scenario):
            logger.warning(UNSAVED_SESSION_WARNING, scenario.identifier(), "exported")
            return {}
        try:
            users = scenario.list_users()
        except Exception as e:
            logger.warning("Failed to fetch users for scenario '%s': %s", scenario.identifier(), e)
            return {}
        return {u["user_email"]: u["role"] for u in users if u.get("user_email") and u.get("role")}

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
                if not isinstance(scenario, Scenario):
                    logger.warning(UNSAVED_SESSION_WARNING, scenario.identifier(), "imported")
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
