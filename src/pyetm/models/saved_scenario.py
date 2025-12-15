from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, Optional, TYPE_CHECKING
from pydantic import Field, PrivateAttr
from pyetm.models.base import Base
from pyetm.clients import BaseClient
from pyetm.services.scenario_runners.create_saved_scenario import (
    CreateSavedScenarioRunner,
)
from pyetm.services.scenario_runners.update_saved_scenario import (
    UpdateSavedScenarioRunner,
)

if TYPE_CHECKING:
    from pyetm.models.scenario import Scenario


class SavedScenarioError(Exception):
    """Base saved scenario error"""


class SavedScenario(Base):
    """
    Pydantic model for a MyETM SavedScenario.

    A SavedScenario wraps an ETEngine session scenario and persists it in MyETM.
    The response includes both SavedScenario metadata and the full nested Scenario.
    """

    id: int = Field(..., description="Unique saved scenario identifier in MyETM")
    scenario_id: int = Field(..., description="Reference to ETEngine scenario")
    title: str = Field(..., description="Title of the saved scenario")
    description: Optional[str] = None
    private: Optional[bool] = False
    area_code: Optional[str] = None
    end_year: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    scenario: Optional[Dict[str, Any]] = None

    _scenario_model: Optional[Scenario] = PrivateAttr(None)

    @classmethod
    def create(cls, client: BaseClient, params: Dict[str, Any]) -> SavedScenario:
        """
        Create a new SavedScenario in MyETM from an existing session scenario.

        Args:
            client: BaseClient instance
            params: Dictionary with required keys (scenario_id, title) and optional keys
                   (description, private)

        Returns:
            SavedScenario instance

        Raises:
            SavedScenarioError if creation fails
        """
        result = CreateSavedScenarioRunner.run(client, params)

        if not result.success:
            raise SavedScenarioError(
                f"Could not create saved scenario: {result.errors}"
            )

        saved_scenario = cls.model_validate(result.data)
        for warning in result.errors:
            saved_scenario.add_warning("base", warning)

        for field, value in params.items():
            if hasattr(saved_scenario, field) and field not in result.data:
                setattr(saved_scenario, field, value)

        return saved_scenario

    @classmethod
    def from_scenario(
        cls, client: BaseClient, scenario: Scenario, title: str, **kwargs
    ) -> SavedScenario:
        """
        Convenience method to create SavedScenario from a Scenario instance.

        Args:
            client: BaseClient instance
            scenario: Scenario instance to save
            title: Title for the saved scenario
            **kwargs: Optional params (description, private)

        Returns:
            SavedScenario instance
        """
        params = {"scenario_id": scenario.id, "title": title, **kwargs}
        return cls.create(client, params)

    def get_scenario(self, client: BaseClient) -> Scenario:
        """
        Get the associated Scenario model instance.

        Loads from cache if available, otherwise creates from nested data or fetches.

        Args:
            client: BaseClient instance

        Returns:
            Scenario instance
        """
        from pyetm.models.scenario import Scenario

        if self._scenario_model is not None:
            return self._scenario_model

        if self.scenario is not None:
            self._scenario_model = Scenario.model_validate(self.scenario)
            return self._scenario_model

        self._scenario_model = Scenario.load(self.scenario_id)
        return self._scenario_model

    def update(self, client: BaseClient, **kwargs) -> None:
        """
        Update this SavedScenario

        Args:
            client: BaseClient instance
            **kwargs: Fields to update (title, description, private, discarded)
        """
        result = UpdateSavedScenarioRunner.run(client, self.id, kwargs)

        if not result.success:
            raise SavedScenarioError(
                f"Could not update saved scenario: {result.errors}"
            )

        for warning in result.errors:
            self.add_warning("update", warning)

        if result.data:
            for field, value in result.data.items():
                if hasattr(self, field):
                    setattr(self, field, value)

        for field, value in kwargs.items():
            if hasattr(self, field) and (not result.data or field not in result.data):
                setattr(self, field, value)
