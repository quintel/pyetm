"""Session management utilities."""

from __future__ import annotations
from os import PathLike
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Optional, Union, cast, TYPE_CHECKING
from pydantic import Field
from pyetm.models.base import Base
from pyetm.clients import BaseClient
from .session import Session, ScenarioError

if TYPE_CHECKING:
    from pyetm.models.scenario import Scenario


class Sessions(Base):
    """
    A collection of Scenario objects
    """

    items: List[Session] = Field(default_factory=list)

    def __iter__(self) -> Iterator[Session]:  # type: ignore[override]
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> Session:
        return self.items[index]

    def add(self, *scenarios: Session) -> None:
        self.items.extend(scenarios)

    def extend(self, scenarios: Iterable[Session]) -> None:
        self.items.extend(list(scenarios))

    @classmethod
    def load_all(
        cls,
        page: Optional[int] = None,
        per_page: Optional[int] = None,
        client: Optional[BaseClient] = None,
    ) -> "Sessions":
        """Load scenarios belonging to the authenticated user.

        Fetches ETEngine sessions for the authenticated user. By default, automatically
        fetches all pages. If page parameter is provided, fetches only that specific page.

        Args:
            page: Optional page number (1-indexed). If provided, fetches only that page.
                  If None, fetches all pages automatically.
            per_page: Optional number of results per page
            client: Optional BaseClient instance for API communication

        Returns:
            Sessions collection containing user's sessions

        Raises:
            ValueError: If authentication fails or API request fails
        """
        from pyetm.services.scenario_runners.fetch_user_scenarios import (
            FetchUserScenariosRunner,
        )

        if client is None:
            from pyetm.clients import BaseClient

            client = BaseClient()

        result = FetchUserScenariosRunner.run(
            client=client,
            page=page,
            per_page=per_page,
        )

        if not result.success:
            raise ValueError(
                f"Failed to fetch user scenarios: {'; '.join(result.errors)}"
            )

        if result.data is None:
            raise ValueError("No data returned from API")

        # Use model_validate to avoid N+1 API calls
        scenarios = [Session.model_validate(data) for data in result.data]
        return cls(items=scenarios)

    @classmethod
    def load_many(
        cls, scenario_ids: Iterable[int], client: Optional[BaseClient] = None
    ) -> "Sessions":
        """Load multiple Session objects by their ETEngine session IDs.

        This is a bulk operation - individual failures are collected as warnings
        to allow partial success. Use PYETM_ERROR_MODE=safe to raise on first error.

        Args:
            scenario_ids: Iterable of ETEngine session IDs to load
            client: Optional BaseClient instance for API communication

        Returns:
            Sessions collection containing the loaded Session objects.
            Warnings from failures are displayed automatically.
        """
        sessions = cls(items=[])
        sessions.set_bulk_context(True)

        for sid in scenario_ids:
            try:
                session = Session.load(sid, client=client)
                session.set_bulk_context(True)
                sessions.items.append(session)
            except ScenarioError as e:
                sessions.add_warning(
                    "load_many",
                    f"Could not load scenario {sid}: {e}",
                    severity="error",
                )

        # Auto-display warnings if any
        if len(sessions.warnings) > 0:
            sessions.show_warnings()

        return sessions

    @classmethod
    def create_many(
        cls,
        scenario_params: Iterable[dict[str, Any]],
        area_code: str | None = None,
        end_year: int | None = None,
    ) -> "Sessions":
        """Create multiple Session objects from parameter dicts.

        This is a bulk operation - individual failures are collected as warnings
        to allow partial success. Use PYETM_ERROR_MODE=safe to raise on first error.

        Args:
            scenario_params: Iterable of parameter dicts containing area_code, end_year, etc.
            area_code: Default area_code for all sessions (if not in params)
            end_year: Default end_year for all sessions (if not in params)

        Returns:
            Sessions collection containing created Session objects.
            Warnings from failures are displayed automatically.
        """
        sessions = cls(items=[])
        sessions.set_bulk_context(True)

        created_sessions = []
        for params in scenario_params:
            area = params.get("area_code") or area_code
            year = params.get("end_year") or end_year
            if area is None or year is None:
                sessions.add_warning(
                    "create_many",
                    f"Could not create scenario with {params}: Missing area_code or end_year. Provide them in each dict or as defaults.",
                    severity="warning",
                )
                continue
            try:
                extra = {
                    k: v
                    for k, v in params.items()
                    if k not in ("area_code", "end_year")
                }
                session = Session.new(area, year, **extra)
                session.set_bulk_context(True)
                created_sessions.append(session)
            except (ScenarioError, ValueError) as e:
                sessions.add_warning(
                    "create_many",
                    f"Could not create scenario with {params}: {e}",
                    severity="error",
                )

        # Add created sessions to collection
        sessions.items = created_sessions

        # Merge warnings from individual sessions
        for session in created_sessions:
            sessions._merge_submodel_warnings(session)

        # Auto-display warnings if any
        if len(sessions.warnings) > 0 or any(len(s.warnings) > 0 for s in created_sessions):
            print("\n=== Batch Creation Summary ===")
            sessions.show_warnings()

        return sessions

    def to_excel(self, path: PathLike[str] | str, **export_options: Any) -> None:
        """Export all scenarios to Excel."""
        from pyetm.models.scenario_packer import ScenarioPacker

        if not self.items:
            raise ValueError("No scenarios to export")

        packer = ScenarioPacker()
        packer.add(*self.items)
        packer.to_excel(str(Path(path).expanduser().resolve()), **export_options)

    @classmethod
    def from_excel(cls, xlsx_path: PathLike[str] | str) -> "Sessions":
        """
        Import scenarios (Sessions) from Excel file.

        Only loads scenarios where the 'session' column is True.
        SavedScenarios (session=False or missing) are ignored.

        Args:
            xlsx_path: Path to Excel file

        Returns:
            Scenarios collection containing only Session instances
        """
        from pyetm.models.scenario_packer import ScenarioPacker
        from pyetm.models.scenario import Scenario

        resolved_path = Path(xlsx_path).expanduser().resolve()
        packer = ScenarioPacker.from_excel(str(resolved_path))
        all_scenarios = list(packer._scenarios())
        sessions = [s for s in all_scenarios if not isinstance(s, Scenario)]

        if not sessions:
            print(f"No Sessions found in Excel file: {resolved_path}")

        sessions.sort(key=lambda s: s.id if hasattr(s, "id") else 0)
        return cls(items=sessions)
