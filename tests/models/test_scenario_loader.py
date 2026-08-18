"""A saved scenario that cannot be persisted must not look like a success."""

import pytest

from pyetm.models.scenario import Scenario
from pyetm.models.scenario_loader import SavedScenarioLoader
from pyetm.models.session import Session


class StubPackerHelper:
    """Minimal stand-in for ScenarioPacker's loader callbacks."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def _load_or_create_scenario(self, *args, **kwargs) -> Session:
        return self.session

    def _apply_metadata_to_scenario(self, *args, **kwargs) -> None:
        return None


@pytest.fixture
def session() -> Session:
    return Session(id=1, area_code="nl2023", end_year=2050)


@pytest.fixture
def loader(monkeypatch, session) -> SavedScenarioLoader:
    monkeypatch.setattr(SavedScenarioLoader, "_require_authentication", lambda self: None)
    return SavedScenarioLoader(StubPackerHelper(session))


def test_failed_save_warns_and_still_returns_the_session(loader, session, monkeypatch):
    """The warning must not escalate into an exception, or the row is lost."""

    def raise_422(**kwargs):
        raise RuntimeError("422: title: is missing, scenario_id: is missing")

    monkeypatch.setattr(session, "save", raise_422)

    result = loader.create_new("nl2023", 2050, "SESSION_A", {})

    assert result is session
    messages = [str(warning) for warning in result.warnings]
    assert len(messages) == 1
    assert "SESSION_A" in messages[0]
    assert "422" in messages[0]


def test_successful_save_records_no_warning(loader, session, monkeypatch):
    saved = Scenario(id=2, scenario_id=1, title="Saved")
    monkeypatch.setattr(session, "save", lambda **kwargs: saved)

    result = loader.create_new("nl2023", 2050, "SESSION_A", {})

    assert result is saved
    assert len(session.warnings) == 0
