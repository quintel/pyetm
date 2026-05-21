from typing import ClassVar, Set, Callable, Optional, Dict, Any, Union, TYPE_CHECKING
import logging
import pandas as pd
from pydantic import BaseModel, Field, PrivateAttr
from xlsxwriter import Workbook

from pyetm.models.session import Session
from pyetm.utils import excel_utils

logger = logging.getLogger(__name__)


class Packable(BaseModel):
    """
    Abstract base class for managing collections of scenarios.

    Packable provides the foundation for organizing, processing, and exporting
    scenario data in various formats. It defines the common interface and utilities
    used by all specific packable implementations (inputs, queries, curves, etc.).

    Subclasses should implement:
        - _build_dataframe_for_scenario: Extract data from single scenario
        - from_dataframe: Import and apply data to scenarios
        - Any specialized import/export logic
    """

    scenarios: Set[Session] = Field(default_factory=set)
    key: ClassVar[str] = "base_pack"
    sheet_name: ClassVar[str] = "SHEET"
    _scenario_id_cache: Dict[str, Session] | None = PrivateAttr(default=None)

    def add(self, *scenarios):
        """Add one or more scenarios to the packable."""
        if not scenarios:
            return
        self.scenarios.update(scenarios)
        self._scenario_id_cache = None

    def discard(self, scenario):
        "Removes a scenario from the pack"
        self.scenarios.discard(scenario)
        self._scenario_id_cache = None

    def clear(self):
        self.scenarios.clear()
        self._scenario_id_cache = None

    def summary(self) -> dict:
        return {self.key: {"scenario_count": len(self.scenarios)}}

    def _key_for(self, scenario: Session) -> str:
        """Return the identifier used as the top-level column key when packing.
        Uses short name, identifier, or ID in that priority order."""
        return self._get_scenario_display_key(scenario)

    def _scenario_short_name(self, scenario: Session) -> Optional[str]:
        """Return the short name set on a scenario, handling both Session and Scenario objects."""
        if hasattr(scenario, "session"):
            return scenario.session.short_name
        if hasattr(scenario, "short_name"):
            return scenario.short_name
        return None

    def _get_scenario_display_key(self, scenario: Session) -> str:
        """Get the display key for a scenario (short name, identifier, or ID)."""
        short_name = self._scenario_short_name(scenario)
        if short_name:
            return short_name

        try:
            identifier = scenario.identifier()
            if isinstance(identifier, (str, int)):
                return str(identifier)
        except Exception:
            pass

        return str(scenario.id)

    def _build_dataframe_for_scenario(
        self, scenario: Session, columns: str = "", **kwargs
    ) -> Optional[pd.DataFrame]:
        return None

    def _concat_frames(
        self, frames: list[pd.DataFrame], keys: list[Any]
    ) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=1, keys=keys)

    def build_pack_dataframe(self, columns: str = "", **kwargs) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        keys: list[Any] = []
        for scenario in self.scenarios:
            try:
                df = self._build_dataframe_for_scenario(
                    scenario, columns=columns, **kwargs
                )
                self.log_scenario_warnings(scenario, self.key, self.sheet_name)
            except Exception as e:
                logger.warning(
                    "Failed building frame for scenario %s in %s: %s",
                    scenario.identifier(),
                    self.__class__.__name__,
                    e,
                )
                continue
            if df is None or df.empty:
                continue
            frames.append(df)
            keys.append(self._key_for(scenario))
        return self._concat_frames(frames, keys)

    def to_dataframe(self, columns="") -> pd.DataFrame:
        """Convert the pack into a dataframe"""
        if len(self.scenarios) == 0:
            return pd.DataFrame()
        return self._to_dataframe(columns=columns)

    def from_dataframe(self, df):
        """Should parse the df and call correct setters on identified scenarios"""
        raise NotImplementedError

    def _to_dataframe(self, columns="", **kwargs) -> pd.DataFrame:
        """Base implementation - kids should implement this or use build_pack_dataframe"""
        return pd.DataFrame()

    def _refresh_cache(self):
        self._scenario_id_cache = {str(s.identifier()): s for s in self.scenarios}

    def _find_by_identifier(self, identifier: str):
        ident_str = str(identifier)
        if self._scenario_id_cache is None or len(self._scenario_id_cache) != len(
            self.scenarios
        ):
            self._refresh_cache()
        return self._scenario_id_cache.get(ident_str)

    def resolve_scenario(self, label: Any) -> Optional[Session]:
        """Resolve a scenario from various label formats (short name, identifier, or numeric ID)."""
        if label is None:
            return None

        label_str = str(label).strip()

        # Try short name first
        for scenario in self.scenarios:
            if self._scenario_short_name(scenario) == label_str:
                return scenario

        # Identifier/title
        found_scenario = self._find_by_identifier(label_str)
        if found_scenario is not None:
            return found_scenario

        # Try numeric ID as fallback
        try:
            numeric_id = int(float(label_str))
            for scenario in self.scenarios:
                if scenario.id == numeric_id:
                    return scenario
        except (ValueError, TypeError):
            pass

        return None

    def _extract_grid_data(
        self, df: pd.DataFrame, min_columns: int = 2
    ) -> Optional[pd.DataFrame]:
        """
        Extract data from grid-format sheet with header row.

        Returns:
            DataFrame with header row as columns and data rows, or None if invalid
        """
        if df is None or df.empty:
            return None

        df = df.dropna(how="all")
        if df.empty:
            return None

        header_positions = self.first_non_empty_row_positions(df, 1)
        if not header_positions:
            return None

        header_row_index = header_positions[0]
        header_row = df.iloc[header_row_index].astype(str)

        # Extract data rows
        data_df = df.iloc[header_row_index + 1 :].copy()
        data_df.columns = header_row.values

        if data_df.empty or len(data_df.columns) < min_columns:
            return None

        return data_df

    def _prepare_grid_data(
        self, data_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.Series, list]:
        """
        Prepare grid data by cleaning first column and identifying scenario columns.
        """
        import numpy as np

        first_column = data_df.columns[0]
        row_keys = data_df[first_column].astype(str).str.strip()

        # Filter out empty keys
        valid_mask = row_keys != ""
        data_df = data_df.loc[valid_mask]
        row_keys = row_keys.loc[valid_mask]
        data_df.index = row_keys

        # Process scenario columns (skip first column and filter out NaN values)
        scenario_columns = [col for col in data_df.columns[1:] if not pd.isna(col)]
        data_df[scenario_columns] = data_df[scenario_columns].replace(
            {"": np.nan, "nan": np.nan}
        )

        return data_df, row_keys, scenario_columns

    def _resolve_scenario_with_warning(
        self, column_name: str, context: str = None
    ) -> Optional[Session]:
        """
        Resolve scenario with warning logging.
        """
        scenario = self.resolve_scenario(column_name)
        if scenario is None:
            context_str = f" for {context}" if context else ""
            logger.warning(
                "Could not find scenario%s column label '%s'",
                context_str,
                column_name,
            )
        return scenario

    @staticmethod
    def is_blank(value: Any) -> bool:
        return (
            value is None
            or (isinstance(value, float) and pd.isna(value))
            or (isinstance(value, str) and value.strip() == "")
        )

    @staticmethod
    def drop_all_blank(df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()
        return df.dropna(how="all")

    @staticmethod
    def first_non_empty_row_positions(df: pd.DataFrame, count: int = 2) -> list[int]:
        positions: list[int] = []
        if df is None:
            return positions
        for idx, (_, row) in enumerate(df.iterrows()):
            if not row.isna().all():
                positions.append(idx)
                if len(positions) >= count:
                    break
        return positions

    def _log_fail(self, context: str, exc: Exception):
        logger.warning("%s failed in %s: %s", context, self.__class__.__name__, exc)

    def apply_identifier_blocks(
        self,
        df: pd.DataFrame,
        apply_block: Callable[[Session, pd.DataFrame], None],
        resolve: Optional[Callable[[Any], Optional[Session]]] = None,
    ):
        if df is None or not isinstance(df.columns, pd.MultiIndex):
            return
        identifiers = df.columns.get_level_values(0).unique()
        for identifier in identifiers:
            scenario = (
                resolve(identifier) if resolve else None
            ) or self._find_by_identifier(identifier)
            if scenario is None:
                logger.warning(
                    "Could not find scenario for identifier '%s' in %s",
                    identifier,
                    self.__class__.__name__,
                )
                continue
            block = df[identifier]
            try:
                apply_block(scenario, block)
            except Exception as e:
                logger.warning(
                    "Failed applying block for scenario '%s' in %s: %s",
                    identifier,
                    self.__class__.__name__,
                    e,
                )

    def _normalize_single_header_sheet(
        self,
        df: pd.DataFrame,
        *,
        helper_columns: Optional[set[str]] = None,
        drop_empty: bool = True,
        reset_index: bool = False,
    ) -> pd.DataFrame:
        """Normalize a sheet that uses a single header row.
        - First non-empty row becomes header.
        - Subsequent rows are data.
        - Optionally drop columns whose header is blank or in helper_columns.
        - Optionally reset the row index.
        Returns a DataFrame with a single-level column index.
        """
        helper_columns_lc = {h.lower() for h in (helper_columns or set())}
        if df is None:
            return pd.DataFrame()
        df = df.dropna(how="all")
        if df.empty:
            return df

        positions = self.first_non_empty_row_positions(df, 1)
        if not positions:
            return pd.DataFrame()
        header_pos = positions[0]
        header_row = df.iloc[header_pos].astype(str).map(lambda s: s.strip())
        data = df.iloc[header_pos + 1 :].copy()
        data.columns = header_row.values

        def _is_blank(v):
            return (
                v is None
                or (isinstance(v, float) and pd.isna(v))
                or (isinstance(v, str) and v.strip() == "")
            )

        if drop_empty or helper_columns_lc:
            keep = []
            for c in data.columns:
                if drop_empty and _is_blank(c):
                    continue
                if isinstance(c, str) and c.strip().lower() in helper_columns_lc:
                    continue
                keep.append(c)
            data = data[keep]

        if reset_index:
            data.reset_index(drop=True, inplace=True)
        return data

    def add_to_workbook(self, workbook: Workbook, sheet_name: str = None, **kwargs):
        "Add this pack's data to an Excel workbook as a sheet."
        name = sheet_name if sheet_name else self.sheet_name
        df = self.to_dataframe(**kwargs)
        if df is not None and not df.empty:
            self._add_dataframe_to_workbook(workbook, name, df)

    def _add_dataframe_to_workbook(
        self, workbook: Workbook, sheet_name: str, df: pd.DataFrame
    ):
        "Add a DataFrame to the workbook as a new sheet."
        cleaned_df = df.fillna("").infer_objects()
        excel_utils.add_frame(
            name=sheet_name,
            frame=cleaned_df,
            workbook=workbook,
            column_width=18,
            scenario_styling=True,
        )

    def import_from_excel(
        self,
        excel_file: pd.ExcelFile,
        main_df: Optional[pd.DataFrame] = None,
        scenarios_by_column: Optional[Dict[str, Session]] = None,
        update_set: set[str] = None,
    ):
        """Import pack data from Excel file.
        Subclasses should override this to implement specific import logic."""
        df = excel_utils.parse_excel_sheet(excel_file, self.sheet_name, header=None)
        if df is not None and not df.empty:
            self.from_dataframe(df, update_set)

    def log_scenario_warnings(
        self, scenario: Session, attribute_name: str, context: str
    ):
        """Log warnings from scenario attributes if available."""
        try:
            attribute = getattr(scenario, attribute_name, None)
            if attribute is not None and hasattr(attribute, "log_warnings"):
                attribute.log_warnings(
                    logger,
                    prefix=f"{context} warning for '{scenario.identifier()}'",
                )
        except Exception:
            pass

    def _should_include_upload(self, update_set: set[str] = None) -> bool:
        """
        Check if uploads should be included for this pack type.

        Args:
            update_set: Set of type names to upload (user_values, custom_curves, sortables, users)

        Returns:
            True if this pack's type should upload based on update_set
        """
        if not update_set:
            return False

        # Check if pack key is directly in update set
        if self.key in update_set:
            return True

        # Standard type-to-pack mapping for backwards compatibility
        pack_mapping = {
            "user_values": "inputs",
            "custom_curves": "custom_curves",
            "sortables": "sortables",
            "users": "users",
        }

        # Check if any of the update types map to this pack's key
        for update_type, pack_key in pack_mapping.items():
            if update_type in update_set and self.key == pack_key:
                return True

        return False
