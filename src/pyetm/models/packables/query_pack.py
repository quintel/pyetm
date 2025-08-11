import logging
from typing import ClassVar, Any

import pandas as pd

from pyetm.models.packables.packable import Packable

logger = logging.getLogger(__name__)


class QueryPack(Packable):
    key: ClassVar[str] = "gquery"
    sheet_name: ClassVar[str] = "GQUERIES_RESULTS"

    def _build_dataframe_for_scenario(self, scenario: Any, columns: str = "future", **kwargs):  # type: ignore[override]
        try:
            return scenario.results(columns=columns)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(
                "Failed building gquery results for %s: %s", scenario.identifier(), e
            )
            return None

    def _to_dataframe(self, columns="future", **kwargs) -> pd.DataFrame:  # type: ignore[override]
        return self.build_pack_dataframe(columns=columns, **kwargs)

    def _normalize_queries_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize a GQUERIES sheet into simple shape.
        Restores previous heuristic so we don't drop the first real gquery row when there is only one header row.
        Logic:
        - Drop fully blank rows/cols.
        - Detect if there are 2 header rows (second row contains a helper token like 'gquery', 'key', etc.).
        - If only one header row, keep remaining rows as data.
        - Return DataFrame with columns = scenario identifiers and rows listing gquery keys (strings).
        """
        if df is None:
            return pd.DataFrame()

        # Drop completely empty rows/cols
        df = df.dropna(how="all")
        if df.empty:
            return df
        df = df.dropna(axis=1, how="all")

        non_empty_rows = [
            i for i, (_, r) in enumerate(df.iterrows()) if not r.isna().all()
        ]
        if not non_empty_rows:
            return pd.DataFrame()

        helper_tokens = {"gquery", "gqueries", "key", "queries"}
        header_rows = 1
        if len(non_empty_rows) > 1:
            second = (
                df.iloc[non_empty_rows[1]].astype(str).str.strip().str.lower().tolist()
            )
            if any(val in helper_tokens for val in second):
                header_rows = 2

        header_start = non_empty_rows[0]
        header_end = header_start + header_rows - 1
        headers = df.iloc[header_start : header_end + 1].astype(str)
        data = df.iloc[header_end + 1 :].copy()

        # Assign columns
        if header_rows == 2:
            cols = pd.MultiIndex.from_arrays(
                [headers.iloc[0].values, headers.iloc[1].values]
            )
            data.columns = cols
        else:
            data.columns = pd.Index(headers.iloc[0].values)

        def _is_empty(v):
            return (
                (not isinstance(v, str))
                or (v.strip() == "")
                or (v.strip().lower() == "nan")
            )

        def _is_helper_label(v):
            return isinstance(v, str) and v.strip().lower() in helper_tokens

        if isinstance(data.columns, pd.MultiIndex):
            keep = [
                c
                for c in data.columns
                if not _is_empty(c[0]) and not _is_helper_label(c[0])
            ]
            data = data[keep]
            data.columns = [c[0] for c in data.columns]
        else:
            keep = [
                c
                for c in data.columns
                if isinstance(c, str) and not _is_empty(c) and not _is_helper_label(c)
            ]
            data = data[keep]

        for c in data.columns:
            data[c] = data[c].apply(lambda x: None if pd.isna(x) else str(x).strip())
        data = data.dropna(how="all")
        return data

    def from_dataframe(self, df: pd.DataFrame):  # type: ignore[override]
        if df is None or getattr(df, "empty", False):
            return
        try:
            df = self._normalize_queries_dataframe(df)
        except Exception as e:
            logger.warning("Failed to normalize gqueries sheet: %s", e)
            return
        if df is None or df.empty:
            return

        # Here columns are single-level; wrap into a synthetic MultiIndex so we can reuse apply_identifier_blocks
        df_multi = df.copy()
        df_multi.columns = pd.MultiIndex.from_arrays(
            [df_multi.columns, ["gquery_keys"] * len(df_multi.columns)]
        )

        def _apply(scenario, block: pd.DataFrame):
            # block is a DataFrame with single column 'gquery_keys' (after collapse) or Series
            series = block.iloc[:, 0] if isinstance(block, pd.DataFrame) else block
            values = [
                v for v in series.tolist() if isinstance(v, str) and v.strip() != ""
            ]
            seen = set()
            keys = []
            for v in values:
                if v not in seen:
                    seen.add(v)
                    keys.append(v)
            if keys:
                scenario.add_queries(keys)

        self.apply_identifier_blocks(df_multi, _apply)
