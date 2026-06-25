"""Service for fetching and caching curve and export metadata from ETEngine.

Curve and export names are no longer hard-coded in pyetm; they are discovered
from ETEngine's ``/curves/metadata`` and ``/exports/metadata`` endpoints. This
service fetches that metadata through the shared ETM client (so it honours the
same base URL, SSL, proxy and authentication settings as every other request)
and caches it in memory and on disk.

Note on hidden network access: validators, ``_infer_curve_type`` and
``create_empty_collection`` rely on this metadata, so those otherwise-pure calls
trigger a (cached) HTTP request the first time they run. When the API and disk
cache are both unavailable the service falls back to a built-in default list so
pyetm keeps working offline and against engines without the metadata endpoints.

Caching strategy:
- In-memory cache with a TTL (``metadata_cache_ttl``) so long-running processes
  pick up newly added curves without a restart.
- Disk cache keyed by base URL, so different environments (pro/beta/local) never
  share metadata; it survives restarts and brief outages.
- Built-in defaults as a last resort (may be stale; the API is authoritative).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any, Optional, cast

import aiohttp

from pyetm.config import curve_registry
from pyetm.config.settings import get_settings

logger = logging.getLogger(__name__)

# Best-effort fallback used only when the API is unreachable and no disk cache
# exists (offline, or pointed at an engine without the metadata endpoints). The
# live API is always preferred and is the source of truth. Curve defaults are
# derived from curve_registry so the canonical names have a single home.
_DEFAULT_CURVES: list[dict[str, Any]] = curve_registry.default_curve_metadata()

_DEFAULT_EXPORTS: list[dict[str, Any]] = [
    {"name": "energy_flow", "description": ""},
    {"name": "energy_flow_present", "description": ""},
    {"name": "molecule_flow", "description": ""},
    {"name": "sankey", "description": ""},
    {"name": "storage_parameters", "description": ""},
    {"name": "costs_parameters", "description": ""},
    # Capacity tables — annual by shape, served from the /exports endpoint.
    {"name": "electricity_capacities", "description": ""},
    {"name": "district_heating_capacities", "description": ""},
    {"name": "hydrogen_capacities", "description": ""},
    {"name": "network_gas_capacities", "description": ""},
]

# Per-kind configuration: API path, JSON envelope key, and offline defaults.
_KINDS: dict[str, dict[str, Any]] = {
    "curves": {
        "path": "/curves/metadata",
        "json_key": "hourly_outputs",
        "defaults": _DEFAULT_CURVES,
    },
    "exports": {
        "path": "/exports/metadata",
        "json_key": "annual_exports",
        "defaults": _DEFAULT_EXPORTS,
    },
}

# Exceptions meaning "the API is unreachable/unusable" that trigger the
# disk-cache/defaults fallback. Programming errors are intentionally not caught.
_FETCH_ERRORS = (
    PermissionError,
    ValueError,
    ConnectionError,
    aiohttp.ClientError,
    asyncio.TimeoutError,
)


class CurveMetadataService:
    """Fetches and caches curve/export metadata from ETEngine.

    All methods are class-level and the cache is shared per process. Use
    :meth:`clear_cache` to force a refresh (``reload_configuration`` does this
    automatically when settings change).
    """

    _cache: dict[str, list[dict[str, Any]]] = {}
    _fetched_at: dict[str, float] = {}
    _cache_dir: Optional[Path] = None

    # --------------------------------------------------------------- public API

    @classmethod
    def get_curve_metadata(cls) -> list[dict[str, Any]]:
        """Return metadata for all available hourly output curves."""
        return cls._get_metadata("curves")

    @classmethod
    def get_export_metadata(cls) -> list[dict[str, Any]]:
        """Return metadata for all available annual exports."""
        return cls._get_metadata("exports")

    @classmethod
    def get_curve_names(cls) -> list[str]:
        """Return the names of all available curves."""
        return [curve["name"] for curve in cls.get_curve_metadata()]

    @classmethod
    def get_export_names(cls) -> list[str]:
        """Return the names of all available exports."""
        return [export["name"] for export in cls.get_export_metadata()]

    @classmethod
    def get_curve_type(cls, curve_name: str) -> Optional[str]:
        """Return a curve's type (e.g. 'merit_curve'), or None if unknown."""
        for curve in cls.get_curve_metadata():
            if curve["name"] == curve_name:
                return curve.get("type")
        return None

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the in-memory cache (leaves the disk cache in place)."""
        cls._cache.clear()
        cls._fetched_at.clear()

    @classmethod
    def clear_disk_cache(cls) -> None:
        """Remove disk-cached metadata for the current base URL."""
        for kind in _KINDS:
            path = cls._cache_path(kind)
            try:
                path.unlink(missing_ok=True)
            except OSError as error:
                logger.warning("Failed to clear disk cache %s: %s", path, error)

    @classmethod
    def set_cache_dir(cls, cache_dir: Path) -> None:
        """Override the directory used for the disk cache (mainly for tests)."""
        cls._cache_dir = cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------- internals

    @classmethod
    def _get_metadata(cls, kind: str) -> list[dict[str, Any]]:
        cached = cls._memory_cache(kind)
        if cached is not None:
            return cached
        try:
            data = cls._fetch(kind)
        except _FETCH_ERRORS as error:
            return cls._fallback(kind, error)
        cls._store(kind, data)
        return data

    @classmethod
    def _fetch(cls, kind: str) -> list[dict[str, Any]]:
        # Imported lazily to avoid an import cycle at module load time.
        from pyetm.clients import get_client

        config = _KINDS[kind]
        response = get_client().session.get(
            config["path"], timeout=get_settings().metadata_timeout
        )
        payload = response.json()
        metadata = cast(list[dict[str, Any]], payload.get(config["json_key"], []))
        logger.info("Fetched %d %s metadata entries from API", len(metadata), kind)
        return metadata

    @classmethod
    def _fallback(cls, kind: str, error: Exception) -> list[dict[str, Any]]:
        disk = cls._load_from_disk(cls._cache_path(kind))
        if disk is not None:
            logger.warning(
                "Using disk-cached %s metadata after API failure (%s); "
                "it may be stale if new entries were added.",
                kind,
                error,
            )
            cls._set_memory(kind, disk)
            return disk

        logger.warning(
            "Using built-in default %s metadata: ETEngine is unreachable and no "
            "disk cache exists (%s). This may be incomplete or stale.",
            kind,
            error,
        )
        defaults = cast(list[dict[str, Any]], _KINDS[kind]["defaults"])
        cls._set_memory(kind, defaults)
        return defaults

    @classmethod
    def _store(cls, kind: str, data: list[dict[str, Any]]) -> None:
        cls._set_memory(kind, data)
        cls._save_to_disk(cls._cache_path(kind), data)

    @classmethod
    def _memory_cache(cls, kind: str) -> Optional[list[dict[str, Any]]]:
        data = cls._cache.get(kind)
        if data is None:
            return None
        ttl = get_settings().metadata_cache_ttl
        if ttl and (time.monotonic() - cls._fetched_at.get(kind, 0.0)) > ttl:
            return None
        return data

    @classmethod
    def _set_memory(cls, kind: str, data: list[dict[str, Any]]) -> None:
        cls._cache[kind] = data
        cls._fetched_at[kind] = time.monotonic()

    @classmethod
    def _cache_dir_path(cls) -> Path:
        if cls._cache_dir is None:
            import tempfile

            cls._cache_dir = Path(tempfile.gettempdir()) / "pyetm_cache"
        cls._cache_dir.mkdir(parents=True, exist_ok=True)
        return cls._cache_dir

    @classmethod
    def _cache_path(cls, kind: str) -> Path:
        # Key the file by base URL so environments never share metadata.
        digest = hashlib.sha256(str(get_settings().base_url).encode()).hexdigest()[:12]
        return cls._cache_dir_path() / f"{kind}_metadata_{digest}.json"

    @classmethod
    def _load_from_disk(cls, path: Path) -> Optional[list[dict[str, Any]]]:
        try:
            if not path.exists():
                return None
            with open(path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except (OSError, json.JSONDecodeError) as error:
            logger.warning("Failed to load disk cache %s: %s", path, error)
            return None
        # Tolerate both the wrapped ({"data": [...]}) and a legacy bare-list form.
        if isinstance(payload, dict):
            return cast(list[dict[str, Any]], payload.get("data", []))
        return cast(list[dict[str, Any]], payload)

    @classmethod
    def _save_to_disk(cls, path: Path, data: list[dict[str, Any]]) -> None:
        payload = {"version": 1, "fetched_at": time.time(), "data": data}
        try:
            with open(path, "w", encoding="utf-8") as file:
                json.dump(payload, file, indent=2)
        except OSError as error:
            logger.warning("Failed to save disk cache %s: %s", path, error)
