# Changelog

All notable changes to pyetm are documented here.

## [2.0.2] - 23-07-2026

### Added

- pyetm now ships a `py.typed` marker, so type checkers (mypy, pyright) pick up
  its inline type hints when the package is imported (PEP 561).

### Changed

- Partial updates to inputs, sortables, and custom curves no longer fail wholesale
  on a single invalid entry. Invalid entries are excluded from the API payload while
  the valid ones are still applied. Excluded custom curves are reported as warnings
  instead of raising `ScenarioError`.

### Fixed

- Fixed a file-descriptor leak that could exhaust the OS open-file limit (`ulimit`)
  when many sessions were created (#205). `ETMSession` now closes its event loop on
  `close()`and can be used as a context manager (`with ETMSession(...) as session:`).

## [2.0.1] - 29-06-2026

### Changed

- Capacity tables (`electricity_capacities`, `district_heating_capacities`,
  `hydrogen_capacities`, `network_gas_capacities`) are classified as **annual exports**,
  not hourly output curves. Fetch them with `get_annual_export()` /
  `get_annual_exports()` instead of `get_hourly_curve()`.

### Breaking

- Capacity tables are only available on modern engines (`pro`). On the stable `2025-01` tag
  they do not exist; `get_annual_export()` returns `None` in that case.
