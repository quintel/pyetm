# Changelog

All notable changes to pyetm are documented here.

## [2.1.0] - Unreleased

### Removed

- Support for the stable `2025-01` engine. To keep using it, pin `pyetm<2.1`.
- The `production_parameters` annual export. Only `2025-01` served it; newer engines
  replaced it with the four capacity exports (`electricity_capacities`,
  `district_heating_capacities`, `hydrogen_capacities`, `network_gas_capacities`).
  Requesting it now fails validation, and `annual_exports: true` no longer asks for it.
- The 17 item limit on a sortable order. ETEngine's own `space_heating` order is longer,
  so every scenario reported an error. ETEngine still rejects items it does not know.

### Changed

- Hourly curves are requested under their current names (`electricity_profiles`,
  `district_heating_profiles`, `hydrogen_profiles`, `network_gas_profiles`). The old
  names are still accepted as input, with a deprecation warning.
- Saved scenarios are always sent under the `saved_scenario` root key.

### Fixed

- A MAIN row whose `scenario_id` cannot be loaded is now skipped with an error.
  Previously, if the row also had an `area_code` and `end_year`, pyetm silently created
  a new scenario in its place.
- Blank, `false` and `no` cells in `EXPORT_CONFIG` now switch an export off. Previously
  any non-empty value, and any blank cell, switched it on.
- A blank `hourly_curves` cell means no hourly curves, without a warning.
- Numbers written to Excel are rounded to the nearest value at the configured precision.
  Previously they were always rounded up, so `0.07` could be written as `0.0700000001`.
- A `sortables` or `custom_curves` cell in MAIN that names a missing sheet now warns.
- Two MAIN rows with the same `short_name` now warn, since other sheets find scenarios
  by `short_name`.
- MAIN in the results workbook shows a session's title, not its `short_name`.
- A copied session keeps the title given to it, without an "Ignoring invalid field" warning.

## [2.0.4] - 14-10-2026

### Fixed

- `Scenario.update(scenario_id=...)` now switches `scenario.session` to the new
  session. Previously it kept returning the old one until the scenario was reloaded.
  Pointing a scenario at a new session saves a version in MyETM, as now documented.
- `Scenario.update()` no longer fails with `AttributeError: property 'version' of
  'Scenario' object has no setter` when MyETM includes the version tag in its
  response. Only fields declared on the model are applied from the response.

## [2.0.3] - 26-08-2026

### Fixed

- Saving scenarios to MyETM works again on the stable `2025-01` version. pyetm now
  sends the request shape its target expects: `2025-01` reads the saved scenario
  attributes from the top level, newer engines from a `saved_scenario` root key.
  Affected `Scenario.new()`, `Session.save()` and any `Scenarios.from_excel()`
  run whose MAIN rows are not marked `session=TRUE`, which previously failed with
  `422: title: is missing, scenario_id: is missing`.
- A scenario that cannot be saved to MyETM no longer passes silently. The failure is
  recorded on the scenario and reported by `Scenarios.from_excel()`.
- Saving a scenario on any non-production environment (a stable tag such as `2025-01`,
  `beta`, or a custom deployment) now tells MyETM which one.

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
