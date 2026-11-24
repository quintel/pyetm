# Changelog

All notable changes to pyetm are documented here.

## [2.1.0] - Unreleased

### Breaking

pyetm talks to the ETM through one API, at `my.energytransitionmodel.com`, and no longer
reaches an engine directly. Every scenario is a saved scenario. `Session` and `Sessions` are
gone. pyetm 2.1 targets `latest`; to keep
working on a stable Version such as `2025-01`, stay on pyetm 2.0.x (see
[Compatibility](getting-started/compatibility.md)).

#### Upgrading

**Settings**

| 2.0 | 2.1 |
|---|---|
| `ENVIRONMENT=pro`, `beta` or `local` | Unchanged names, now picking the ETM's API. `local` is the ETLauncher stack at `https://myetm.local.energytransitionmodel.com` |
| `ENVIRONMENT=2025-01` or another stable tag | Fails at startup. Install pyetm 2.0.x for a stable Version |
| `ENVIRONMENT=tyndp2024` or another custom label | Fails at startup. Use `pro`, `beta` or `local`, or set `BASE_URL` |
| `BASE_URL=https://...engine.../api/v3` | Fails at startup. Remove it, or set the ETM's API URL ending in `/api/v4` |
| (none) | `ETM_VERSION`, the Version new scenarios are created on, default `latest` |

**Session**

| 2.0 | 2.1 |
|---|---|
| `from pyetm import Session` | Raises `ImportError` naming `Scenario`, for this release |
| `Session.new(area_code, end_year)` | `Scenario.new(title, area_code, end_year)`. A title is required |
| `Session.load(session_id)` | `Scenario.load(saved_scenario_id)`. Sessions are not addressed directly any more |
| `session.save(title=...)` | Not needed: `Scenario.new()` already saves. `scenario.save()` now appends a Snapshot |
| `session.copy(**overrides)` | `scenario.copy(title=..., private=...)` |
| `session.copy_with_preset(**overrides)` | `scenario.copy_with_preset(title=..., private=...)` |
| `Session.interpolate(sessions, *years)` | `Scenario.interpolate(scenario, *years, titles=...)`, from one source. Interpolating between several scenarios is no longer supported |
| `session.delete()` | `scenario.delete()`, which deletes the saved scenario |
| `session.preset_scenario_id` | `scenario.template_id`, which now reads the engine's `template` field. It was always `None` in 2.0 |
| `session.version` (parsed from the engine URL) | `scenario.version`, the saved scenario's Version tag |
| `session.id` | `scenario.scenario_id` for reference. Calls use `scenario.id`, the saved scenario |
| `session.list_users()`, `update_users()`, `apply_pending_users()` | The same methods on `Scenario` |
| `session.from_excel(path)` | `Scenario.from_excel(path)` |
| Every data method: `inputs`, `user_values()`, `update_user_values()`, `remove_user_values()`, `set_user_values_from_dataframe()`, `sortables`, `update_sortables()`, `remove_sortables()`, `set_sortables_from_dataframe()`, `custom_curves`, `custom_curve_series()`, `custom_curves_series()`, `update_custom_curves()`, `remove_custom_curves()`, `hourly_output_curves`, `get_hourly_curve()`, `get_hourly_curves()`, `all_hourly_output_curves()`, the four `clear_*_cache()` methods, `annual_exports`, `get_annual_export()`, `get_annual_exports()`, `set_export_config()`, `get_export_config()`, `add_queries()`, `remove_queries()`, `clear_queries()`, `execute_queries()`, `results()`, `queries_requested()`, `couplings`, `update_couplings()`, `update_metadata()`, `set_short_name()`, `identifier()`, `show_all_warnings()`, `to_excel()`, `collect_export_data()` | The same methods on `Scenario` |
| The fields `start_year`, `keep_compatible`, `scaling`, `source`, `url`, `metadata`, `template_id` | Read only properties on `Scenario`, fetched from the current Session on first use |

**Sessions**

| 2.0 | 2.1 |
|---|---|
| `from pyetm import Sessions` | Raises `ImportError` naming `Scenarios`, for this release |
| `Sessions.load_all(page, per_page)` | `Scenarios.load_all()`, every saved scenario you can access |
| `Sessions.load_many(session_ids)` | `Scenarios.load_many(saved_scenario_ids)` |
| `Sessions.create_many(params)` | `Scenarios.create_many(params)`, with a `title` in each |
| `Sessions.from_excel(path)`, `to_excel(path)` | `Scenarios.from_excel(path)`, `to_excel(path)` |
| `scenarios.sessions` | Removed: `Scenarios` holds only `Scenario` |

**Scenario**

| 2.0 | 2.1 |
|---|---|
| `Scenario.new(title, session_id=...)` | Removed. Create with `area_code` and `end_year`, or copy a saved scenario |
| `Scenario.new(..., area_code, end_year)` | Unchanged, and now sends `ETM_VERSION` |
| `scenario.session` | Removed: the methods it held are on `Scenario` |
| `scenario.scenario` (the nested engine scenario) | Removed: see the read only properties above |
| `scenario.update(scenario_id=...)` | `scenario.save()`, which appends a Snapshot |
| `scenario.update(discarded=...)` | `scenario.discard()` |
| `update()`, `discard()`, `delete()`, `list_users()`, `update_users()`, `apply_pending_users()` with `client=` | Without `client=`. A scenario uses the client it was loaded with |
| `scenario.copy(**overrides)` passing engine fields | `copy(title=..., private=...)`. Set engine fields afterwards with `update_metadata()` |
| `SavedScenarioError` | Still raised, now an alias of `ScenarioError` |
| `list_users()` | Returns `user_email`, `role` and `pending` for each user |

**Scenarios**

| 2.0 | 2.1 |
|---|---|
| `create_many` with `scenario_id` (save a Session) | Removed |
| `create_many` with `template_id` (a Session ID) | `source`, a saved scenario ID |

**Collection**

| 2.0 | 2.1 |
|---|---|
| `Collection.create(title, scenario_ids=..., area_code=..., end_year=..., interpolation=...)` | `Collection.create(title, saved_scenario_ids, version=None)`. Interpolated collections and engine scenario IDs are not supported through MyETM's API |
| The fields `interpolation`, `area_code`, `end_year`, `scenario_ids`, `interpolation_params`, `discarded` | Removed. `discarded_at` replaces `discarded` |
| `collection.update(...)` | Accepts `title` and `saved_scenario_ids` |

**Excel**

| 2.0 | 2.1 |
|---|---|
| The `session` column in MAIN | Ignored with one warning. Every row is a saved scenario: `scenario_id` loads one, `copy_from` copies one, and `area_code` with `end_year` creates one |

**Lower level**

| 2.0 | 2.1 |
|---|---|
| `pyetm.clients.session.ETMSession` | `pyetm.clients.etm_client.ETMClient` |
| `CreateScenarioRunner`, `CopyScenarioRunner`, `InterpolateScenariosRunner`, `DestroySessionRunner`, `FetchUserScenariosRunner` | Removed: MyETM creates, copies and interpolates through `CreateSavedScenarioRunner` |
| `pyetm.config.api_compat` | Removed |
| `curve_registry.build_path()` | Removed: every path is built under `saved_scenarios/{id}/` |
| `base_client.make_batch_requests()` | `BaseRunner._make_batch_requests()` |
| `ScenarioRunnerProtocol`, `ScenarioIdentifier` | Removed |

### Added

- `Scenario.save()` appends a Snapshot: the current Session joins the scenario's history and
  a copy of it becomes current.
- `ETM_VERSION` setting.
- Every request sends `User-Agent: pyetm/<version>`.
- Errors read the API's v4 grammar: a message names the error's code and detail, and a field
  error names its pointer, for example `422: validation_failed: must be an integer (at
  /saved_scenario/end_year)`. Calculation errors read as before.
- A `401` says whether a token is set. A `502 engine_unavailable` raises `ConnectionError`.
- Loading a scenario on a Version other than `ETM_VERSION` warns once.
- A Compatibility page and the release line rule.

### Removed

- Support for the stable `2025-01` engine. To keep using it, pin `pyetm<2.1`.
- The unused `requests` dependency.
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
- Listing saved scenarios and collections follows the API's pagination, so every one is
  returned.
- `created_at` and `updated_at` in MAIN now describe the saved scenario, not its current
  session.
- A `BASE_URL` beside a different `ENVIRONMENT` now warns that `ENVIRONMENT` is ignored.

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
- `user_values`, `custom_curves` and `sortables` passed to `Scenario.new()` or `copy()` are
  uploaded. Custom curves and sortables were always refused before, as warnings.
- A scenario's calls use the client it was loaded with, including curve downloads, which
  used the default client.
- An empty `ENVIRONMENT` picks `pro`. It used to build an invalid `production.engine...` URL.

## [2.0.5] - 12-11-2026

### Changed

- Access is managed on the saved Scenario only. `Session.list_users()`,
  `Session.update_users()` and `Session.apply_pending_users()` now raise a
  `ScenarioError` pointing to the Scenario, without calling the engine.
- The `USERS` sheet reads and writes members through the saved Scenario, so they come
  from MyETM. A Session that is not saved exports no users, and importing users onto
  one logs a warning and skips it.
- `Session.delete()` says plainly that deleting a Session is admin only when the engine
  refuses it.

### Removed

- The `ScenarioUsersIndexRunner`, `ScenarioUsersCreateRunner`, `ScenarioUsersUpdateRunner` and `ScenarioUsersDestroyRunner`.

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
