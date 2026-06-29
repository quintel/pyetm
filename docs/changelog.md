# Changelog

All notable changes to pyetm are documented here.

## [2.0.1] - 29-06-2026

### Changed

- Capacity tables (`electricity_capacities`, `district_heating_capacities`,
  `hydrogen_capacities`, `network_gas_capacities`) are classified as **annual exports**,
  not hourly output curves. Fetch them with `get_annual_export()` /
  `get_annual_exports()` instead of `get_hourly_curve()`.

### Breaking

- Capacity tables are only available on modern engines (`pro`). On the stable `2025-01` tag
  they do not exist; `get_annual_export()` returns `None` in that case.
