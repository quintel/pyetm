# API Reference

Complete API documentation for pyetm, auto-generated from source code docstrings.

## Overview

This reference documents all public classes, methods, and functions in the pyetm library. The documentation is organized by module:

## Core Modules

### [Clients](clients/base_client.md)
HTTP client infrastructure for communicating with the ETM API.

- **[BaseClient](clients/base_client.md)** - Base HTTP client with request/response handling
- **[Session](clients/session.md)** - Session management for authenticated requests

### [Models](models/scenario.md)
Domain models representing ETM entities.

- **[Scenario](models/scenario.md)** - Main scenario model
- **[Inputs](models/inputs.md)** - Input management
- **[Custom Curves](models/custom_curves.md)** - Custom curve uploads
- **[GQueries](models/gqueries.md)** - Query results
- **[Scenario Packer](models/scenario_packer.md)** - Batch scenario generation
- **[Packables](models/packables/packable.md)** - Reusable data structures

### [Services](services/service_result.md)
Business logic and API service operations.

- **[Service Result](services/service_result.md)** - Service operation results
- **[Scenario Runners](services/scenario_runners/base_runner.md)** - API operation runners

### [Config](config/settings.md)
Configuration management and settings.

- **[Settings](config/settings.md)** - Application configuration and environment

### [Utils](utils/safe_cast.md)
Utility functions and helpers.

- **[Excel Utils](utils/excel_utils.md)** - Excel file operations
- **[Scenario Excel Service](utils/scenario_excel_service.md)** - Scenario Excel export/import
- **[Safe Cast](utils/safe_cast.md)** - Type conversion utilities
- **[Singleton](utils/singleton.md)** - Singleton pattern implementation

### [Types](types.md)
Type definitions and type aliases.

### [Validators](validators.md)
Data validation utilities.

## Quick Navigation

**Working with Scenarios:**
- Create new: [Scenario.create(title, area_code, end_year)](models/scenario.md)
- Save existing session: [Scenario.create(title, session_id)](models/scenario.md)
- Load: [Scenario.load(saved_scenario_id)](models/scenario.md)

**Managing Inputs:**
- Set inputs: [Scenario.user_values](models/scenario.md)
- Load from file: [InputsPack](models/packables/inputs_pack.md)

**Querying Results:**
- Query single: [Scenario.query_result()](models/scenario.md)
- Query multiple: [Scenario.query_results()](models/scenario.md)

**Exporting Data:**
- Hourly curves: [HourlyOutputCurves](models/hourly_output_curves.md)
- Annual data: [AnnualExports](models/annual_exports.md)

## Documentation Style

This documentation uses **Google-style docstrings** with:

- **Args**: Function/method parameters
- **Returns**: Return values and types
- **Raises**: Exceptions that may be raised
- **Example**: Usage examples

## See Also

- [User Guide](../user-guide/index.md) - Tutorials and guides
- [Getting Started](../getting-started/installation.md) - Installation and setup
- [Contributing](../contributing/guide.md) - Development guide
