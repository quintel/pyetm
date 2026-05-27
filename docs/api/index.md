# API Reference

Complete API documentation for pyetm, auto-generated from source code docstrings.

## Overview

This reference documents all public classes, methods, and functions in the pyetm library. The documentation is organized by module:

## Core Modules
- **[BaseClient](clients/base_client.md)** - Base HTTP client with request/response handling
- **[Session](clients/session.md)** - Session management for authenticated requests

### [Models](models/scenario.md)
Domain models representing ETM entities.

- **[Scenario](models/scenario.md)** - Main scenario model
- **[Inputs](models/inputs.md)** - Input management
- **[Custom Curves](models/custom_curves.md)** - Custom curve uploads
- **[GQueries](models/gqueries.md)** - Query results
- **[Scenario Packer](models/scenario_packer.md)** - Batch scenario management
- **[Packables](models/packables/packable.md)** - Reusable data structures

### [Services](services/scenario_runners/base_runner.md)
Business logic and API service operations.

- **[Scenario Runners](services/scenario_runners/base_runner.md)** - API operation runners
- **[Service Result](services/service_result.md)** - Service operation results

### [Config](config/settings.md)
Configuration management and settings.

- **[Settings](config/settings.md)** - Application configuration and environment

### [Utils](utils/excel_utils.md)
Utility functions and helpers.

- **[Excel Utils](utils/excel_utils.md)** - Excel file operations
- **[Safe Cast](utils/safe_cast.md)** - Type conversion utilities

### [Types](types.md)
Type definitions and type aliases.

### [Validators](validators.md)
Data validation utilities.

## See Also

- [User Guide](../user-guide/index.md) - Tutorials and guides
- [Getting Started](../getting-started/installation.md) - Installation and setup
- [Contributing](../contributing/guide.md) - Development guide
