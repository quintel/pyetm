# Contributing Guide

Thank you for your interest in contributing to pyetm! This guide will help you get started.

## Ways to Contribute

- **Report bugs**: Open an issue on [GitHub Issues](https://github.com/quintel/pyetm/issues)
- **Suggest features**: Discuss ideas in [GitHub Discussions](https://github.com/quintel/pyetm/discussions)
- **Improve documentation**: Submit documentation improvements
- **Write code**: Fix bugs or implement new features

## Getting Started

See [Development](development.md) for setup instructions.

## Code Style

pyetm follows these conventions:

- **PEP 8** for Python code style
- **Google-style docstrings** for documentation
- **Type hints** for all functions and methods
- **100 characters** maximum line length

## Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run tests: `poetry run pytest`
5. Commit your changes
6. Push and create a pull request

## Testing

All new code must include tests:

```bash
# Run all tests
poetry run pytest

# Run specific test
poetry run pytest tests/models/test_scenario.py

# Run with coverage
poetry run pytest --cov=pyetm
```

## Documentation

Update documentation for user-facing changes:

- Update docstrings using Google style
- Add examples to user guide if applicable
- Build docs locally: `poetry run mkdocs serve`

## Questions?

Ask in [GitHub Discussions](https://github.com/quintel/pyetm/discussions) or open an issue.
