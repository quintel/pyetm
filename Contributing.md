# Contributing to pyetm

Thank you for considering contributing to **pyetm**
This document explains how to set up your development environment, follow coding
standards, run tests, report issues and submit changes.

---

## 1. Development Setup

We use **[Poetry](https://python-poetry.org/)** to manage dependencies and environments. But of course
you can use whichever setup you're comfortable with.

Clone the repository:

```bash
git clone https://github.com/quintel/pyetm.git
cd pyetm
```

Install dependencies (including dev tools):

```bash
poetry install --with dev
```

Run commands inside the Poetry environment:

```bash
poetry run pytest
poetry run pyetm
```

Or enter the shell:

```bash
poetry shell
```

---

## 2. Code Standards

- **Python version**: 3.12+ (enforced in `pyproject.toml`)
- **Linting**: [Pylint](https://pylint.pycqa.org/) (configured to require a minimum score of 8.0).
- **Tests**: We would like to maintain a high test coverage, so please test your contributions

Lint before committing

```bash
poetry run pylint src/pyetm
poetry run pytest
```

---

## 3. Contributing Workflow

1. **Fork** the repository on GitHub.
2. **Create a new branch** for your feature or bugfix:
   ```bash
   git checkout -b feature/my-new-feature
   ```
3. **Write code and tests.**
4. **Run linting and tests** to ensure everything passes.
5. **Commit** using clear messages.
6. **Push** your branch and open a Pull Request.

---

## 4. Documentation

- User-facing docs live at [docs.energytransitionmodel.com](https://docs.energytransitionmodel.com/main/pyetm/introduction).
- Developer docs live here in the repo.
- Update **docstrings** when you add or modify functionality.

---

## 5. Reporting Issues

- Check existing [issues](https://github.com/quintel/pyetm/issues) first.
- When reporting a bug, please include:
  - Steps to reproduce
  - Expected vs actual behavior
  - Relevant logs/tracebacks
