# Installation

This guide will help you install pyetm and set up your development environment.

## Prerequisites

Before installing pyetm, ensure you have Python 3.12 or later installed on your system.

### Check Your Python Version

```bash
python3 --version
```

You should see output like `Python 3.12.0` or higher.

### Installing Python

If you need to install or upgrade Python:

=== "macOS"
    Using Homebrew:
    ```bash
    brew install python@3.12
    ```

=== "Windows"
    Download the installer from [python.org](https://www.python.org/downloads/windows/) and run it.

    !!! warning "Add to PATH"
        Make sure to check "Add Python to PATH" during installation.

=== "Linux"
    Using apt (Debian/Ubuntu):
    ```bash
    sudo apt update
    sudo apt install python3.12 python3.12-venv python3-pip
    ```

    Using dnf (Fedora/RHEL):
    ```bash
    sudo dnf install python3.12
    ```

## Installation Methods

### Method 1: pip (Recommended for Users)

Install pyetm directly from PyPI:

```bash
pip install pyetm
```

To install a specific version:

```bash
pip install pyetm==2.0.0b9
```

### Method 2: Poetry (Recommended for Development)

If you're contributing to pyetm or want to work with the source code:

1. **Install Poetry** (if not already installed):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Clone the repository**:
   ```bash
   git clone https://github.com/quintel/pyetm.git
   cd pyetm
   ```

3. **Install dependencies**:
   ```bash
   poetry install
   ```

4. **Activate the virtual environment**:
   ```bash
   poetry shell
   ```

### Method 3: From Source

To install directly from the GitHub repository:

```bash
pip install git+https://github.com/quintel/pyetm.git
```

## Virtual Environments

!!! tip "Best Practice"
    Always use a virtual environment to isolate your project dependencies.

### Using venv

Create a virtual environment:

```bash
python3 -m venv .venv
```

Activate it:

=== "macOS/Linux"
    ```bash
    source .venv/bin/activate
    ```

=== "Windows (PowerShell)"
    ```powershell
    .venv\Scripts\Activate.ps1
    ```

=== "Windows (Command Prompt)"
    ```batch
    .venv\Scripts\activate.bat
    ```

Install pyetm:

```bash
pip install pyetm
```

### Using Poetry

Poetry automatically manages virtual environments for you:

```bash
poetry add pyetm
```

## Verifying Installation

After installation, verify that pyetm is working correctly:

```python
import pyetm

print(pyetm.__version__)
```

Or from the command line:

```bash
python3 -c "import pyetm; print(pyetm.__version__)"
```

You should see the version number printed (e.g., `2.0.0b9`).

## Optional Dependencies

pyetm has minimal required dependencies, but you may want to install additional packages for specific use cases:

### For Jupyter Notebooks

```bash
pip install jupyter notebook
```

### For Data Analysis

```bash
pip install pandas matplotlib seaborn
```

These are already included in the base installation of pyetm.

## Next Steps

Now that you have pyetm installed, proceed to the [Quick Start Guide](quickstart.md) to create your first scenario!

## Troubleshooting

### SSL Certificate Errors

If you encounter SSL certificate errors when connecting to the ETM API, see the [Configuration Guide](configuration.md#ssl-configuration) for solutions.

### Import Errors

If you see `ModuleNotFoundError: No module named 'pyetm'`:

1. Ensure your virtual environment is activated
2. Verify the installation with `pip list | grep pyetm`
3. Try reinstalling: `pip install --force-reinstall pyetm`

### Permission Errors

If you see permission errors during installation:

- Use a virtual environment (recommended)
- Or install with the `--user` flag: `pip install --user pyetm`
- Never use `sudo pip install` as it can cause system-wide conflicts
