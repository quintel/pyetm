# Running Notebooks

This guide will help you install everything you need to open and run our Jupyter notebooks in VS Code,
even if you don’t have Python, Jupyter, or any VS Code extensions installed yet.

If you'd like some additional guidance or you're having trouble, check out [this helpful guide from VS Code](https://www.youtube.com/watch?v=suAkMeWJ1yE&ab_channel=VisualStudioCode).
Just note: in the video they use `venv`, whereas we use [Poetry](https://python-poetry.org/) for dependency management and virtual environments.

---

## 1. Prerequisites

* A computer running **Windows 10+**, **macOS 10.14+**, or a recent **Linux** distribution.
* Internet access to download installers.

---

## 2. Install Python

Go to the [official Python download page](https://www.python.org/downloads/) and download the latest
**Python 3.12+** installer for your OS.

- **Windows:** Run the installer, **check** "Add Python to PATH", and click **Install Now**.
- **macOS/Linux:** Follow the on-screen instructions. On macOS you can also use [Homebrew](https://brew.sh/):
  ```bash
  brew install python@3.12
  ```
  On Linux, you can use your package manager:
  ```bash
  # Ubuntu/Debian
  sudo apt update && sudo apt install python3 python3-pip
  ```

Verify installation by opening a terminal and running:
```bash
python3 --version
# or on Windows
python --version
```

---

## 3. Install VS Code

1. Download VS Code from [code.visualstudio.com](https://code.visualstudio.com/).
2. Run the installer and follow the prompts.
3. (Optional) Check "Add to PATH" during installation for easier command-line use.

Verify by running:
```bash
code --version
```

---

## 4. Install Necessary VS Code Extensions

1. Open VS Code.
2. Click the **Extensions** icon (or press `Ctrl+Shift+X` / `Cmd+Shift+X` on macOS).
3. Install:
   * **Python** (by Microsoft)
   * **Jupyter** (by Microsoft)

---

## 5. Set Up a Python Environment (using Poetry)

We use [Poetry](https://python-poetry.org/) to manage dependencies and virtual environments.

1. **Install Poetry** (if you don’t have it already):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```
   Or on Windows (PowerShell):
   ```powershell
   (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
   ```

2. **Install dependencies** from `pyproject.toml`:
   ```bash
   poetry install --with dev
   ```

3. **Activate** the Poetry environment:
   ```bash
   eval $(poetry env activate)
   ```

4. When adding new packages:
   ```bash
   poetry add <package>
   ```

---

## 6. Open & Run the Notebook

1. In VS Code, go to **File → Open Folder** and select this project’s root folder.
2. Locate a `.ipynb` file in the `examples` folder, and click to open it.
3. At the top right of the notebook editor, click **Select Kernel** and choose the interpreter from your Poetry environment (it will mention `.venv` or the Poetry-managed path).
4. Run cells by clicking the ▶️ icon or pressing `Shift+Enter`.

---

## 7. Important Notes

After this setup you will be *almost* ready to run the Jupyter notebooks in the examples folder.
You still need to configure your settings as [outlined in the main README](README.md).
