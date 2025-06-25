# Running Notebooks

This guide will help you install everything you need to open and run our Jupyter notebooks in VS Code,
even if you don’t have Python, Jupyter, or any VS Code extensions installed yet. If you'd like some
additional guidance or you're having trouble, check out [this helpful guide from vscode](https://www.youtube.com/watch?v=suAkMeWJ1yE&ab_channel=VisualStudioCode).
Just note that in the video they use venv, whereas we use pipenv (these are practically interchangable).

---

## 1. Prerequisites

* A computer running **Windows 10+**, **macOS 10.14+**, or a recent **Linux** distribution.
* Internet access to download installers.

---

## 2. Install Python

Go to the [official Python download page](https://www.python.org/downloads/) and download the latest
**Python 3.x** installer for your OS.
-  **Windows:** Run the installer, **check** "Add Python to PATH", and click **Install Now**.
-  **macOS/Linux:** Follow the on-screen instructions. On Linux, you can also use your package manager:

   ```bash
   # Ubuntu/Debian
   sudo apt update && sudo apt install python3 python3-venv python3-pip
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
2. Click the **Extensions** icon (or press <kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>X</kbd>).
3. Install:

   * **Python** (by Microsoft)
   * **Jupyter** (by Microsoft)

---

## 5. Set Up a Python Environment (using Pipenv)

If you'd like to learn more about pipenv, [check out the docs here.](https://pipenv.pypa.io/en/latest/index.html)

In your project folder:

1. **Install Pipenv** (if you don’t have it already):

   ```bash
   pip install pipenv
   ```
2. **Install dependencies** from `Pipfile`:

   ```bash
   pipenv install
   ```
3. **Activate** the Pipenv environment:

   ```bash
   pipenv shell
   ```
4. When adding new packages:

   ```bash
   pipenv install <package>
   ```

---

## 6. Open & Run the Notebook

1. In VS Code, go to **File → Open Folder** and select this project’s root folder.
2. Locate a `.ipynb` file in the Examples folder, and click to open it.
3. At the top right of the notebook editor, click **Select Kernel** and choose the interpreter from
your Pipenv environment (it will mention `Pipenv`).
4. Run cells by clicking the ▶️ icon or pressing <kbd>Shift</kbd>+<kbd>Enter</kbd>.

---

## 7. Important Notes
After this setup you will be *almost* ready to run the Jupyter notebooks in the examples folder. You
still need to configure your settings as [outlined in the main readme.](README.md)
