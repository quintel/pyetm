<img style="max-width:100%;height:auto;" alt="PyETM Logo (16xRes)" src="https://github.com/user-attachments/assets/3570d78f-681f-4360-935e-906a95807f15" />

---

# Basic Guide

This guide walks you through setting up PyETM on your computer and using it to update your Energy Transition Model scenarios from an Excel file.

---

## What you will need

- A computer running **Windows** or **macOS**
- An **ETM API token** — found in the **Personal Access Tokens** section of your MyETM user profile.

---

## Step 1 — Install Python

Python is the programming language that powers this tool. You only need to install it once.

**Windows:**
1. Go to [python.org/downloads](https://www.python.org/downloads/)
2. Click **Download Python** (choose the latest version)
3. Run the installer — **important:** check the box that says **"Add Python to PATH"** before clicking Install

**Mac:**
1. Go to [python.org/downloads](https://www.python.org/downloads/)
2. Click **Download Python** and run the installer

To verify Python is installed, open a terminal and run:
```
python3 --version
```
You should see something like `Python 3.12.x`. On Windows you may need to use `python` instead of `python3`.

**How to open a terminal:**
- **Mac:** Press `Cmd + Space`, type `Terminal`, press Enter
- **Windows:** Press `Win + R`, type `cmd`, press Enter

---

## Step 2 — Install the ETM scenario tool

In your terminal, run:

```
pip install pyetm
```

This installs the tool and everything it needs. You only need to do this once.

---

## Step 3 — Create your project folder

Choose a folder on your computer where you will keep your scenario files. In your terminal, navigate to that folder:

```
cd path/to/your/folder
```

For example on Windows: `cd C:\Users\YourName\ETM`
For example on Mac: `cd /Users/YourName/ETM`

Then run:

```
pyetm init
```

The tool will ask for:
- Your **ETM API token** — paste it and press Enter
- The **environment** — type your environment and press Enter

After this, your folder will contain:
- `.env` — your configuration file. **Never share this file — it contains your token.**
- `input.xlsx` — the Excel file you fill in to define your scenarios
- `outputs/` — this folder will be created automatically when you run the tool

---

## Step 4 — Fill in your input Excel file

Open `input.xlsx`. It contains three sheets:

**MAIN** — one row per scenario:
- `short_name` — a short label for the scenario (no spaces), used to link sheets together
- `scenario_id` — the ID of an existing MyETM saved scenario to update
- `title` — the scenario title
- `area_code` — the region code (e.g. `nl2019`)
- `end_year` — the target year (e.g. `2050`)
- `private` — set to `True` to make the scenario private in MyETM
- `custom_curves` — name of a sheet in this file containing custom curve uploads for this scenario (optional)
- `sortables` — name of a sheet in this file containing sortable order settings for this scenario (optional)

**EXPORT_CONFIG** — set `True` or `False` to control what appears in the output file. To also export annual energy data, fill in the `include_annual_exports` column with a comma-separated list of export types (e.g. `energy_flow, production_parameters`). This produces an additional file: `outputs/output_annual_exports.xlsx`.

**SLIDER_SETTINGS** — set input values for each scenario:
- First column: the input slider key (e.g. `buildings_solar_pv_solar_radiation`)
- Remaining columns: one per scenario, with the column header matching the `short_name` from MAIN
- Enter a number in a scenario column cell to set that slider value; leave it empty to keep the ETM default

**GQUERIES** _(optional)_ — list the ETM query keys you want to fetch results for:
- One key per row in the first column, no header row
- Results are written to a separate `GQUERIES_RESULTS` sheet in the output file
- Leave this sheet empty if you do not need query results

Save the file when you are done.

---

## Step 5 — Run the tool

In your terminal (from your project folder), run:

```
pyetm run
```

This will:
1. Read your scenarios from `input.xlsx`
2. Push your input values to the Energy Transition Model
3. Fetch the results
4. Write everything to `outputs/output.xlsx` (and `outputs/output_annual_exports.xlsx` if annual exports are enabled in EXPORT_CONFIG)

---

## Step 6 — View your results

Open the Excel files generated in the `outputs/` folder to see the results for all your scenarios.

---

## Using custom file names

If you want to use a different input or output file:

```
pyetm run --input my_scenarios.xlsx --output outputs/my_results.xlsx
```

Any additional files (e.g. annual exports) are written to the same folder, using the same base name: `outputs/my_results_annual_exports.xlsx`.
