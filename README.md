# STITCH: A Spatio-Temporal Integration Tool for Contextual and Historical data.

STITCH is a Python-based interactive interface for linking diverse data sources across geospatial and temporal dimensions to enable contextual and historical enrichment of individual-level observational data. Its primary goal is to facilitate the augmentation of datasets, such as survey, clinical, or cohort data, with spatio-temporal contextual information (e.g., air quality, weather, or neighborhood characteristics), which often are high-dimensional and large. STITCH supports flexible geospatial (e.g., by census tract, ZIP code, or geographic coordinates) and precise time-lagged linkage, allowing researchers to create and align longitudinal contextual data based on the date of the observation (e.g. when the interview was conducted). It also supports the integration of participants’ residential histories, enabling accurate linkage of contextual data to periods of residence and relocation. Designed for local deployment, STITCH provides a scalable, reproducible, and user-friendly solution for high-resolution spatio-temporal data integration, offering both command-line (CLI) and graphical (GUI) interfaces for flexible use across research workflows.

## Features

- **Lagged Data Linkage** - Compute n-day prior exposure over a configurable lag window (choose both the start and end day prior)
- **Post-lag Averaging** - Optionally collapse the whole lag window into a single averaged column per measure (e.g. mean exposure over 0–364 days prior)
- **Residential History Support** - Account for participant moves during study period
- **Flexible Data Formats** - Supports CSV, Stata, Parquet, Feather, and Excel files
- **Parallel Processing** - Optimized for large-scale datasets with multiprocessing in local environment
- **Inspectable Intermediates** - Optionally keep the per-lag intermediate files as CSV in your output directory
- **Two Interfaces** - Choose between CLI for automation or GUI for interactive use

## Installation

### Quick Start (Standalone Application)

Download the pre-built standalone application for your platform:

- **macOS (Apple Silicon/M1/M2/M3)**: [Download STITCH-macOS-ARM.zip](https://github.com/njw0709/stitch/releases/latest/download/STITCH-macOS-ARM.zip)
  - Extract the ZIP file and run `STITCH.app`
  - **First-time users**: Right-click the app and select "Open" to bypass macOS Gatekeeper (app is unsigned).  If it does not show you an option to "Open Anyway", you can go to System Preferences > Security & Privacy > General and click "Allow Anyway" for STITCH.app.
  - If you see "damaged" error, run in Terminal: `xattr -cr /path/to/STITCH.app`

- **Windows**: [Download STITCH-Windows.zip](https://github.com/njw0709/stitch/releases/latest/download/STITCH-Windows.zip)
  - Extract the ZIP file and run `STITCH.exe`
  - If Windows Defender SmartScreen warns about the app, click "More info" → "Run anyway"
  - Note: Unsigned apps may trigger antivirus warnings (false positive)

**Troubleshooting**: If the app fails to start, check the error log file:
- **macOS/Linux**: `~/.stitch.log`
- **Windows**: `C:\Users\YourUsername\.stitch.log`

(When running from source instead of the standalone app, the default log file is
`~/.STITCH-linkage-tool.log`. In either case you can override the location by
setting the `STITCH_LOG_FILE` environment variable.)

### Build from Source

If you prefer to run from source or need the CLI:

```bash
# Clone the repository
git clone <repository-url>
cd stitch

# Install dependencies using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

## Usage

### Option 1: Graphical User Interface (GUI)

Launch the interactive wizard:

```bash
uv run python gui_app.py
```

The GUI provides a step-by-step wizard for:
1. Selecting STITCH survey data with dropdown selection for ID column, date column, and GEOID column
2. Configuring optional residential history with dynamic dropdown population from data
3. Selecting and validating contextual data directories with file name filtering and column selection
4. Setting pipeline parameters (the temporal lag window, parallel processing, output options)
5. Running the pipeline with real-time progress monitoring

#### Queueing and running jobs sequentially

The GUI is organized as a multi-job dashboard. Each time you complete the wizard
you add a configured job to the queue (the wizard's final button is labeled
**Add Job**), so you can set up several linkage runs — for example different
measures, lag windows, or output files — before running anything. You can
**Edit** or **Remove** a queued job, or open its output directory.

Clicking **Run All** executes every pending job **sequentially** in a single
modal progress dialog, one after another, reporting each job's status
(Running / Done / Failed) live. Jobs that already ran are skipped on the next
**Run All**; edit a job to reset it to Pending and run it again.

See [stitch/gui/README.md](stitch/gui/README.md) for detailed GUI documentation.

### Option 2: Command-Line Interface (CLI)

For automation and scripting:

```bash
python stitch_cli.py \
    --survey-data "path/to/surveyprep2016full.dta" \
    --context-dir "path/to/daily_heat_long" \
    --output_name "surveyHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col GEOID2010 \
    --measure-type heat_index \
    --data-col HeatIndex \
    --contextual-geoid-col GEOID10 \
    --n-lags 365 \
    --save-dir "path/to/output" \
    --parallel
```

#### With Residential History

```bash
python stitch_cli.py \
    --survey-data "path/to/surveyprep2016full.dta" \
    --residential-hist "path/to/residential_history.dta" \
    --res-hist-id-col hhidpn \
    --res-hist-date-col move_date \
    --res-hist-geoid-col GEOID \
    --context-dir "path/to/daily_heat_long" \
    --output_name "surveyHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col GEOID2010 \
    --measure-type heat_index \
    --data-col HeatIndex \
    --contextual-geoid-col GEOID10 \
    --n-lags 365 \
    --save-dir "path/to/output" \
    --parallel
```

## CLI Arguments

### Required Arguments

- `--survey-data`: Path to survey survey data file (.dta)
- `--context-dir`: Directory containing daily contextual data files
- `--id-col`: Unique identifier column name (e.g., hhidpn)
- `--date-col`: Interview/collection date column name
- `--measure-type`: Measurement type (e.g., heat_index, pm25, ozone)
- `--save-dir`: Directory for output and temporary files

### Optional Arguments

- `--output_name`: Output filename (default: linked_data.dta)
- `--data-col`: Explicit data column name (overrides measure type inference)
- `--geoid-col`: GEOID column name in survey data (default: GEOID2010)
- `--contextual-geoid-col`: GEOID column name in contextual data files (default: GEOID10)
- `--file-extension`: File extension to search for (e.g., .csv, .parquet)
- `--n-lags`: Number of lag days to compute, i.e. the exclusive upper bound of the lag window (default: 365, so lags 0–364 days prior)
- `--start-lag`: Lag day to start from, i.e. the minimum days prior (default: 0). Combined with `--n-lags`, the pipeline processes lags `start_lag`–`n_lags − 1` days prior
- `--parallel`: Enable parallel processing
- `--include-lag-date`: Include lag date and GEOID columns in output (one `{date_col}_{n}day_prior` and one `{geoid_col}_{n}day_prior` column per lag). Ignored if `--post-lag-average` is also set (averaging wins)
- `--post-lag-average`: Average each measure across all lags into a single column per measure (e.g. `HeatIndex_avg_0_364day_prior`) instead of one column per lag. Strict handling: a participant missing a value for **any** lag in the range gets a missing (NaN) average. Incompatible with `--include-lag-date`
- `--save-temp-to-output`: Write the intermediate per-lag files as CSV into `<save-dir>/<output_stem>_lag_files/` and keep them after the run (default: hidden Parquet files in a private temp directory, deleted on success)

### Residential History Arguments

The residential history file is a long-format table with one row per residence
and three columns: a participant ID, a move date, and a GEOID. The earliest
entry per person is used as their residence at survey entry (no special "first
tract" marker is needed).

- `--residential-hist`: Path to residential history file
- `--res-hist-id-col`: ID column in residential history (default: hhidpn)
- `--res-hist-date-col`: Move date column (default: move_date). The format is
  inferred per value: full dates (`2010-03-15`, `March 2010`, `21sep2018`),
  year-month (`2010-03`), or numeric `YYYY` / `YYYYMM` / `YYYYMMDD`. Values
  coarser than daily are anchored to the midpoint of the period they span
  (year-only → mid-year, year-month → mid-month, date-only → noon).
- `--res-hist-geoid-col`: GEOID column in residential history (default: GEOID)

## Package Structure

```
stitch/
├── stitch/
│   ├── hrs.py                 # survey data handling classes
│   ├── daily_measure.py       # Contextual data loading
│   ├── process.py             # Parallel/batch processing
│   ├── io_utils.py            # File I/O utilities
│   └── gui/                   # GUI application
│       ├── main_window.py     # Main wizard window
│       ├── validators.py      # Data validation
│       ├── pages/             # Wizard pages
│       └── widgets/           # Reusable UI components
├── stitch_cli.py               # CLI entry point
├── gui_app.py                 # GUI entry point
└── tests/                     # Test suite
```

## Key Classes

### `HRSInterviewData`
Wrapper for HRS survey data with date-based GEOID creation for contextual data linkage.

### `ResidentialHistoryHRS`
Parses residential move history and enables date-based GEOID lookup accounting for participant moves.

### `DailyMeasureDataDir`
Directory-level wrapper that lazy-loads yearly contextual data files with validation.

### `HRSContextLinker`
Handles temporal/geographic alignment between survey and contextual data, including n-day prior date calculation and GEOID assignment.

## Examples

### Example 1: Heat Index Linkage (0–364 days prior)

```bash
python stitch_cli.py \
    --survey-data "data/survey2016.dta" \
    --context-dir "data/heat_index" \
    --measure-type heat_index \
    --data-col HeatIndex \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col GEOID2010 \
    --contextual-geoid-col GEOID10 \
    --start-lag 0 \
    --n-lags 365 \
    --save-dir "output/heat" \
    --parallel
```

### Example 2: PM2.5 with Residential History (30–729 days prior)

This example skips the most recent 30 days by starting the lag window at day 30,
processing lags 30 through 729 days prior.

```bash
python stitch_cli.py \
    --survey-data "data/survey2016.dta" \
    --residential-hist "data/residential_moves.dta" \
    --context-dir "data/pm25" \
    --measure-type pm25 \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col GEOID2010 \
    --contextual-geoid-col GEOID10 \
    --start-lag 30 \
    --n-lags 730 \
    --save-dir "output/pm25" \
    --parallel
```

### Example 3: Post-lag Averaging (single averaged column per measure)

Add `--post-lag-average` to collapse the entire lag window into one averaged
column per measure instead of one column per lag day. Here the output contains a
single `HeatIndex_avg_0_364day_prior` column holding each participant's mean heat
index over the 0–364 days prior to their interview. Averaging is strict: if a
participant is missing the value for any lag in the range, their average is
missing (NaN). This option cannot be combined with `--include-lag-date` (it is
ignored if both are given).

```bash
python stitch_cli.py \
    --survey-data "data/survey2016.dta" \
    --context-dir "data/heat_index" \
    --measure-type heat_index \
    --data-col HeatIndex \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col GEOID2010 \
    --contextual-geoid-col GEOID10 \
    --start-lag 0 \
    --n-lags 365 \
    --save-dir "output/heat" \
    --post-lag-average \
    --parallel
```

### Example 4: Saving the Intermediate Lag Files to the Output Directory

Add `--save-temp-to-output` to keep the per-lag intermediate files as CSV so you
can inspect them. Instead of hidden Parquet files in a private temporary
directory (deleted on success), the lag files are written to
`output/heat/surveyHeatLinked_lag_files/` (named `<save-dir>/<output_stem>_lag_files/`)
and kept after the run. This can be combined with any other option, including
`--post-lag-average`.

```bash
python stitch_cli.py \
    --survey-data "data/survey2016.dta" \
    --context-dir "data/heat_index" \
    --output_name "surveyHeatLinked.dta" \
    --measure-type heat_index \
    --data-col HeatIndex \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col GEOID2010 \
    --contextual-geoid-col GEOID10 \
    --start-lag 0 \
    --n-lags 365 \
    --save-dir "output/heat" \
    --save-temp-to-output \
    --parallel
```

## Performance

- **Parallel Processing**: Recommended for datasets with 500+ lags
- **Memory Optimization**: Uses chunked reading and GEOID filtering for large files
- **Efficient Storage**: Temporary lag files stored as Parquet for fast I/O

## Temporary Files and Resuming

While the pipeline runs, each lag is written to an intermediate ("lag") Parquet
file before being merged into the final output.

- **Location**: These files are written to a unique, private per-job directory
  created inside the operating system's temporary location — `$TMPDIR` / `/tmp`
  on Linux/macOS, `%TEMP%` on Windows — named `stitch_<id>_...`. They are
  **not** placed in your `--save-dir` / output directory. The directory is
  created with owner-only permissions (mode `0o700`) so the intermediate files,
  which may contain confidential information, stay out of reach of other users,
  and concurrent jobs never collide.
- **Cleanup on completion**: When a job finishes successfully and its output is
  merged and saved, its temporary directory (and all lag files it held) is
  deleted automatically.
- **Resuming interrupted runs**: If a run is interrupted (it fails, is stopped
  mid-run, or crashes), its temporary directory is left in place with the lag
  files already computed. Re-running the **identical** configuration detects the
  matching directory (via a job signature) and resumes into it, reprocessing
  only the lags that are still missing rather than starting over.
- **GUI hygiene**: The GUI removes leftover (incomplete) STITCH temporary
  directories on startup and again on quit, so no confidential intermediate lag
  files persist across sessions.

### Keeping the intermediate files (`--save-temp-to-output`)

If you want to inspect or archive the per-lag intermediate files, pass
`--save-temp-to-output` (CLI) or check **"Save intermediate lag files to output
directory (as CSV)"** (GUI). This changes the behavior above:

- The lag files are written as **CSV** into `<save-dir>/<output_stem>_lag_files/`
  (a subfolder next to your final output) instead of a hidden private directory.
- They are **kept after a successful run** rather than deleted.
- Note this places the intermediate files, which may contain confidential
  information, inside your user-visible output directory — use it deliberately.

## Data Requirements

### Survey/Interview Data
- Format: Stata (.dta), CSV, Parquet, Feather, or Excel file
- Required columns:
  - Unique participant ID (selectable via dropdown in GUI or `--id-col` in CLI)
  - Date column (interview/collection date, selectable via `--date-col`)
  - GEOID column (census tract identifier, selectable via `--geoid-col`)

### Contextual Data Files
- Format: CSV, Stata, Parquet, Feather, or Excel
- Naming: Must include 4-digit year (e.g., `heat_2016.csv`)
- Structure: Long format with date, GEOID, and measure columns
  - Date column (selectable via dropdown in GUI or defaults to "Date")
  - GEOID column (selectable via `--contextual-geoid-col`, default: GEOID10)
- Consistency: All files must have identical column names
- File filtering: Use `--measure-type` to select files containing specific substrings

### Residential History (Optional)
- Format: Stata (.dta), CSV, Parquet, Feather, or Excel file
- Long format with one row per residence and three required columns:
  - Participant ID (links to the primary dataset)
  - Move date (when the person started living at this location; format inferred
    automatically — e.g. `2013`, `2013-06`, `March 2013`, `2013-06-15`)
  - GEOID for that residence
- The earliest-dated row per person is treated as their residence at survey
  entry; dates earlier than a person's first recorded date resolve to NA.

## Troubleshooting

### "No year information found in filenames"
Ensure contextual data filenames contain 4-digit years (e.g., `data_2016.csv`).

### "Column mismatch between files"
All contextual data files must have identical column names. Check for typos or naming inconsistencies.

### Memory Issues
- Use `--geoid-filter` to limit to specific geographic areas
- Process in smaller lag batches
- Use Parquet format for contextual data

### GUI Won't Start
- Ensure PyQt6 is installed: `uv add PyQt6`
- GUI requires a display server (not available on headless systems)
- Use CLI on servers without displays

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- Tests pass
- Documentation is updated

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).  
You are free to use, modify, and distribute this software, provided that you include the original copyright and license notice.
## Citation

