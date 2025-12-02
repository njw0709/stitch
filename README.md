# STITCH: A Spatio-Temporal Integration Tool for Contextual and Historical data.

STITCH is a Python-based interactive interface for linking diverse data sources across geospatial and temporal dimensions to enable contextual and historical enrichment of individual-level observational data. Its primary goal is to facilitate the augmentation of datasets, such as survey, clinical, or cohort data, with spatio-temporal contextual information (e.g., air quality, weather, or neighborhood characteristics), which often are high-dimensional and large. STITCH supports flexible geospatial (e.g., by census tract, ZIP code, or geographic coordinates) and precise time-lagged linkage, allowing researchers to create and align longitudinal contextual data based on the date of the observation (e.g. when the interview was conducted). It also supports the integration of participants’ residential histories, enabling accurate linkage of contextual data to periods of residence and relocation. Designed for local deployment, STITCH provides a scalable, reproducible, and user-friendly solution for high-resolution spatio-temporal data integration, offering both command-line (CLI) and graphical (GUI) interfaces for flexible use across research workflows.

## Features

- **Lagged Data Linkage** - Compute n-day prior exposure for any number of lag periods
- **Residential History Support** - Account for participant moves during study period
- **Flexible Data Formats** - Supports CSV, Stata, Parquet, Feather, and Excel files
- **Parallel Processing** - Optimized for large-scale datasets with multiprocessing in local environment
- **Two Interfaces** - Choose between CLI for automation or GUI for interactive use

## Installation

### Quick Start (Standalone Application)

Download the pre-built standalone application for your platform:

- **macOS (Apple Silicon/M1/M2/M3)**: [Download STITCH-macOS-ARM.zip](https://github.com/njw0709/stitch/releases/latest/download/STITCH-macOS-ARM.zip)
  - Extract the ZIP file and run `STITCH.app`
  - **First-time users**: Right-click the app and select "Open" to bypass macOS Gatekeeper (app is unsigned)
  - If you see "damaged" error, run in Terminal: `xattr -cr /path/to/STITCH.app`

- **Windows**: [Download STITCH-Windows.zip](https://github.com/njw0709/stitch/releases/latest/download/STITCH-Windows.zip)
  - Extract the ZIP file and run `STITCH.exe`
  - If Windows Defender SmartScreen warns about the app, click "More info" → "Run anyway"
  - Note: Unsigned apps may trigger antivirus warnings (false positive)

**Troubleshooting**: If the app fails to start, check the error log file:
- **macOS/Linux**: `~/.stitch.log`
- **Windows**: `C:\Users\YourUsername\.stitch.log`

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
1. Selecting HRS survey data with dropdown selection for ID column, date column, and GEOID column
2. Configuring optional residential history with dynamic dropdown population from data
3. Selecting and validating contextual data directories with file name filtering and column selection
4. Setting pipeline parameters (number of lags, parallel processing)
5. Running the pipeline with real-time progress monitoring

See [stitch/gui/README.md](stitch/gui/README.md) for detailed GUI documentation.

### Option 2: Command-Line Interface (CLI)

For automation and scripting:

```bash
python stitch_cli.py \
    --hrs-data "path/to/HRSprep2016full.dta" \
    --context-dir "path/to/daily_heat_long" \
    --output_name "HRSHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col LINKCEN2010 \
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
    --hrs-data "path/to/HRSprep2016full.dta" \
    --residential-hist "path/to/residential_history.dta" \
    --res-hist-hhidpn hhidpn \
    --res-hist-movecol trmove_tr \
    --res-hist-mvyear mvyear \
    --res-hist-mvmonth mvmonth \
    --res-hist-moved-mark "1. move" \
    --res-hist-geoid LINKCEN2010 \
    --res-hist-survey-yr-col year \
    --res-hist-first-tract-mark 999.0 \
    --context-dir "path/to/daily_heat_long" \
    --output_name "HRSHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col LINKCEN2010 \
    --measure-type heat_index \
    --data-col HeatIndex \
    --contextual-geoid-col GEOID10 \
    --n-lags 365 \
    --save-dir "path/to/output" \
    --parallel
```

## CLI Arguments

### Required Arguments

- `--hrs-data`: Path to HRS survey data file (.dta)
- `--context-dir`: Directory containing daily contextual data files
- `--id-col`: Unique identifier column name (e.g., hhidpn)
- `--date-col`: Interview/collection date column name
- `--measure-type`: Measurement type (e.g., heat_index, pm25, ozone)
- `--save-dir`: Directory for output and temporary files

### Optional Arguments

- `--output_name`: Output filename (default: linked_data.dta)
- `--data-col`: Explicit data column name (overrides measure type inference)
- `--geoid-col`: GEOID column name in HRS data (default: LINKCEN2010)
- `--contextual-geoid-col`: GEOID column name in contextual data files (default: GEOID10)
- `--file-extension`: File extension to search for (e.g., .csv, .parquet)
- `--n-lags`: Number of lag days to compute (default: 365)
- `--parallel`: Enable parallel processing
- `--include-lag-date`: Include lag date columns in output

### Residential History Arguments

- `--residential-hist`: Path to residential history file
- `--res-hist-hhidpn`: ID column in residential history
- `--res-hist-movecol`: Move indicator column
- `--res-hist-mvyear`: Move year column
- `--res-hist-mvmonth`: Move month column
- `--res-hist-moved-mark`: Value indicating a move (default: "1. move")
- `--res-hist-geoid`: GEOID column in residential history
- `--res-hist-survey-yr-col`: Survey year column
- `--res-hist-first-tract-mark`: First tract indicator value (default: 999.0)

## Package Structure

```
stitch/
├── stitch/
│   ├── hrs.py                 # HRS data handling classes
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
Handles temporal/geographic alignment between HRS and contextual data, including n-day prior date calculation and GEOID assignment.

## Examples

### Example 1: Heat Index Linkage (365 days)

```bash
python stitch_cli.py \
    --hrs-data "data/HRS2016.dta" \
    --context-dir "data/heat_index" \
    --measure-type heat_index \
    --data-col HeatIndex \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col LINKCEN2010 \
    --contextual-geoid-col GEOID10 \
    --n-lags 365 \
    --save-dir "output/heat" \
    --parallel
```

### Example 2: PM2.5 with Residential History

```bash
python stitch_cli.py \
    --hrs-data "data/HRS2016.dta" \
    --residential-hist "data/residential_moves.dta" \
    --context-dir "data/pm25" \
    --measure-type pm25 \
    --id-col hhidpn \
    --date-col iwdate \
    --geoid-col LINKCEN2010 \
    --contextual-geoid-col GEOID10 \
    --n-lags 730 \
    --save-dir "output/pm25" \
    --parallel
```

## Performance

- **Parallel Processing**: Recommended for datasets with 500+ lags
- **Memory Optimization**: Uses chunked reading and GEOID filtering for large files
- **Efficient Storage**: Temporary lag files stored as Parquet for fast I/O

## Data Requirements

### HRS Survey Data
- Format: Stata (.dta) file
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
- Format: Stata (.dta) file
- Required columns:
  - Participant ID
  - Move indicator
  - Move year/month
  - GEOID for each residence
  - Survey year

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

