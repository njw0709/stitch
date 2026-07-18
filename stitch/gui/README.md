# STITCH GUI

A PyQt6-based graphical user interface for STITCH - A Spatio-Temporal Integration Tool for Contextual enricHment of survey and observational data.

## Overview

This GUI provides a wizard-style interface for configuring and running the lagged contextual data linkage pipeline. It guides users through:

1. **Base (survey/interview) Dataset Selection** - Load HRS survey/interview data and select the date column
2. **Residential History (Optional)** - Configure residential move history if participants moved
3. **Contextual Data Directory** - Select daily contextual data (heat, PM2.5, etc.) with automatic validation
4. **Pipeline Configuration** - Set processing parameters (lags, parallel execution, output)
5. **Execution** - Run the pipeline with real-time progress monitoring

## Requirements

- Python 3.8+
- PyQt6
- pandas
- All dependencies from the main stitch package

## Installation

The GUI dependencies are included with the main package. If you haven't already:

```bash
# Using uv (recommended)
uv add PyQt6

# Or using pip
pip install PyQt6
```

## Usage

### Launching the GUI

From the project root directory:

```bash
# Using uv
uv run python gui_app.py

# Or if in activated virtual environment
python gui_app.py
```

### Workflow

1. **Select Base (survey/interview) Dataset**
   - Browse and select your survey/interview data (e.g., HRS) file (.dta format)
   - Preview the first 5 rows
   - Choose which column contains the date information

2. **Configure Residential History** (Optional)
   - Check "Use residential history data" if participants moved during study period
   - Load the residential history file (one row per residence)
   - Map the three columns: participant ID, move date, and GEOID
   - The move-date column is checked automatically to confirm it parses as dates;
     the earliest entry per person is used as their residence at survey entry

3. **Select Contextual Data**
   - Browse to directory containing daily contextual data files
   - Specify measure type (e.g., "heat_index", "pm25")
   - Choose file extension or use auto-detect
   - Preview automatically shows first file
   - Select data and GEOID columns
   - Automatic validation checks:
     - Year information in filenames
     - Column consistency across all files

4. **Configure Pipeline**
   - Set ID column name
   - Choose number of lags to compute
   - Enable parallel processing for faster execution
   - Select output directory and filename
   - Review configuration summary

5. **Execute Pipeline**
   - Review command that will be executed
   - Click "Run Pipeline" to start
   - Monitor real-time output
   - Save log file if needed
   - Open output directory when complete

## Features

- **Data Preview** - Preview data files before processing
- **Automatic Validation** - Checks file formats, columns, and consistency
- **Progress Monitoring** - Real-time output from the pipeline
- **Error Handling** - User-friendly error messages and validation
- **Configuration Summary** - Review all settings before execution
- **Cross-Platform** - Works on Windows, macOS, and Linux

## Architecture

```
stitch/gui/
├── __init__.py
├── main_window.py          # Main wizard window
├── validators.py           # Data validation functions
├── pages/
│   ├── hrs_data_page.py           # Step 1: HRS data selection
│   ├── residential_history_page.py # Step 2: Residential history
│   ├── contextual_data_page.py    # Step 3: Contextual data
│   ├── pipeline_config_page.py    # Step 4: Pipeline settings
│   └── execution_page.py          # Step 5: Run pipeline
└── widgets/
    ├── data_preview_table.py  # DataFrame preview widget
    └── file_picker.py         # File/directory selection widgets
```

## Troubleshooting

### "No module named 'PyQt6'"

Make sure PyQt6 is installed:
```bash
uv add PyQt6
```

### "Could not load the Qt platform plugin"

This usually occurs on headless servers. The GUI requires a display server (X11, Wayland, or similar). Use the CLI version (`link_lags.py`) on headless systems.

### "File not found" errors

Ensure all file paths are absolute or relative to the correct working directory. The GUI will validate file existence before allowing you to proceed.

## Development

### Adding New Validation Rules

Add validation functions to `validators.py`:

```python
def validate_custom_check(data) -> Tuple[bool, str]:
    """
    Custom validation function.
    
    Returns:
        (is_valid, error_message)
    """
    # Your validation logic
    return True, ""
```

### Adding New Pages

1. Create page class inheriting from `QWizardPage`
2. Implement `isComplete()` method for validation
3. Register fields with `registerField()`
4. Add to main wizard in `main_window.py`

## License

Same as the main STITCH package.

