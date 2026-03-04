# STITCH Test Fixtures

This directory contains pytest fixtures for testing the `ResidentialHistoryHRS` and `HRSInterviewData` classes from the `stitch.hrs` module.

## Overview

The test fixtures generate fake data that mimics the structure and patterns of real HRS (Health and Retirement Study) data, including:

- **Residential History Data**: Multiple rows per person showing residential moves over time
- **Survey/Interview Data**: One row per person with interview dates and demographic information
- **Varied Move Patterns**: People with no moves, single moves, and multiple moves
- **Realistic Data**: Properly formatted GEOIDs, chronological move dates, and consistent person IDs

## Files

### Test Modules
- `test_hrs_data.py` - Fixtures and tests for `ResidentialHistoryHRS` class
- `test_hrs_interview_data.py` - Fixtures and tests for `HRSInterviewData` class  
- `test_integration.py` - Integration tests combining both classes
- `test_end_to_end_linkage.py` - End-to-end workflow tests with batch and parallel processing
- `test_performance_no_residential_history.py` - Performance tests for static GEOID linkage
- `test_multicolumn_linkage.py` - Tests for linking multiple data columns simultaneously
- `conftest.py` - Shared fixtures for session-scoped testing

### Test Data
- `test_data/fake_residential_history.csv` - Generated residential history data (CSV format)
- `test_data/fake_survey_data.csv` - Generated survey/interview data (CSV format)

## Data Structure

### Residential History Data
Each person has multiple rows representing their residential history:

| Column | Description | Example Values |
|--------|-------------|----------------|
| `hhidpn` | Person ID | 10000001, 10000002, ... |
| `trmove_tr` | Move indicator | 999.0 (first tract), "1. move" (moves) |
| `mvyear` | Move year | 2012, 2015, 2018 (empty for first tract) |
| `mvmonth` | Move month | 1-12 (empty for first tract) |
| `GEOID2010` | Census tract GEOID | 11-digit zero-padded string |
| `year` | Survey year | 2010 (empty for moves) |

### Survey Data
Each person has one row with interview information:

| Column | Description | Example Values |
|--------|-------------|----------------|
| `hhidpn` | Person ID | 10000001, 10000002, ... |
| `bcdate` | Interview date | "2017-03-15", "2018-08-20" |
| `GEOID2010_2010` | Static GEOID for 2010 | 11-digit zero-padded string |
| `GEOID2010_2015` | Static GEOID for 2015 | 11-digit zero-padded string |
| `GEOID2010_2020` | Static GEOID for 2020 | 11-digit zero-padded string |
| `age` | Age at interview | 50-89 |
| `gender` | Gender | "Male", "Female" |
| `education` | Education level | "Less than HS", "HS", "Some College", "College+" |

## Usage

### Running Tests

```bash
# Run all HRS tests
pytest tests/test_hrs_data.py tests/test_hrs_interview_data.py tests/test_integration.py

# Run specific test file
pytest tests/test_hrs_data.py

# Run with verbose output
pytest tests/test_hrs_data.py -v
```

### Using Fixtures in Your Tests

```python
def test_my_function(residential_history_hrs, survey_data_hrs):
    """Test using the fixtures."""
    # residential_history_hrs is a ResidentialHistoryHRS instance
    # survey_data_hrs is an HRSInterviewData instance
    
    assert len(residential_history_hrs.df) > 0
    assert len(survey_data_hrs.df) > 0

def test_integration(survey_with_residential_history):
    """Test with linked data."""
    # survey_with_residential_history has residential history linked
    
    test_date = pd.Timestamp('2015-06-15')
    result = survey_with_residential_history.get_geoid_based_on_date(
        pd.Series([test_date])
    )
    assert len(result) == 1
```

### Converting CSV to Stata Format

The test data is generated as CSV files. To convert to Stata format (required by the HRS classes):

```python
import pandas as pd

# Convert residential history
df_res = pd.read_csv('tests/test_data/fake_residential_history.csv')
df_res.to_stata('tests/test_data/fake_residential_history.dta', write_index=False)

# Convert survey data  
df_survey = pd.read_csv('tests/test_data/fake_survey_data.csv')
df_survey.to_stata('tests/test_data/fake_survey_data.dta', write_index=False)
```

## Data Patterns

The generated data includes realistic patterns:

- **55 people total** with IDs 10000001-10000055
- **~20 people with no moves** (only first tract from 2010)
- **~20 people with 1 move** (first tract + one move between 2011-2019)
- **~15 people with 2-4 moves** (multiple moves with chronological ordering)
- **Interview dates** between 2015-2020
- **Realistic GEOIDs** with proper 11-digit formatting
- **Consistent person IDs** between residential and survey data

## Multi-Column Linkage Tests

The `test_multicolumn_linkage.py` module provides comprehensive tests for linking multiple data columns from contextual data sources simultaneously.

### What is Multi-Column Linkage?

Instead of running separate linkages for each data column (e.g., `tmax`, `tmin`, `humidity`), multi-column linkage allows you to link all columns at once:

```python
# Multi-column approach (efficient!)
weather_data = DailyMeasureDataDir(
    data_dir,
    data_col=["tmax", "tmin", "humidity"],  # All three at once
    measure_type=None,
)
```

### Test Coverage

- **Batch Processing**: Tests multi-column linkage with sequential processing
- **Parallel Processing**: Tests multi-column linkage with parallel workers
- **Consistency**: Verifies batch and parallel produce identical results
- **Correctness**: Compares multi-column vs separate single-column approaches
- **Residential History**: Tests multi-column linkage with participant moves

### Running Multi-Column Tests

```bash
# Run all multi-column tests
pytest tests/test_multicolumn_linkage.py -v -s

# Run specific categories
pytest tests/test_multicolumn_linkage.py -k "batch" -v -s
pytest tests/test_multicolumn_linkage.py -k "parallel" -v -s
pytest tests/test_multicolumn_linkage.py -k "consistency" -v -s
```

See [MULTICOLUMN_LINKAGE_TESTS.md](MULTICOLUMN_LINKAGE_TESTS.md) for detailed documentation.

## Dependencies

- `pytest` - Testing framework
- `pandas` - Data manipulation
- `numpy` - Numerical operations

## Notes

- The fixtures use `tmp_path` for temporary files during testing
- Persistent fixtures are available for session-scoped tests
- Data generation is deterministic within each run but varies between runs
- All GEOIDs are properly zero-padded to 11 digits
- Move dates are chronologically ordered for each person
- Interview dates are logically placed after residential moves
