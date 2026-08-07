"""
Validation utilities for the HRS Linkage Tool GUI.
"""

from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from ..daily_measure import parse_period_from_filename
from ..io_utils import read_data, get_file_format
from ..validation import (
    check_contextual_column_roles,
    check_generated_column_collisions,
    check_output_path_conflicts,
    check_residential_history_column_roles,
    check_survey_column_roles,
)


def validate_file_exists(path: str) -> bool:
    """Check if a file exists."""
    return Path(path).exists() and Path(path).is_file()


def validate_directory_exists(path: str) -> bool:
    """Check if a directory exists."""
    return Path(path).exists() and Path(path).is_dir()


def validate_stata_file(path: str) -> Tuple[bool, str]:
    """
    Validate that a file is a readable Stata file.

    Returns:
        (is_valid, error_message)
    """
    if not validate_file_exists(path):
        return False, f"File not found: {path}"

    try:
        file_format = get_file_format(Path(path))
        if file_format != "stata":
            return False, f"File is not a Stata file (.dta): {path}"

        # Try to read the file
        df = read_data(Path(path))
        if df.empty:
            return False, "File is empty"

        return True, ""
    except Exception as e:
        return False, f"Error reading Stata file: {str(e)}"


def validate_data_file(path: str) -> Tuple[bool, str]:
    """
    Validate that a file is a readable data file in any supported format.

    Supported formats: CSV, Stata (.dta), Parquet, Feather, Excel

    Returns:
        (is_valid, error_message)
    """
    if not validate_file_exists(path):
        return False, f"File not found: {path}"

    try:
        # Try to determine file format
        file_format = get_file_format(Path(path))

        # Try to read a preview of the file
        df, error_msg = load_preview_data(path, n_rows=1)

        if df is None:
            return False, error_msg

        if df.empty:
            return False, "File is empty"

        return True, ""
    except ValueError as e:
        # get_file_format raises ValueError for unsupported formats
        return False, f"Unsupported file format: {str(e)}"
    except Exception as e:
        return False, f"Error reading file: {str(e)}"


def validate_date_column(df: pd.DataFrame, col: str) -> Tuple[bool, str]:
    """
    Validate that a column exists and can be interpreted as dates.

    Returns:
        (is_valid, error_message)
    """
    if col not in df.columns:
        return False, f"Column '{col}' not found in data"

    try:
        # Try to convert to datetime
        pd.to_datetime(df[col].dropna().head(100), format="mixed", errors="coerce")
        return True, ""
    except Exception as e:
        return False, f"Column '{col}' cannot be interpreted as dates: {str(e)}"


def _as_result(problems: List[str]) -> Tuple[bool, str]:
    """Adapt a ``stitch.validation`` problem list to this module's contract."""
    if not problems:
        return True, ""
    return False, " ".join(problems)


def validate_survey_column_roles(
    date_col: Optional[str],
    id_col: Optional[str],
    geoid_col: Optional[str],
) -> Tuple[bool, str]:
    """
    Validate that the survey's date, ID and GEOID columns are distinct.

    Returns:
        (is_valid, error_message)
    """
    return _as_result(check_survey_column_roles(date_col, id_col, geoid_col))


def validate_residential_history_column_roles(
    id_col: Optional[str],
    date_col: Optional[str],
    geoid_col: Optional[str],
) -> Tuple[bool, str]:
    """
    Validate that the residential history's three columns are distinct.

    Returns:
        (is_valid, error_message)
    """
    return _as_result(
        check_residential_history_column_roles(id_col, date_col, geoid_col)
    )


def validate_contextual_column_roles(
    date_col: Optional[str],
    geoid_col: Optional[str],
    data_cols: Optional[List[str]],
) -> Tuple[bool, str]:
    """
    Validate that the contextual date, GEOID and measure columns are distinct.

    Returns:
        (is_valid, error_message)
    """
    return _as_result(check_contextual_column_roles(date_col, geoid_col, data_cols))


def validate_output_path_conflicts(
    save_dir: str,
    output_name: str,
    survey_path: Optional[str] = None,
    residential_history_path: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Validate that the output file is not one of the input files.

    Returns:
        (is_valid, error_message)
    """
    if not save_dir or not output_name:
        return True, ""
    return _as_result(
        check_output_path_conflicts(
            Path(save_dir) / Path(output_name), survey_path, residential_history_path
        )
    )


def validate_generated_column_names(
    survey_columns: List[str],
    *,
    date_col: Optional[str],
    geoid_col: Optional[str],
    data_cols: Optional[List[str]],
    start_lag: int,
    end_lag: int,
    linkage_resolution,
    post_lag_average: bool = False,
) -> Tuple[bool, str]:
    """
    Validate that the survey data does not already carry the columns this
    configuration would create (i.e. it is not a previous STITCH output).

    Returns:
        (is_valid, error_message)
    """
    if not survey_columns or end_lag < start_lag:
        return True, ""
    return _as_result(
        check_generated_column_collisions(
            survey_columns,
            date_col=date_col,
            geoid_col=geoid_col,
            data_cols=data_cols,
            lags=range(start_lag, end_lag + 1),
            resolution=linkage_resolution,
            post_lag_average=post_lag_average,
            start_lag=start_lag,
            max_lag=end_lag,
        )
    )


def validate_contextual_directory(
    dir_path: str,
    measure_type: Optional[str] = None,
    file_extension: Optional[str] = None,
) -> Tuple[bool, List[str], str]:
    """
    Validate a contextual data directory and extract years from filenames.

    Args:
        dir_path: Path to directory
        measure_type: Optional measure type to filter files
        file_extension: Optional file extension to filter files

    Returns:
        (is_valid, list_of_years, error_message)
    """
    if not validate_directory_exists(dir_path):
        return False, [], f"Directory not found: {dir_path}"

    dirpath = Path(dir_path)

    # Determine which file extensions to search for
    if file_extension is None:
        supported_extensions = [
            ".csv",
            ".dta",
            ".parquet",
            ".pq",
            ".feather",
            ".xlsx",
            ".xls",
        ]
    else:
        supported_extensions = [file_extension]

    # Collect files
    all_files = []
    for ext in supported_extensions:
        all_files.extend(dirpath.glob(f"*{ext}"))

    # Filter by measure type if specified
    if measure_type is not None:
        files = [f for f in all_files if measure_type in f.name]
    else:
        files = all_files

    if not files:
        msg = f"No files found in directory"
        if measure_type:
            msg += f" matching measure type '{measure_type}'"
        if file_extension:
            msg += f" with extension '{file_extension}'"
        return False, [], msg

    # Extract years using the same rules as DailyMeasureDataDir, so the years
    # previewed here match the years the run will actually discover.
    years = []
    for f in files:
        try:
            year, _month = parse_period_from_filename(f.name, measure_type)
        except ValueError:
            continue
        years.append(year)

    years = sorted(set(years))

    if not years:
        return False, [], "No year information (4-digit numbers) found in filenames"

    return True, years, ""


def check_column_consistency(file_paths: List[Path]) -> Tuple[bool, str]:
    """
    Check that all files have consistent column names.

    Returns:
        (is_valid, error_message)
    """
    if not file_paths:
        return False, "No files provided"

    try:
        # Read just the headers
        all_columns = []
        for fpath in file_paths:
            file_format = get_file_format(fpath)

            if file_format == "csv":
                header = pd.read_csv(fpath, nrows=0)
            elif file_format == "stata":
                header = pd.read_stata(fpath, chunksize=1).__next__().iloc[:0]
            elif file_format in ("parquet", "feather"):
                df = read_data(fpath)
                header = df.iloc[:0]
            elif file_format == "excel":
                header = pd.read_excel(fpath, nrows=0)
            else:
                df = read_data(fpath)
                header = df.iloc[:0]

            all_columns.append(set(header.columns))

        # Check consistency
        first_cols = all_columns[0]
        for i, cols in enumerate(all_columns[1:], 1):
            if cols != first_cols:
                missing_in_current = first_cols - cols
                extra_in_current = cols - first_cols
                msg = f"Column mismatch between {file_paths[0].name} and {file_paths[i].name}"
                if missing_in_current:
                    msg += f"\n  Missing: {missing_in_current}"
                if extra_in_current:
                    msg += f"\n  Extra: {extra_in_current}"
                return False, msg

        return True, ""
    except Exception as e:
        return False, f"Error checking column consistency: {str(e)}"


def load_preview_data(
    file_path: str, n_rows: int = 5
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Load a preview of data from a file.

    Returns:
        (dataframe, error_message)
    """
    try:
        path = Path(file_path)
        file_format = get_file_format(path)

        if file_format == "csv":
            df = pd.read_csv(path, nrows=n_rows)
        elif file_format == "stata":
            df = pd.read_stata(path, chunksize=n_rows).__next__()
        elif file_format in ("parquet", "feather"):
            full_df = read_data(path)
            df = full_df.head(n_rows)
        elif file_format == "excel":
            df = pd.read_excel(path, nrows=n_rows)
        else:
            full_df = read_data(path)
            df = full_df.head(n_rows)

        return df, ""
    except Exception as e:
        return None, f"Error loading preview: {str(e)}"
