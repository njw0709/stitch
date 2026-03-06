"""
Flexible data I/O utilities for reading and writing various file formats.

This module provides format-agnostic data reading and writing functions that
automatically detect the file format from the file extension and use the
appropriate pandas I/O method.

Supported formats:
- CSV (.csv)
- Stata (.dta)
- Parquet (.parquet, .pq)
- Feather (.feather)
- Excel (.xlsx, .xls)
"""

from __future__ import annotations
import inspect
import re
import warnings
from pathlib import Path
from typing import Any, Dict, Optional, Union

import numpy as np
import pandas as pd


class GeoidTruncationWarning(UserWarning):
    """Raised when a GEOID value is truncated to the target number of digits."""


# ------------------------------------------------------------------
# GEOID normalization — private helpers
# ------------------------------------------------------------------

def _clean_geoid(val) -> str:
    """Strip a raw GEOID value down to its digit-only string. Returns ``""`` for missing."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, (int, float)) and not pd.isna(val):
        try:
            val = int(val)
        except (OverflowError, ValueError):
            pass
    s = re.sub(r"\D", "", str(val))
    if not s or s in ("nan", "None", "<NA>"):
        return ""
    return s


def _format_geoid(digits: str, treatment: str, n_digits: int, numeric_type: str) -> str:
    """Format a cleaned digit string according to *treatment*. Always returns ``str``.

    When *treatment* is ``"code"`` and *n_digits* is ``0`` (or negative), no
    zero-padding is applied — the cleaned digit string is returned as-is.
    """
    if not digits:
        return ""
    if treatment == "code":
        if n_digits <= 0:
            return digits
        if len(digits) > n_digits:
            digits = digits[:n_digits]
        return digits.zfill(n_digits)
    # numeric — cast then stringify
    try:
        if numeric_type == "float":
            return str(float(digits))
        return str(int(digits))
    except (ValueError, OverflowError):
        return ""


# ------------------------------------------------------------------
# GEOID normalization — public API
# ------------------------------------------------------------------

def normalize_geoid_value_for_processing(
    val,
    treatment: str = "code",
    n_digits: int = 11,
    numeric_type: str = "int",
) -> str:
    """Normalize a single GEOID value to a string (for matching / intermediate use)."""
    return _format_geoid(_clean_geoid(val), treatment, n_digits, numeric_type)


def normalize_geoid_for_processing(
    series: pd.Series,
    treatment: str = "code",
    n_digits: int = 11,
    numeric_type: str = "int",
) -> pd.Series:
    """
    Normalize a GEOID Series and return **strings** suitable for intermediate
    matching (``isin``, merge keys, etc.).

    * ``treatment="code"``: zero-padded string of *n_digits* digits.
    * ``treatment="numeric"``: cast to int/float first, then ``str()`` so all
      sources share the same plain-string representation.
    """
    if treatment == "code" and n_digits > 0:
        digits_only = series.astype(str).str.replace(r"\D", "", regex=True)
        digits_only = digits_only.replace({"nan": "", "None": "", "<NA>": ""})
        if (digits_only.str.len() > n_digits).any():
            warnings.warn(
                f"Some GEOID values had more than {n_digits} digits after stripping "
                "non-digits and were truncated (e.g. float representation artifacts).",
                GeoidTruncationWarning,
                stacklevel=2,
            )
    return series.apply(
        lambda v: _format_geoid(_clean_geoid(v), treatment, n_digits, numeric_type)
    )


def apply_geoid_normalization(
    series: pd.Series,
    treatment: str = "code",
    n_digits: int = 11,
    numeric_type: str = "int",
) -> pd.Series:
    """
    Produce the **final output** format for a GEOID Series.

    * ``treatment="code"``: zero-padded *n_digits*-digit string.
    * ``treatment="numeric"``: native ``Int64`` or ``float`` column.
    """
    as_str = normalize_geoid_for_processing(series, treatment, n_digits, numeric_type)
    if treatment == "numeric":
        numeric = pd.to_numeric(as_str.replace("", pd.NA), errors="coerce")
        if numeric_type == "float":
            return numeric.astype(float)
        return numeric.astype("Int64")
    return as_str


def _filter_kwargs(func: callable, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter kwargs to only include parameters that are valid for the given function.

    Parameters
    ----------
    func : callable
        The function to filter kwargs for.
    kwargs : Dict[str, Any]
        The keyword arguments to filter.

    Returns
    -------
    Dict[str, Any]
        Filtered kwargs containing only valid parameters for the function.
    """
    try:
        sig = inspect.signature(func)
        valid_params = set(sig.parameters.keys())

        # If function has **kwargs parameter, accept all kwargs
        for param in sig.parameters.values():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                return kwargs

        # Otherwise, filter to only valid parameters
        return {k: v for k, v in kwargs.items() if k in valid_params}
    except Exception:
        # If we can't inspect the function, pass all kwargs
        return kwargs


# Helper: sanitize DataFrame for CSV/Excel/Stata exports
def _sanitize_for_tabular(input_df: pd.DataFrame, mode: str = "string") -> pd.DataFrame:
    """
    Sanitize DataFrame for export to tabular formats.

    Parameters
    ----------
    input_df : pd.DataFrame
        Input DataFrame to sanitize
    mode : str, default "string"
        - "string": Convert all values to strings (for CSV/Excel)
        - "preserve": Keep numeric types, only fix problematic types (for Stata)
    """
    sanitized = input_df.copy()

    # Handle categoricals early to avoid downstream surprises
    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_categorical_dtype(col):
            sanitized[col_name] = col.astype("string").astype(object)

    # Datetime handling
    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_datetime64_any_dtype(col):
            series = col
            try:
                # If timezone-aware, drop tz info (local naive)
                if getattr(series.dt, "tz", None) is not None:
                    series = series.dt.tz_localize(None)
            except Exception:
                # Fallback: attempt to convert to datetime then drop tz
                series = pd.to_datetime(series, errors="coerce")
                series = series.dt.tz_localize(None)

            if mode == "preserve":
                # Keep as datetime for Stata (Stata supports datetime)
                sanitized[col_name] = series
            else:
                # Convert to ISO string for CSV/Excel
                sanitized[col_name] = series.dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Timedelta handling
    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_timedelta64_dtype(col):
            total_seconds = col.dt.total_seconds()
            if mode == "preserve":
                # Keep as numeric for Stata
                as_int = total_seconds % 1 == 0
                sanitized[col_name] = np.where(
                    as_int, total_seconds.astype("Int64"), total_seconds.astype(float)
                )
            else:
                # Convert to string for CSV/Excel
                as_int = total_seconds % 1 == 0
                sanitized[col_name] = np.where(
                    as_int,
                    total_seconds.astype("Int64").astype(object),
                    total_seconds.astype(float),
                ).astype(str)

    # Booleans handling
    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_bool_dtype(col):
            if mode == "preserve":
                # Keep as int for Stata (0/1 numeric)
                sanitized[col_name] = col.astype(int)
            else:
                # Convert to string for CSV/Excel
                sanitized[col_name] = col.astype(int).astype(str)

    # Object columns: coerce element-wise
    def _coerce_object_value(value: Any, as_string: bool = True) -> Any:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        # Keep primitives that are already safe
        if isinstance(value, (str, int, float)):
            return value
        if isinstance(value, (np.integer, np.floating)):
            return value.item()
        if isinstance(value, (bool, np.bool_)):
            return (
                "1" if bool(value) else "0" if as_string else (1 if bool(value) else 0)
            )
        if isinstance(value, (bytes, bytearray)):
            try:
                return bytes(value).decode("utf-8", errors="replace")
            except Exception:
                return str(value)
        # Lists/dicts/numpy arrays -> repr()
        if isinstance(value, (list, dict, tuple, set, np.ndarray)):
            return repr(value)
        # Fallback to str()
        return str(value)

    for col_name in sanitized.columns:
        col = sanitized[col_name]
        if pd.api.types.is_object_dtype(col):
            as_string = mode != "preserve"
            sanitized[col_name] = col.map(
                lambda v: _coerce_object_value(v, as_string=as_string)
            )

    return sanitized


def read_data(
    file_path: Union[str, Path],
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Read data from a file, automatically detecting the format from the file extension.

    Supports CSV, Stata (.dta), Parquet, Feather, and Excel formats.

    Parameters
    ----------
    file_path : str or Path
        Path to the file to read.
    **kwargs : Any
        Additional keyword arguments to pass to the underlying pandas read function.
        Common examples:
        - usecols: List of columns to read (automatically mapped to 'columns' for
          Stata, Parquet, and Feather formats)
        - dtype: Dictionary of column dtypes
        - parse_dates: List of columns to parse as dates
        - chunksize: For CSV, return an iterator (not supported for other formats)

        Note: Unsupported kwargs for each format are automatically filtered out.

    Returns
    -------
    pd.DataFrame
        The loaded data as a pandas DataFrame.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file extension is not supported.

    Examples
    --------
    >>> # Read a Stata file
    >>> df = read_data("data/survey.dta")

    >>> # Read a CSV with specific columns
    >>> df = read_data("data/measures.csv", usecols=["Date", "GEOID10", "Tmax"])

    >>> # Read a Parquet file with specific columns (usecols works here too!)
    >>> df = read_data("data/results.parquet", usecols=["id", "value"])
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get file extension (lowercase, without dot)
    ext = file_path.suffix.lower().lstrip(".")

    # Map extension to pandas read function
    if ext == "csv":
        filtered_kwargs = _filter_kwargs(pd.read_csv, kwargs)
        return pd.read_csv(file_path, **filtered_kwargs)
    elif ext == "dta":
        # Stata uses 'columns' instead of 'usecols'
        mapped_kwargs = kwargs.copy()
        if "usecols" in mapped_kwargs and "columns" not in mapped_kwargs:
            mapped_kwargs["columns"] = mapped_kwargs.pop("usecols")
        filtered_kwargs = _filter_kwargs(pd.read_stata, mapped_kwargs)
        return pd.read_stata(file_path, **filtered_kwargs)
    elif ext in ("parquet", "pq"):
        # Parquet uses 'columns' instead of 'usecols'
        mapped_kwargs = kwargs.copy()
        if "usecols" in mapped_kwargs and "columns" not in mapped_kwargs:
            mapped_kwargs["columns"] = mapped_kwargs.pop("usecols")
        filtered_kwargs = _filter_kwargs(pd.read_parquet, mapped_kwargs)
        return pd.read_parquet(file_path, **filtered_kwargs)
    elif ext == "feather":
        # Feather uses 'columns' instead of 'usecols'
        mapped_kwargs = kwargs.copy()
        if "usecols" in mapped_kwargs and "columns" not in mapped_kwargs:
            mapped_kwargs["columns"] = mapped_kwargs.pop("usecols")
        filtered_kwargs = _filter_kwargs(pd.read_feather, mapped_kwargs)
        return pd.read_feather(file_path, **filtered_kwargs)
    elif ext in ("xlsx", "xls"):
        filtered_kwargs = _filter_kwargs(pd.read_excel, kwargs)
        return pd.read_excel(file_path, **filtered_kwargs)
    else:
        raise ValueError(
            f"Unsupported file format: '.{ext}'. "
            f"Supported formats: .csv, .dta, .parquet, .pq, .feather, .xlsx, .xls"
        )


def write_data(
    df: pd.DataFrame,
    file_path: Union[str, Path],
    **kwargs: Any,
) -> None:
    """
    Write DataFrame to a file, automatically detecting the format from the file extension.

    Supports CSV, Stata (.dta), Parquet, Feather, and Excel formats.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to write.
    file_path : str or Path
        Path to the output file.
    **kwargs : Any
        Additional keyword arguments to pass to the underlying pandas write function.
        Common examples:
        - index: Whether to write row index (default behavior varies by format)
        - compression: Compression type for supported formats

    Raises
    ------
    ValueError
        If the file extension is not supported.

    Notes
    -----
    - For Stata files, the index is not written by default (Stata doesn't support it).
    - For CSV files, index writing depends on the kwargs (default is True in pandas).
    - For Parquet and Feather, index writing depends on kwargs.

    Examples
    --------
    >>> # Write to Stata
    >>> write_data(df, "output/results.dta")

    >>> # Write to CSV without index
    >>> write_data(df, "output/results.csv", index=False)

    >>> # Write to Parquet with compression
    >>> write_data(df, "output/results.parquet", compression="gzip")
    """
    file_path = Path(file_path)

    # Create parent directory if it doesn't exist
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Get file extension (lowercase, without dot)
    ext = file_path.suffix.lower().lstrip(".")

    # Prepare a defensive copy for sanitation across all formats
    out_df = df.copy()

    # Map extension to pandas write function
    if ext == "csv":
        # Excel-first friendly CSV
        sanitized_df = _sanitize_for_tabular(out_df)
        write_kwargs = {"index": False, "encoding": "utf-8-sig", "na_rep": ""}
        write_kwargs.update(kwargs)
        sanitized_df.to_csv(file_path, **write_kwargs)
    elif ext == "dta":
        # Stata uses write_index (not index); default to False for consistency
        write_kwargs = {k: v for k, v in kwargs.items() if k != "index"}
        write_kwargs["write_index"] = kwargs.get("index", False)

        # Apply sanitation for Stata with type preservation
        sanitized_df = _sanitize_for_tabular(out_df, mode="preserve")

        # Additional Stata-specific pass: ensure all object columns are truly string-or-None
        for name in sanitized_df.columns:
            col = sanitized_df[name]
            if pd.api.types.is_object_dtype(col):
                # Convert all values to str (or None for missing)
                sanitized_df[name] = col.astype(object).where(~pd.isna(col), None)

        sanitized_df.to_stata(file_path, **write_kwargs)
    elif ext in ("parquet", "pq"):
        write_kwargs = {"index": False}
        write_kwargs.update(kwargs)
        out_df.to_parquet(file_path, **write_kwargs)
    elif ext == "feather":
        # Feather doesn't support index param; reset index to avoid writing it as column
        write_kwargs = {k: v for k, v in kwargs.items() if k != "index"}
        if kwargs.get("index", True):
            out_df = out_df.reset_index(drop=True)
        out_df.to_feather(file_path, **write_kwargs)
    elif ext in ("xlsx", "xls"):
        # Apply the same sanitation for Excel to ensure consistent, readable values
        sanitized_df = _sanitize_for_tabular(out_df)
        write_kwargs = {"index": False}
        write_kwargs.update(kwargs)
        sanitized_df.to_excel(file_path, **write_kwargs)
    else:
        raise ValueError(
            f"Unsupported file format: '.{ext}'. "
            f"Supported formats: .csv, .dta, .parquet, .pq, .feather, .xlsx, .xls"
        )


def get_file_format(file_path: Union[str, Path]) -> str:
    """
    Get the file format from a file path.

    Parameters
    ----------
    file_path : str or Path
        Path to the file.

    Returns
    -------
    str
        The file format (e.g., "csv", "dta", "parquet", "feather", "excel").

    Examples
    --------
    >>> get_file_format("data/file.csv")
    'csv'
    >>> get_file_format("data/file.parquet")
    'parquet'
    """
    file_path = Path(file_path)
    ext = file_path.suffix.lower().lstrip(".")

    if ext == "csv":
        return "csv"
    elif ext == "dta":
        return "stata"
    elif ext in ("parquet", "pq"):
        return "parquet"
    elif ext == "feather":
        return "feather"
    elif ext in ("xlsx", "xls"):
        return "excel"
    else:
        raise ValueError(
            f"Unsupported file format: '.{ext}'. "
            f"Supported formats: .csv, .dta, .parquet, .pq, .feather, .xlsx, .xls"
        )
