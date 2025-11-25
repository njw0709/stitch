from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional, Union
import pandas as pd
import re

from .io_utils import read_data, get_file_format

# Map file prefix to column name
FILENAME_TO_VARNAME_DICT = {
    "tmmx": "Tmax",
    "rmin": "Rmin",
    "pm25": "pm25",
    "ozone": "o3",
    "heat_index": "HeatIndex",
}


class DailyMeasureData:
    """
    Wrapper for a single daily measure CSV file (e.g., Tmax, PM2.5, HeatIndex).
    Can read both 'long' and 'wide' formats and reshape if needed.
    """

    YEAR_PATTERN = re.compile(r"(\d{4})")

    def __init__(
        self,
        file_path: Union[str, Path],
        data_col: Union[str, List[str], None] = None,
        measure_type: Optional[str] = None,
        read_dtype: str = "float32",
        expected_format: str = "long",
        current_format: str = "long",
        geoid_col: str = "GEOID10",
        date_col: str = "Date",
        rename_col: Optional[dict] = None,
        geoid_filter: Optional[set] = None,
    ):
        """
        Initialize a DailyMeasureData object by reading and processing a single
        daily measure CSV file.

        This class supports both **long** format (one row per GEOID–date combination)
        and **wide** format (dates as rows, GEOIDs as columns). Wide-format files are
        automatically reshaped to long format if `expected_format="long"`.

        Parameters
        ----------
        file_path : str or Path
            Path to the daily measure CSV file (e.g., "heat_2010.csv").

        data_col : str, optional
            Name of the column containing the daily measure values (e.g., "HeatIndex",
            "Tmax", "pm25"). If not provided, it will be inferred from `measure_type`
            using the global mapping `FILENAME_TO_VARNAME_DICT`.

        measure_type : str, optional
            Shorthand identifier for the type of daily measure (e.g., "heat", "tmmx",
            "pm25"). Used to look up the appropriate `data_col` name if `data_col`
            is not explicitly provided. Either `data_col` or `measure_type` must be
            provided.

        read_dtype : str, default "float32"
            Numeric dtype to use when reading the data column. Using "float32"
            typically reduces memory footprint with minimal precision loss.

        expected_format : {"long", "wide"}, default "long"
            Expected format for downstream processing. If the file is wide but
            `expected_format="long"`, the data will be melted into long format.

        geoid_col : str, default "GEOID10"
            Name of the column that stores geographic identifiers. In wide format,
            this name will be used as the `var_name` when melting columns.

        date_col : str, default "Date"
            Name of the column containing date information. Dates are parsed into
            pandas `datetime64[ns]` dtype.

        rename_col : dict, optional
            Optional dictionary for renaming columns **before** processing, typically
            used to handle inconsistent column names across years (e.g.,
            `{"HeatIndex_2010": "HeatIndex"}`).

        geoid_filter : set, optional
            Optional set of GEOID strings to filter the data to. If provided, only
            rows with GEOIDs in this set will be retained. This dramatically reduces
            memory usage when you only need data for a small subset of GEOIDs.

        Raises
        ------
        FileNotFoundError
            If the file does not exist at `file_path`.

        ValueError
            If neither `data_col` nor `measure_type` is provided.
        ValueError
            If the inferred or specified `data_col` is not found in the file after applying `rename_col`.
        ValueError
            If the file format cannot be parsed (e.g., malformed CSV, missing required columns).

        Notes
        -----
        - Only the header is initially read to detect column names and format, which
          makes it efficient for large files.
        - For wide-format files, all columns are read so that they can be melted to
          long format. For long-format files, only the relevant columns are read.
        - The GEOID column is zero-padded to 11 characters to standardize identifiers.
        - After initialization, the processed data is stored in `self.df` as a
          pandas DataFrame with standardized columns: `[date_col, geoid_col, data_col]`.

        Examples
        --------
        >>> data = DailyMeasureData("data/heat_2010.csv", measure_type="heat")
        >>> data.df.head()
                 Date      GEOID10  HeatIndex
        0  2010-01-01  01001020100       45.2
        1  2010-01-01  01001020200       43.8
        2  2010-01-01  01001020300       44.1
        """
        self.filepath = Path(file_path)
        self.date_col = date_col
        self.geoid_col = geoid_col
        self.read_dtype = read_dtype
        self.format = current_format
        self.expected_format = expected_format
        self.rename_col = rename_col
        self.geoid_filter = geoid_filter

        # Infer data_col from measure_type if not explicitly passed
        if data_col is None:
            if measure_type is None:
                raise ValueError("Either `data_col` or `measure_type` must be provided")
            data_col = FILENAME_TO_VARNAME_DICT[measure_type]

        # Normalize data_col to list
        if isinstance(data_col, str):
            data_col = [data_col]
        self.data_col = data_col

        # --- 1. Inspect header and apply rename if needed ---
        header = self._read_header()
        self.columns = header.columns.tolist()

        # Check if all target data_cols are in columns after renaming
        missing_cols = [col for col in self.data_col if col not in self.columns]
        if missing_cols:
            raise ValueError(
                f"Column(s) {missing_cols} not found in file: {self.filepath.name}\n"
                f"Available columns: {self.columns}"
            )

        # --- 2. Load data ---
        # Detect file format
        file_format = get_file_format(self.filepath)

        # For non-CSV formats, use the flexible reader
        if file_format != "csv":
            dtype_dict = None
            if self.read_dtype != "float64":
                # Create dtype dict for all data columns
                dtype_dict = {col: self.read_dtype for col in self.data_col}

            if self.format == "long":
                usecols = [self.date_col, self.geoid_col] + self.data_col
            else:
                usecols = None  # need all columns to melt later

            # Read the entire file using flexible reader
            df = read_data(self.filepath, usecols=usecols, dtype=dtype_dict)
            df = self._apply_rename(df)

            # --- 3. Reshape if wide ---
            if self.format == "wide" and self.expected_format == "long":
                df = df.melt(
                    id_vars=[self.date_col],
                    var_name=self.geoid_col,
                    value_name=self.data_col,
                )

            # --- 4. Format columns ---
            if df[self.date_col].dtype != "datetime64[ns]":
                df[self.date_col] = pd.to_datetime(df[self.date_col], errors="coerce")
            df[self.geoid_col] = df[self.geoid_col].astype(str).str.zfill(11)

            # --- 5. Filter by GEOID if provided ---
            if self.geoid_filter is not None:
                before_count = len(df)
                df = df[df[self.geoid_col].isin(self.geoid_filter)]
                after_count = len(df)
                print(
                    f"  Filtered to {after_count:,} rows ({len(self.geoid_filter)} GEOIDs) from {before_count:,} rows"
                )

            self.df = df
            # Check for duplicate date-geoid pairs
            self._check_unique_date_geoid_pairs()

        # For CSV files, use optimized reading logic
        else:
            dtype_dict = None
            if self.read_dtype != "float64":
                # Create dtype dict for all data columns
                dtype_dict = {col: self.read_dtype for col in self.data_col}

            if self.format == "long":
                usecols = [self.date_col, self.geoid_col] + self.data_col
            else:
                usecols = None  # need all columns to melt later

            # Use chunked reading with filtering for long format when geoid_filter is provided
            if self.format == "long" and self.geoid_filter is not None:
                print(
                    f"  Reading in chunks and filtering to {len(self.geoid_filter)} GEOIDs..."
                )
                chunks = []

                # Try pyarrow engine first (faster), fall back to default C engine
                try:
                    csv_reader = pd.read_csv(
                        self.filepath,
                        dtype=dtype_dict,
                        usecols=usecols,
                        chunksize=1_000_000,
                        engine="pyarrow",
                    )
                except (ImportError, ValueError, TypeError):
                    # pyarrow not available or doesn't support chunksize
                    csv_reader = pd.read_csv(
                        self.filepath,
                        dtype=dtype_dict,
                        usecols=usecols,
                        chunksize=1_000_000,
                    )

                total_before = 0
                for chunk in csv_reader:
                    chunk = self._apply_rename(chunk)
                    # Format GEOID for filtering
                    chunk[self.geoid_col] = (
                        chunk[self.geoid_col].astype(str).str.zfill(11)
                    )
                    # Filter immediately - discard unwanted data early
                    total_before += len(chunk)
                    filtered = chunk[chunk[self.geoid_col].isin(self.geoid_filter)]
                    if len(filtered) > 0:
                        chunks.append(filtered)

                df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

                # Parse dates after filtering (faster on smaller data)
                if (
                    self.date_col in df.columns
                    and df[self.date_col].dtype != "datetime64[ns]"
                ):
                    df[self.date_col] = pd.to_datetime(
                        df[self.date_col], errors="coerce"
                    )

                print(
                    f"  Filtered to {len(df):,} rows ({len(self.geoid_filter)} GEOIDs) from {total_before:,} rows"
                )

                self.df = df
                # Check for duplicate date-geoid pairs
                self._check_unique_date_geoid_pairs()
            else:
                # Original full-load path for wide format or no filtering
                # Try pyarrow engine for speed, fall back to default
                try:
                    df = pd.read_csv(
                        self.filepath,
                        dtype=dtype_dict,
                        usecols=usecols,
                        parse_dates=(
                            [self.date_col] if self.date_col in self.columns else None
                        ),
                        engine="pyarrow",
                    )
                except (ImportError, ValueError, TypeError):
                    df = pd.read_csv(
                        self.filepath,
                        dtype=dtype_dict,
                        usecols=usecols,
                        parse_dates=(
                            [self.date_col] if self.date_col in self.columns else None
                        ),
                    )

                df = self._apply_rename(df)

                # --- 3. Reshape if wide ---
                if self.format == "wide" and self.expected_format == "long":
                    df = df.melt(
                        id_vars=[self.date_col],
                        var_name=self.geoid_col,
                        value_name=self.data_col,
                    )

                # --- 4. Format columns ---
                if df[self.date_col].dtype != "datetime64[ns]":
                    df[self.date_col] = pd.to_datetime(
                        df[self.date_col], errors="coerce"
                    )
                df[self.geoid_col] = df[self.geoid_col].astype(str).str.zfill(11)

                # --- 5. Filter by GEOID if provided (for wide format or non-chunked reads) ---
                if self.geoid_filter is not None:
                    before_count = len(df)
                    df = df[df[self.geoid_col].isin(self.geoid_filter)]
                    after_count = len(df)
                    print(
                        f"  Filtered to {after_count:,} rows ({len(self.geoid_filter)} GEOIDs) from {before_count:,} rows"
                    )

                self.df = df

        # --- 6. Check for duplicate date-geoid pairs ---
        self._check_unique_date_geoid_pairs()

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def _apply_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply column renaming if a rename dict is provided."""
        if self.rename_col:
            return df.rename(columns=self.rename_col)
        return df

    def _read_header(self) -> pd.DataFrame:
        """Read header row and apply rename to check columns."""
        file_format = get_file_format(self.filepath)

        if file_format == "csv":
            header = pd.read_csv(self.filepath, nrows=0)
        elif file_format == "stata":
            # For Stata, we need to read the whole file but can use iterator to get columns
            header = pd.read_stata(self.filepath, chunksize=1).__next__().iloc[:0]
        elif file_format in ("parquet", "feather"):
            # For Parquet/Feather, read columns only (no rows)
            df = read_data(self.filepath)
            header = df.iloc[:0]
        elif file_format == "excel":
            header = pd.read_excel(self.filepath, nrows=0)
        else:
            # Fallback: try to read with read_data and take no rows
            df = read_data(self.filepath)
            header = df.iloc[:0]

        return self._apply_rename(header)

    def _check_unique_date_geoid_pairs(self) -> None:
        """
        Verify that (date, GEOID) pairs are unique in the loaded data.

        Raises
        ------
        ValueError
            If duplicate (date, GEOID) pairs are found, with details about
            the first 10 duplicates.
        """
        if self.df.empty:
            return

        # Check for duplicates on the combination of date and geoid columns
        duplicates = self.df[
            self.df.duplicated(subset=[self.date_col, self.geoid_col], keep=False)
        ]

        if not duplicates.empty:
            n_duplicates = len(duplicates)
            # Get unique duplicate pairs for reporting
            duplicate_pairs = duplicates[
                [self.date_col, self.geoid_col]
            ].drop_duplicates()
            n_unique_pairs = len(duplicate_pairs)

            # Show first 10 duplicate pairs as examples
            sample_duplicates = duplicate_pairs.head(10)
            sample_str = "\n".join(
                f"  - Date: {row[self.date_col]}, GEOID: {row[self.geoid_col]}"
                for _, row in sample_duplicates.iterrows()
            )

            raise ValueError(
                f"Found {n_duplicates:,} duplicate rows for {n_unique_pairs:,} unique "
                f"(date, GEOID) pairs in file: {self.filepath.name}\n"
                f"Each (date, GEOID) combination should appear exactly once.\n"
                f"First {min(10, n_unique_pairs)} duplicate pairs:\n{sample_str}"
            )

    def __repr__(self):
        return f"DailyMeasureData({self.filepath.name}, col={self.data_col}, format={self.format}, rows={len(self.df)})"

    def head(self, n=5):
        return self.df.head(n)


class DailyMeasureDataDir:
    """
    Directory wrapper that lazy-loads yearly DailyMeasureData files
    for a given measure type, validating column presence similarly to
    DailyMeasureData itself.
    """

    YEAR_PATTERN = re.compile(r"(\d{4})")

    def __init__(
        self,
        dir_name: Union[str, Path],
        measure_type: Optional[str] = None,
        data_col: Union[str, List[str], None] = None,
        geoid_col: str = "GEOID10",
        date_col: str = "Date",
        rename_col_dict: Optional[dict] = None,
        read_dtype: str = "float32",
        geoid_filter: Optional[set] = None,
        file_extension: Optional[str] = None,
    ):
        """
        Initialize a directory-level wrapper for daily measure files spanning multiple years.

        This class manages:
        - Locating all yearly data files for a given `measure_type` (or all files if `measure_type` is None),
        - Validating that each file contains the expected `data_col` (after applying any year-specific renaming),
        - Lazy-loading and caching of `DailyMeasureData` objects by year.

        Supports CSV, Stata (.dta), Parquet, Feather, and Excel formats.

        Parameters
        ----------
        dir_name : str or Path
            Directory containing yearly data files. Each file should typically correspond
            to one year (e.g., "heat_2010.csv", "heat_2011.dta", "heat_2012.parquet", ...).

        measure_type : str, optional
            Measurement type identifier (e.g., "tmmx", "heat", "pm25").
            File names must contain this measurement type as a substring in order to be included.
            If `data_col` is not provided, it will be inferred using the global mapping
            `FILENAME_TO_VARNAME_DICT[measure_type]`.

        data_col : str, optional
            Explicit name of the data column to use when loading each file.
            If provided, this overrides the `measure_type` inference.

        geoid_col : str, default "GEOID10"
            Name of the column that stores geographic identifiers.

        date_col : str, default "Date"
            Name of the column containing date information in the data files.

        rename_col_dict : dict, optional
            Optional mapping from year (as string) to column-renaming dictionaries.
            Each rename dictionary is applied before validating and reading the CSV file
            for that year. Useful when column names vary between years, e.g.:

            >>> rename_col_dict = {
            ...     "2010": {"HeatIndex_2010": "HeatIndex"},
            ...     "2011": {"HeatIndex2011": "HeatIndex"}
            ... }

        read_dtype : str, default "float32"
            Data type to use for the data column when reading. Using "float32" typically
            reduces memory footprint with minimal precision loss.

        geoid_filter : set, optional
            Optional set of GEOID strings to filter the data to. If provided, this filter
            will be passed to all DailyMeasureData objects created when accessing years.
            This dramatically reduces memory usage when you only need data for a small subset of GEOIDs.

        file_extension : str, optional
            Optional file extension to search for (e.g., ".csv", ".parquet").
            Extension should include the leading dot. If not provided, searches for all
            supported formats: .csv, .dta, .parquet, .pq, .feather, .xlsx, .xls.
            Useful for improving performance when you know your data is in a specific format.

        Raises
        ------
        FileNotFoundError
            If `dir_name` does not exist.

        ValueError
            If neither `measure_type` nor `data_col` is provided.
        ValueError
            If no data files matching `measure_type` are found in the directory.
        ValueError
            If any file does not contain the expected `data_col` after applying its
            year-specific renaming dictionary. The error message will list all problematic files.

        Notes
        -----
        - File names must contain a **4-digit year**, which is extracted automatically.
          Duplicate years are not allowed.
        - Files are **not read immediately**. They are only validated for the presence of
          the `data_col` in their headers. Actual reading happens when accessing `dir[year]`.
        - All loaded data is cached in memory per year for fast repeated access.
        - Typically used together with `HRSContextLinker` for linking daily environmental
          measures to survey data across multiple lag periods.

        Examples
        --------
        >>> # Directory contains: heat_2010.csv, heat_2011.csv, ...
        >>> heat_dir = DailyMeasureDataDir(
        ...     dir_name="data/daily_heat_long",
        ...     measure_type="heat",
        ...     rename_col_dict={"2010": {"HeatIndex_2010": "HeatIndex"}}
        ... )
        >>> heat_dir.list_years()
        ['2010', '2011', '2012', ...]

        >>> # Load a specific year
        >>> df_2010 = heat_dir[2010].df
        >>> df_2010.head()
                 Date      GEOID10  HeatIndex
        0  2010-01-01  01001020100       45.2
        1  2010-01-01  01001020200       43.8
        2  2010-01-01  01001020300       44.1

        >>> # Only search for Parquet files
        >>> pm25_dir = DailyMeasureDataDir(
        ...     dir_name="data/daily_pm25",
        ...     measure_type="pm25",
        ...     file_extension=".parquet"
        ... )
        """
        self.dirpath = Path(dir_name)
        if not self.dirpath.exists():
            raise FileNotFoundError(f"Directory not found: {self.dirpath}")

        # Validate data_col / measure_type logic
        if data_col is None and measure_type is None:
            raise ValueError("Either `data_col` or `measure_type` must be provided")
        if data_col is None:
            data_col = FILENAME_TO_VARNAME_DICT[measure_type]

        # Normalize data_col to list
        if isinstance(data_col, str):
            data_col = [data_col]
        self.data_col = data_col
        self.geoid_col = geoid_col
        self.date_col = date_col
        self.measure_type = measure_type
        self.read_dtype = read_dtype
        self.geoid_filter = geoid_filter

        # Rename dict per year (optional)
        self.rename_col_dict = rename_col_dict or {}

        # Determine which file extensions to search for
        if file_extension is None:
            # Default: search all supported file extensions (same as io_utils.py)
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
            # Use only the specified file extension
            supported_extensions = [file_extension]

        # Collect files for measure_type if specified, otherwise all
        all_files = []
        for ext in supported_extensions:
            all_files.extend(self.dirpath.glob(f"*{ext}"))

        if measure_type is not None:
            self.files: List[Path] = sorted(
                f for f in all_files if measure_type in f.name
            )
        else:
            self.files: List[Path] = sorted(all_files)

        if not self.files:
            raise ValueError(
                f"No data files found for measure type '{measure_type}' in {dir_name}"
            )

        # Build year → file mapping
        self.year_to_file: Dict[str, Path] = self._build_year_file_map()
        self.years_available: List[str] = sorted(self.year_to_file.keys())

        # Validate that each file contains the expected data_col
        self._validate_files_have_datacol()

        # Cache for loaded DailyMeasureData objects
        self._cache: Dict[str, DailyMeasureData] = {}

    # ------------------------------------------------------------------
    def _build_year_file_map(self) -> Dict[str, Path]:
        mapping = {}
        for f in self.files:
            m = self.YEAR_PATTERN.search(f.name)
            if not m:
                raise ValueError(f"Could not extract year from filename: {f.name}")
            year = m.group(1)
            if year in mapping:
                raise ValueError(f"Duplicate year {year} found in directory")
            mapping[year] = f
        return mapping

    # ------------------------------------------------------------------
    def _validate_files_have_datacol(self):
        """
        Checks that each file contains the expected data_col after applying
        any renaming rules for that year. Raises informative error otherwise.
        """
        missing = []
        for year, fpath in self.year_to_file.items():
            # Read just the header based on file format
            file_format = get_file_format(fpath)

            if file_format == "csv":
                header = pd.read_csv(fpath, nrows=0)
            elif file_format == "stata":
                # For Stata, use iterator to get columns without reading full file
                header = pd.read_stata(fpath, chunksize=1).__next__().iloc[:0]
            elif file_format in ("parquet", "feather"):
                # For Parquet/Feather, we need to read (but these are fast)
                df = read_data(fpath)
                header = df.iloc[:0]
            elif file_format == "excel":
                header = pd.read_excel(fpath, nrows=0)
            else:
                # Fallback
                df = read_data(fpath)
                header = df.iloc[:0]

            rename_dict = self.rename_col_dict.get(year, None)
            if rename_dict:
                header = header.rename(columns=rename_dict)

            # Check if all data_cols are present
            missing_cols = [col for col in self.data_col if col not in header.columns]
            if missing_cols:
                missing.append((year, fpath.name, missing_cols, list(header.columns)))

        if missing:
            msg_lines = ["The following files do not contain the expected column(s):"]
            for year, fname, missing_cols, cols in missing:
                msg_lines.append(
                    f" - {year} ({fname}): missing {missing_cols}, available columns = {cols}"
                )
            raise ValueError("\n".join(msg_lines))

    # ------------------------------------------------------------------
    def __getitem__(self, year: Union[int, str]) -> DailyMeasureData:
        """
        Lazy load a specific year's DailyMeasureData object.
        """
        year_key = str(year)
        if year_key not in self.year_to_file:
            raise KeyError(
                f"Year {year_key} not found. Available: {self.years_available}"
            )

        if year_key not in self._cache:
            file_path = self.year_to_file[year_key]
            rename_col = self.rename_col_dict.get(year_key, None)

            print(
                f"📥 Loading {self.measure_type or self.data_col} file for year {year_key}: {file_path.name}"
            )

            self._cache[year_key] = DailyMeasureData(
                file_path=file_path,
                data_col=self.data_col,
                measure_type=self.measure_type,
                read_dtype=self.read_dtype,
                rename_col=rename_col,
                geoid_filter=self.geoid_filter,
                geoid_col=self.geoid_col,
                date_col=self.date_col,
            )

        return self._cache[year_key]

    # ------------------------------------------------------------------
    def preload_years(self, years: Optional[List[str]] = None) -> None:
        """
        Preload data for specified years (or all available years).
        Loads all data into _cache to avoid lazy loading during processing.

        Parameters
        ----------
        years : List[str], optional
            List of years to preload. If None, preloads all available years.

        Examples
        --------
        >>> heat_dir.preload_years(['2016', '2017', '2018'])
        >>> # All specified years are now cached in memory
        """
        if years is None:
            years = self.years_available

        print(
            f"📥 Preloading {len(years)} years of {self.measure_type or self.data_col} data..."
        )
        for year in years:
            if year not in self._cache:
                _ = self[year]  # Triggers lazy loading and caching
        print(f"✅ Preloaded {len(years)} years successfully")

    # ------------------------------------------------------------------
    def list_years(self) -> List[str]:
        return self.years_available

    def __repr__(self):
        years_str = ", ".join(self.years_available)
        measure = self.measure_type or self.data_col
        return f"DailyMeasureDataDir({self.dirpath}, measure={measure}, years=[{years_str}])"
