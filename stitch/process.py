from pathlib import Path
from typing import Optional, List, Union
import argparse
import pandas as pd
from tqdm import tqdm
from .hrs import HRSInterviewData, HRSContextLinker, ResidentialHistoryHRS
from .daily_measure import DailyMeasureDataDir
from .io_utils import write_data


def convert_geoid_columns_to_string(
    df: pd.DataFrame, geoid_cols: List[str]
) -> pd.DataFrame:
    """
    Convert GEOID columns to zero-padded 11-digit strings.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing GEOID columns
    geoid_cols : List[str]
        List of GEOID column names to convert

    Returns
    -------
    pd.DataFrame
        DataFrame with GEOID columns converted to strings
    """
    df = df.copy()
    for col in geoid_cols:
        if col in df.columns:
            # Convert to string, strip non-digits, zero-pad to 11 digits
            # Missing values become empty strings
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"\D", "", regex=True)
                .replace({"nan": "", "None": "", "<NA>": ""})
                .apply(lambda x: x.zfill(11) if x else "")
            )
    return df


def compute_required_years(
    hrs_data: HRSInterviewData,
    max_lag_days: int,
    date_col: Optional[str] = None,
) -> List[int]:
    """
    Compute which years of contextual data are needed based on:
    - Interview dates in HRS data
    - Maximum lag period

    This helps optimize data loading by only loading years that are actually
    needed for the linkage, avoiding loading unnecessary years.

    Parameters
    ----------
    hrs_data : HRSInterviewData
        HRS interview or epigenetic data object containing date information.
    max_lag_days : int
        Maximum lag period in days to consider. For example, if processing
        lags from 0 to 180 days, pass 180.
    date_col : str, optional
        Name of the date column to use. If None, uses hrs_data.datecol.

    Returns
    -------
    List[int]
        List of years needed for data linkage, sorted in ascending order.

    Examples
    --------
    >>> # Survey data from 2016-2020, processing up to 180-day lags
    >>> required_years = compute_required_years(hrs_data, max_lag_days=180)
    >>> # Returns [2015, 2016, 2017, 2018, 2019, 2020]
    >>> # (includes 2015 for 180-day lags from early 2016 dates)
    """
    if date_col is None:
        date_col = hrs_data.datecol

    dates = hrs_data.df[date_col]
    min_date = dates.min() - pd.Timedelta(days=max_lag_days)
    max_date = dates.max()

    return list(range(min_date.year, max_date.year + 1))


def extract_unique_geoids(
    hrs_data_with_lags: pd.DataFrame,
    geoid_col: str = "LINKCEN2010",
) -> set:
    """
    Extract all unique GEOIDs from n-day-prior GEOID columns in the DataFrame.

    This function identifies all GEOID columns (those containing the geoid_col name)
    and collects unique values across all of them. This is useful for filtering
    contextual data to only include GEOIDs that are actually needed.

    Parameters
    ----------
    hrs_data_with_lags : pd.DataFrame
        DataFrame containing GEOID columns (typically output from
        HRSContextLinker.prepare_lag_columns_batch).
    geoid_col : str, default "LINKCEN2010"
        Name of the GEOID column used to identify GEOID-related columns.

    Returns
    -------
    set
        Set of unique GEOID strings needed for contextual data loading.

    Examples
    --------
    >>> hrs_with_lags = HRSContextLinker.prepare_lag_columns_batch(
    ...     hrs_data, n_days=[0, 7, 30]
    ... )
    >>> unique_geoids = extract_unique_geoids(hrs_with_lags, "LINKCEN2010")
    >>> print(f"Need data for {len(unique_geoids)} unique GEOIDs")
    """
    geoid_cols = [c for c in hrs_data_with_lags.columns if geoid_col in c]
    all_geoids = set()

    for col in geoid_cols:
        geoids = hrs_data_with_lags[col].dropna().unique()
        all_geoids.update(geoids)

    return all_geoids


def process_multiple_lags_batch(
    hrs_data: HRSInterviewData,
    contextual_dir: DailyMeasureDataDir,
    n_days: List[int],
    id_col: str,
    temp_dir: Path,
    prefix: str = "",
    geoid_col: Optional[str] = None,
    include_lag_date: bool = False,
    file_format: str = "parquet",
) -> List[Path]:
    """
    Process multiple lags with batch optimization using pre-computed columns and filtering.

    Workflow:
    1. Pre-compute all date/GEOID columns for all lags
    2. Keep in memory (faster than temp files for typical dataset sizes)
    3. Extract unique GEOIDs from all lag columns
    4. Load filtered contextual data once
    5. For each lag, merge pre-computed columns with contextual data
    6. Save each lag result to temp file

    Parameters
    ----------
    hrs_data : HRSInterviewData
        HRS interview or epigenetic data object
    contextual_dir : DailyMeasureDataDir
        Directory containing contextual daily measure data
    n_days : List[int]
        List of lag periods (in days) to process
    id_col : str
        Unique identifier column for joining (e.g., "hhidpn")
    temp_dir : Path
        Directory to save temporary lag files
    prefix : str, optional
        Prefix for output filenames
    geoid_col : str, optional
        Name of the GEOID column in HRS data
    include_lag_date : bool, default False
        Whether to include lag date columns in output
    file_format : {"parquet", "feather", "csv"}, default "parquet"
        File format for temporary output files

    Returns
    -------
    List[Path]
        List of paths to temporary files created for each lag
    """
    if geoid_col is None:
        geoid_col = hrs_data.geoid_col

    print(f"\n🔄 Starting batch processing for {len(n_days)} lags...")

    # Step 1: Pre-compute all lag columns
    print(f"📋 Pre-computing date/GEOID columns for lags: {n_days}")
    hrs_with_lags = HRSContextLinker.prepare_lag_columns_batch(
        hrs_data, n_days, geoid_col
    )

    # Step 2: Extract unique GEOIDs
    unique_geoids = extract_unique_geoids(hrs_with_lags, geoid_col)
    print(f"🔍 Extracted {len(unique_geoids)} unique GEOIDs from all lag columns")

    # Step 3: Compute required years and load filtered contextual data
    max_lag = max(n_days)
    required_years = compute_required_years(hrs_data, max_lag)
    available_years = set(contextual_dir.list_years())
    years_to_load = [str(y) for y in required_years if str(y) in available_years]
    print(f"📅 Loading years: {years_to_load}")

    # Set filter and preload
    contextual_dir.geoid_filter = unique_geoids
    contextual_dir.preload_years(years_to_load)

    # Concatenate all years
    print(f"🔗 Concatenating filtered contextual data...")
    contextual_df = pd.concat([contextual_dir[yr].df for yr in years_to_load], axis=0)
    print(f"  Contextual data shape: {contextual_df.shape}")

    # Extract metadata once to avoid repeated access to contextual_dir
    first_year = years_to_load[0]
    first_context = contextual_dir[first_year]
    contextual_date_col = first_context.date_col
    contextual_geoid_col = first_context.geoid_col
    contextual_data_col = first_context.data_col

    # Step 4: Process each lag using pre-computed data
    temp_files = []
    for n in tqdm(n_days, desc="Processing lags", unit="lag"):
        print(f"  Processing lag {n}...")

        out_df = HRSContextLinker.output_merged_columns(
            hrs_data,
            n=n,
            id_col=id_col,
            precomputed_lag_df=hrs_with_lags,
            preloaded_contextual_df=contextual_df,
            contextual_date_col=contextual_date_col,
            contextual_geoid_col=contextual_geoid_col,
            contextual_data_col=contextual_data_col,
            include_lag_date=include_lag_date,
            geoid_col=geoid_col,
        )

        # Skip if no valid data
        if out_df.shape[1] <= 1:
            continue

        # Convert GEOID columns to strings before saving
        if geoid_col is None:
            geoid_col = hrs_data.geoid_col
        temp_geoid_cols = [c for c in out_df.columns if geoid_col in c]
        out_df = convert_geoid_columns_to_string(out_df, temp_geoid_cols)

        # Save to temp file
        filename = f"{prefix}_lag_{n:04d}.{file_format}"
        temp_file = temp_dir / filename

        write_data(out_df, temp_file, index=False)

        temp_files.append(temp_file)
        print(f"    ✓ Saved to {temp_file.name}")

    print(f"✅ Batch processing complete! Generated {len(temp_files)} files\n")
    return temp_files


def process_multiple_lags_parallel(
    hrs_data: HRSInterviewData,
    contextual_dir: DailyMeasureDataDir,
    n_days: List[int],
    id_col: str,
    temp_dir: Path,
    prefix: str = "",
    geoid_col: Optional[str] = None,
    include_lag_date: bool = False,
    file_format: str = "parquet",
    max_workers: Optional[int] = None,
    auto_memory_limit: bool = True,
) -> List[Path]:
    """
    Process multiple lags with parallel processing using ThreadPoolExecutor.

    Pre-computes all lag columns and filters contextual data once, then processes
    lags in parallel threads that share the same memory space (avoiding serialization).

    Parameters
    ----------
    hrs_data : HRSInterviewData
        HRS interview or epigenetic data object
    contextual_dir : DailyMeasureDataDir
        Directory containing contextual daily measure data
    n_days : List[int]
        List of lag periods (in days) to process
    id_col : str
        Unique identifier column for joining (e.g., "hhidpn")
    temp_dir : Path
        Directory to save temporary lag files
    prefix : str, optional
        Prefix for output filenames
    geoid_col : str, optional
        Name of the GEOID column in HRS data
    include_lag_date : bool, default False
        Whether to include lag date columns in output
    file_format : {"parquet", "feather", "csv"}, default "parquet"
        File format for temporary output files
    max_workers : int, optional
        Maximum number of worker threads. If None and auto_memory_limit is True,
        automatically calculates based on available memory. Otherwise uses default
        from ThreadPoolExecutor.
    auto_memory_limit : bool, default True
        If True and max_workers is None, automatically calculate max_workers
        based on available system memory to prevent OOM errors. Assumes ~2GB
        per worker thread as a conservative estimate.

    Returns
    -------
    List[Path]
        List of paths to temporary files created for each lag
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from .hrs import HRSContextLinker
    import os

    if geoid_col is None:
        geoid_col = hrs_data.geoid_col

    print(f"\n🚀 Starting parallel processing for {len(n_days)} lags...")

    # Step 1: Pre-compute all lag columns
    print(
        f"📋 Pre-computing date/GEOID columns for lags: {min(n_days)} to {max(n_days)}"
    )
    hrs_with_lags = HRSContextLinker.prepare_lag_columns_batch(
        hrs_data, n_days, geoid_col
    )

    # Step 2: Extract unique GEOIDs
    unique_geoids = extract_unique_geoids(hrs_with_lags, geoid_col)
    print(f"🔍 Extracted {len(unique_geoids)} unique GEOIDs from all lag columns")

    # Step 3: Compute required years and load filtered contextual data
    max_lag = max(n_days)
    required_years = compute_required_years(hrs_data, max_lag)
    available_years = set(contextual_dir.list_years())
    years_to_load = [str(y) for y in required_years if str(y) in available_years]
    print(f"📅 Loading years: {years_to_load}")

    # Set filter and preload
    contextual_dir.geoid_filter = unique_geoids
    contextual_dir.preload_years(years_to_load)

    # Concatenate all years
    print(f"🔗 Concatenating filtered contextual data...")
    contextual_df = pd.concat([contextual_dir[yr].df for yr in years_to_load], axis=0)
    print(f"  Contextual data shape: {contextual_df.shape}")

    # Extract metadata once to avoid accessing contextual_dir in threads
    first_year = years_to_load[0]
    first_context = contextual_dir[first_year]
    contextual_date_col = first_context.date_col
    contextual_geoid_col = first_context.geoid_col
    contextual_data_col = first_context.data_col

    # Auto-calculate max_workers based on available memory if not specified
    if max_workers is None and auto_memory_limit:
        try:
            import psutil

            # Get available memory in GB
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024**3)

            # Calculate shared data size
            hrs_size_mb = hrs_with_lags.memory_usage(deep=True).sum() / (1024 * 1024)
            ctx_size_mb = contextual_df.memory_usage(deep=True).sum() / (1024 * 1024)
            shared_size_gb = (hrs_size_mb + ctx_size_mb) / 1024

            # Conservative estimate: 2GB per worker + shared data
            # Use 70% of available memory as safety margin
            usable_gb = (available_gb * 0.7) - shared_size_gb
            gb_per_worker = 2.0

            if usable_gb > gb_per_worker:
                max_workers_by_memory = int(usable_gb / gb_per_worker)
                max_workers_by_cpu = os.cpu_count() or 1
                max_workers = max(1, min(max_workers_by_memory, max_workers_by_cpu))

                print(f"🧮 Memory-aware worker calculation:")
                print(f"   Available memory: {available_gb:.1f} GB")
                print(f"   Shared data: {shared_size_gb:.1f} GB")
                print(f"   Usable for workers: {usable_gb:.1f} GB")
                print(f"   Max workers (memory): {max_workers_by_memory}")
                print(f"   Max workers (CPU): {max_workers_by_cpu}")
                print(f"   Selected max_workers: {max_workers}")
            else:
                max_workers = 1
                print(
                    f"⚠️  Limited memory available ({available_gb:.1f} GB), using max_workers=1"
                )

        except ImportError:
            print("⚠️  psutil not available, using default max_workers")
            max_workers = None
        except Exception as e:
            print(f"⚠️  Error calculating memory-based max_workers: {e}")
            print("   Falling back to default max_workers")
            max_workers = None

    # Step 4: Process lags in parallel using threads (shares memory)
    print(f"⚡ Processing {len(n_days)} lags in parallel...")
    temp_files = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                _process_single_lag_internal,
                n=n,
                hrs_data=hrs_data,
                id_col=id_col,
                temp_dir=temp_dir,
                prefix=prefix,
                include_lag_date=include_lag_date,
                file_format=file_format,
                geoid_col=geoid_col,
                precomputed_lag_df=hrs_with_lags,
                preloaded_contextual_df=contextual_df,
                contextual_date_col=contextual_date_col,
                contextual_geoid_col=contextual_geoid_col,
                contextual_data_col=contextual_data_col,
            ): n
            for n in n_days
        }

        # Collect results as they complete
        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Processing lags",
            unit="lag",
        ):
            n = futures[fut]
            try:
                result = fut.result()
                if result is not None:
                    temp_files.append(result)
            except Exception as e:
                print(f"  ❌ Error processing lag {n}: {e}")

    print(f"✅ Parallel processing complete! Generated {len(temp_files)} files\n")
    return temp_files


def _process_single_lag_internal(
    n: int,
    hrs_data: HRSInterviewData,
    id_col: str,
    temp_dir: Path,
    prefix: str = "",
    include_lag_date: bool = False,
    file_format: str = "parquet",
    geoid_col: Optional[str] = None,
    precomputed_lag_df: Optional[pd.DataFrame] = None,
    preloaded_contextual_df: Optional[pd.DataFrame] = None,
    contextual_date_col: Optional[str] = None,
    contextual_geoid_col: Optional[str] = None,
    contextual_data_col: Union[str, List[str], None] = None,
    contextual_dir: Optional[DailyMeasureDataDir] = None,
) -> Optional[Path]:
    """
    Internal function to process a single lag.

    Used internally by process_multiple_lags_parallel.
    For external use, prefer process_multiple_lags_batch or process_multiple_lags_parallel.

    Parameters
    ----------
    n : int
        Lag (in days) to process.
    hrs_data : HRSInterviewData
        HRS interview or epigenetic data object.
    id_col : str
        Unique identifier column for joining (e.g., "hhidpn").
    temp_dir : Path
        Temporary directory to save output files.
    prefix : str, optional
        Optional prefix to add to the output filename (e.g., "heat", "pm25").
    include_lag_date : bool, default False
        Whether to include the lagged date column in the output.
    file_format : {"parquet", "feather", "csv"}, default "parquet"
        File format for the temporary output file.
    geoid_col : str, optional
        Name of the GEOID column in HRS data.
    precomputed_lag_df : pd.DataFrame, optional
        Pre-computed DataFrame with date and GEOID columns. If provided, skips computation.
    preloaded_contextual_df : pd.DataFrame, optional
        Pre-loaded contextual data. If provided, skips loading from contextual_dir.
    contextual_date_col : str, optional
        Name of date column in contextual data.
    contextual_geoid_col : str, optional
        Name of GEOID column in contextual data.
    contextual_data_col : str or List[str], optional
        Name(s) of data column(s) in contextual data.
    contextual_dir : DailyMeasureDataDir, optional
        Contextual dataset directory. Only needed if metadata columns or preloaded data not provided.

    Returns
    -------
    Path or None
        Path to the written temporary file, or None if no data was produced
        (e.g., if all geoid values were NA for this lag).
    """
    try:
        # If pre-computed data is provided, use it directly
        if precomputed_lag_df is not None and preloaded_contextual_df is not None:
            # Metadata should be provided when using pre-computed/pre-loaded data
            if (
                contextual_date_col is None
                or contextual_geoid_col is None
                or contextual_data_col is None
            ):
                raise ValueError(
                    "When using precomputed_lag_df and preloaded_contextual_df, "
                    "contextual_date_col, contextual_geoid_col, and contextual_data_col must be provided"
                )

            out_df = HRSContextLinker.output_merged_columns(
                hrs_data,
                n=n,
                id_col=id_col,
                precomputed_lag_df=precomputed_lag_df,
                preloaded_contextual_df=preloaded_contextual_df,
                contextual_date_col=contextual_date_col,
                contextual_geoid_col=contextual_geoid_col,
                contextual_data_col=contextual_data_col,
                include_lag_date=include_lag_date,
                geoid_col=geoid_col,
            )
        else:
            # Fallback: Compute lag columns for this single lag
            if contextual_dir is None:
                raise ValueError(
                    "contextual_dir must be provided when not using precomputed data"
                )

            if geoid_col is None:
                geoid_col = hrs_data.geoid_col

            hrs_with_lag = HRSContextLinker.prepare_lag_columns_batch(
                hrs_data, [n], geoid_col
            )

            # Extract unique GEOIDs for this lag
            unique_geoids = extract_unique_geoids(hrs_with_lag, geoid_col)

            # Compute required years and load filtered data
            required_years = compute_required_years(hrs_data, n)
            available_years = set(contextual_dir.list_years())
            years_to_load = [
                str(y) for y in required_years if str(y) in available_years
            ]

            # Set filter and load
            contextual_dir.geoid_filter = unique_geoids
            contextual_dir.preload_years(years_to_load)
            contextual_df = pd.concat(
                [contextual_dir[yr].df for yr in years_to_load], axis=0
            )

            # Extract metadata
            first_year = years_to_load[0]
            first_context = contextual_dir[first_year]
            date_col = first_context.date_col
            contextual_geoid_col_name = first_context.geoid_col
            data_col = first_context.data_col

            # Merge
            out_df = HRSContextLinker.output_merged_columns(
                hrs_data,
                n=n,
                id_col=id_col,
                precomputed_lag_df=hrs_with_lag,
                preloaded_contextual_df=contextual_df,
                contextual_date_col=date_col,
                contextual_geoid_col=contextual_geoid_col_name,
                contextual_data_col=data_col,
                include_lag_date=include_lag_date,
                geoid_col=geoid_col,
            )

        # If only ID column (no valid merged values), skip
        if out_df.shape[1] <= 1:
            return None

        # Convert GEOID columns to strings before saving
        if geoid_col is None:
            geoid_col = hrs_data.geoid_col
        temp_geoid_cols = [c for c in out_df.columns if geoid_col in c]
        out_df = convert_geoid_columns_to_string(out_df, temp_geoid_cols)

        filename = f"{prefix}_lag_{n:04d}.{file_format}"
        temp_file = temp_dir / filename

        write_data(out_df, temp_file, index=False)

        return temp_file

    except Exception as e:
        print(f"❌ Error processing lag {n} ({prefix}): {e}")
        return None


def run_pipeline(args: argparse.Namespace):
    """
    Run the complete lagged contextual data linkage pipeline.

    This function orchestrates the entire linkage process:
    1. Load HRS interview/epigenetic data
    2. Load residential history (if provided)
    3. Load contextual daily measure data
    4. Process lags (parallel or batch)
    5. Merge all lag outputs
    6. Save final dataset

    Parameters
    ----------
    args : argparse.Namespace
        Arguments containing all pipeline configuration:
        - hrs_data: Path to HRS Stata file
        - context_dir: Directory containing contextual data files
        - output_name: Output file name
        - save_dir: Directory to save output and temp files
        - id_col: Unique identifier column
        - date_col: Interview date column
        - measure_type: Measurement type (e.g., heat_index, pm25)
        - data_col: Data column name in contextual files
        - geoid_col: GEOID column name in HRS data
        - contextual_geoid_col: GEOID column name in contextual files
        - n_lags: Number of lags to process
        - file_extension: File extension for contextual files (optional)
        - parallel: Whether to use parallel processing
        - include_lag_date: Whether to include lag date columns
        - residential_hist: Path to residential history file (optional)
        - res_hist_*: Residential history configuration parameters
    """
    hrs_path = Path(args.hrs_data)
    context_dir = Path(args.context_dir)
    out_path = Path(args.save_dir) / Path(args.output_name)

    if not hrs_path.exists():
        raise FileNotFoundError(f"HRS file not found: {hrs_path}")
    if not context_dir.exists():
        raise FileNotFoundError(f"Contextual data directory not found: {context_dir}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Load residential history (optional)
    if args.residential_hist:
        print("Loading residential history...")
        residential_hist = ResidentialHistoryHRS(
            filename=Path(args.residential_hist),
            hhidpn=args.res_hist_hhidpn,
            movecol=args.res_hist_movecol,
            mvyear=args.res_hist_mvyear,
            mvmonth=args.res_hist_mvmonth,
            moved_mark=args.res_hist_moved_mark,
            geoid=args.res_hist_geoid,
            survey_yr_col=args.res_hist_survey_yr_col,
            first_tract_mark=args.res_hist_first_tract_mark,
        )
    else:
        residential_hist = None

    # Load HRS data
    print("Loading HRS interview data...")
    hrs_epi_data = HRSInterviewData(
        hrs_path,
        datecol=args.date_col,
        move=bool(residential_hist),
        residential_hist=residential_hist,
        geoid_col=args.geoid_col,
    )

    # Load contextual data
    print(f"Loading contextual daily data ({args.measure_type})...")
    # Use context_date_col if provided, otherwise default to "Date"
    context_date_col = getattr(args, "context_date_col", None) or "Date"
    # Use contextual_geoid_col if provided, otherwise default to "GEOID10"
    contextual_geoid_col = getattr(args, "contextual_geoid_col", None) or "GEOID10"

    # Parse data_col (may be comma-separated string or list)
    if isinstance(args.data_col, str):
        data_cols = [col.strip() for col in args.data_col.split(",")]
    else:
        data_cols = args.data_col

    contextual_data_all = DailyMeasureDataDir(
        context_dir,
        measure_type=args.measure_type,
        data_col=data_cols,
        geoid_col=contextual_geoid_col,
        date_col=context_date_col,
        file_extension=args.file_extension,
    )

    # Process lags (parallel or batch)
    temp_dir = Path(args.save_dir) / "temp_lag_files"
    temp_dir.mkdir(parents=True, exist_ok=True)
    print(f"Temporary lag files will be saved to: {temp_dir}")

    # Generate list of lags to process
    lags_to_process = list(range(args.n_lags))

    if args.parallel:
        print(f"Using parallel processing for {args.n_lags} lags")
        temp_files = process_multiple_lags_parallel(
            hrs_data=hrs_epi_data,
            contextual_dir=contextual_data_all,
            n_days=lags_to_process,
            id_col=args.id_col,
            temp_dir=temp_dir,
            prefix=args.measure_type,
            geoid_col=args.geoid_col,
            include_lag_date=args.include_lag_date,
            file_format="parquet",
        )
    else:
        print(f"Using batch processing for {args.n_lags} lags")
        temp_files = process_multiple_lags_batch(
            hrs_data=hrs_epi_data,
            contextual_dir=contextual_data_all,
            n_days=lags_to_process,
            id_col=args.id_col,
            temp_dir=temp_dir,
            prefix=args.measure_type,
            geoid_col=args.geoid_col,
            include_lag_date=args.include_lag_date,
            file_format="parquet",
        )

    print(f"Finished processing {len(temp_files)} lag files")
    # clean up
    del contextual_data_all

    # Merge all lag outputs
    print(f"Merging {len(temp_files)} lag outputs with main HRS data...")
    final_df = hrs_epi_data.df

    # Filter files to current measure type (prefix) to avoid leftovers, then sort by lag
    temp_files = [
        f for f in temp_files if f.stem.startswith(f"{args.measure_type}_lag_")
    ]
    temp_files.sort(key=lambda f: int(f.stem.split("_lag_")[1]))

    # Collect all “lag” columns to concatenate at once
    lag_cols = []

    for i, f in enumerate(temp_files):
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(temp_files)} files...")

        lag_df = pd.read_parquet(f)

        # Optional safety check (recommended)
        # Ensures row order is truly aligned
        if not (final_df[args.id_col].values == lag_df[args.id_col].values).all():
            raise ValueError(f"Row order mismatch in file {f}")

        # Drop the id_col from the lag dataframe; we already have it in final_df
        lag_df = lag_df.drop(columns=[args.id_col], errors="ignore")

        lag_cols.append(lag_df)

    # Now concatenate all collected columns at once horizontally
    final_df = pd.concat(
        [final_df.reset_index(drop=True)]
        + [df.reset_index(drop=True) for df in lag_cols],
        axis=1,
    )

    # Convert GEOID columns to strings before saving
    base_geoid = args.geoid_col
    geoid_cols = [
        c
        for c in final_df.columns
        if c == base_geoid
        or (c.startswith(f"{base_geoid}_") and c.endswith("day_prior"))
    ]
    final_df = convert_geoid_columns_to_string(final_df, geoid_cols)

    # Save final dataset (use centralized writer for dtype conversion/sanitation)
    print(f"Saving final dataset to {out_path}")
    write_data(final_df, out_path)
    print("Done.")
