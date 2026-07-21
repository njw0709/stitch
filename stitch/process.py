from pathlib import Path
from typing import Optional, List, Union, Callable
import argparse
import hashlib
import json
import shutil
import tempfile
import uuid
import pandas as pd
from tqdm import tqdm


class PipelineCancelled(Exception):
    """Raised to abort a pipeline run when a cancellation was requested.

    ``run_pipeline`` (and the lag-processing helpers) accept an optional
    ``should_cancel`` callable. When it returns ``True`` at one of the
    cooperative check points, this exception is raised so the run unwinds
    cleanly (the ``finally`` block still removes temp files).
    """


def _raise_if_cancelled(should_cancel: Optional[Callable[[], bool]]) -> None:
    """Raise :class:`PipelineCancelled` if *should_cancel* returns True."""
    if should_cancel is not None and should_cancel():
        raise PipelineCancelled("Pipeline run was cancelled by the user.")
from .hrs import (
    HRSContextLinker,
    HRSInterviewData,
    ResidentialHistoryHRS,
)
from .daily_measure import (
    DailyMeasureDataDir,
    aggregate_contextual_to_resolution,
)
from .io_utils import (
    apply_geoid_normalization,
    normalize_geoid_for_processing,
    write_data,
)
from .temporal import AggMethod, LinkageResolution, infer_temporal_resolution


def convert_geoid_columns(
    df: pd.DataFrame,
    geoid_cols: List[str],
    treatment: str = "code",
    n_digits: int = 11,
    numeric_type: str = "int",
    final: bool = True,
) -> pd.DataFrame:
    """
    Normalize GEOID columns according to the chosen treatment.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing GEOID columns.
    geoid_cols : List[str]
        GEOID column names to normalize.
    treatment : str
        ``"code"`` for zero-padded string, ``"numeric"`` for int/float.
    n_digits : int
        Target digit width (only used when *treatment* is ``"code"``).
    numeric_type : str
        ``"int"`` or ``"float"`` (only used when *treatment* is ``"numeric"``).
    final : bool
        If ``True`` (default), produce the final output format (numeric columns
        stay numeric).  If ``False``, produce string representations suitable
        for intermediate matching while still applying the chosen normalization
        (e.g. numeric-first then stringified).
    """
    df = df.copy()
    normalizer = apply_geoid_normalization if final else normalize_geoid_for_processing
    for col in geoid_cols:
        if col in df.columns:
            df[col] = normalizer(
                df[col],
                treatment=treatment,
                n_digits=n_digits,
                numeric_type=numeric_type,
            )
    return df


def _hrs_resolution(hrs_data: HRSInterviewData) -> LinkageResolution:
    return getattr(hrs_data, "linkage_resolution", LinkageResolution.DAILY)


def _hrs_agg_method(hrs_data: HRSInterviewData) -> AggMethod:
    return getattr(hrs_data, "agg_method", AggMethod.AVERAGE)


def _prepare_contextual_resolution(
    contextual_df: pd.DataFrame,
    date_col: str,
    geoid_col: str,
    data_cols,
    hrs_data: HRSInterviewData,
) -> pd.DataFrame:
    """Align/aggregate contextual data to the linkage resolution.

    The contextual date column is floored to the requested resolution's period
    key so it lines up with the (also floored) survey lag dates. When the
    requested resolution is coarser than the contextual data, values are
    aggregated up using the configured aggregation method.
    """
    res = _hrs_resolution(hrs_data)
    ctx_res = infer_temporal_resolution(contextual_df[date_col])

    if res.is_finer_than(ctx_res):
        raise ValueError(
            f"Requested linkage resolution '{res.value}' is finer than the "
            f"contextual data resolution '{ctx_res.value}'. Choose a resolution "
            f"no finer than the contextual data."
        )

    if res.is_coarser_than(ctx_res):
        return aggregate_contextual_to_resolution(
            contextual_df,
            date_col=date_col,
            geoid_col=geoid_col,
            data_cols=data_cols,
            resolution=res,
            method=_hrs_agg_method(hrs_data),
        )

    # Same resolution: just align the period key (cheap, vectorized).
    out = contextual_df.copy()
    out[date_col] = res.floor(out[date_col])
    return out


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

    # ``max_lag_days`` is expressed in the linkage resolution's unit (days for
    # daily, months for monthly, hours for hourly); convert to a day span so we
    # never drop a year that a lag could reach into.
    res = _hrs_resolution(hrs_data)
    max_lag_days_span = res.max_lag_days(max_lag_days)

    dates = hrs_data.df[date_col]
    min_date = dates.min() - pd.Timedelta(days=max_lag_days_span)
    max_date = dates.max()

    return list(range(min_date.year, max_date.year + 1))


def extract_unique_geoids(
    hrs_data_with_lags: pd.DataFrame,
    geoid_col: str = "GEOID2010",
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
    geoid_col : str, default "GEOID2010"
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
    >>> unique_geoids = extract_unique_geoids(hrs_with_lags, "GEOID2010")
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
    should_cancel: Optional[Callable[[], bool]] = None,
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

    # Align/aggregate contextual data to the linkage resolution.
    contextual_df = _prepare_contextual_resolution(
        contextual_df,
        contextual_date_col,
        contextual_geoid_col,
        contextual_data_col,
        hrs_data,
    )

    # Check for duplicate date and geoid pairs
    duplicate_mask = contextual_df.duplicated(
        subset=[contextual_date_col, contextual_geoid_col], keep=False
    )
    n_duplicates = duplicate_mask.sum()
    if n_duplicates > 0:
        print(
            f"⚠️  Warning: Found {n_duplicates} duplicate date-geoid pairs in contextual data"
        )
        # Show a few examples
        duplicate_examples = contextual_df[duplicate_mask].head(5)[
            [contextual_date_col, contextual_geoid_col]
        ]
        print(f"  Example duplicates:\n{duplicate_examples}")
        # Remove duplicates, keeping the first occurrence
        contextual_df = contextual_df.drop_duplicates(
            subset=[contextual_date_col, contextual_geoid_col], keep="first"
        )
        print(f"  Removed duplicates. New shape: {contextual_df.shape}")

    # Step 4: Process each lag using pre-computed data
    temp_files = []
    for n in tqdm(n_days, desc="Processing lags", unit="lag"):
        _raise_if_cancelled(should_cancel)
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

        # Normalize GEOID columns (intermediate string form for later merging)
        if geoid_col is None:
            geoid_col = hrs_data.geoid_col
        temp_geoid_cols = [c for c in out_df.columns if geoid_col in c]
        out_df = convert_geoid_columns(
            out_df,
            temp_geoid_cols,
            treatment=hrs_data.geoid_treatment,
            n_digits=hrs_data.geoid_n_digits,
            numeric_type=hrs_data.geoid_numeric_type,
            final=False,
        )

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
    should_cancel: Optional[Callable[[], bool]] = None,
) -> List[Path]:
    """
    Process multiple lags with parallel processing using ProcessPoolExecutor.

    Pre-computes all lag columns and filters contextual data once, then
    distributes lag processing across worker processes via a "spawn" context
    to bypass the GIL and avoid fork-safety issues with Qt threads.

    Large shared data (HRS data, pre-computed lag DataFrame, contextual
    DataFrame) is passed to workers through the pool initializer so it is
    serialized only once per worker rather than once per task.

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
        Maximum number of worker processes. If None and auto_memory_limit is
        True, automatically calculates based on available memory. Otherwise
        uses the ProcessPoolExecutor default.
    auto_memory_limit : bool, default True
        If True and max_workers is None, automatically calculate max_workers
        based on available system memory to prevent OOM errors. Each worker
        process receives its own copy of the shared data, so memory usage
        scales with the number of workers.

    Returns
    -------
    List[Path]
        List of paths to temporary files created for each lag
    """
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor, as_completed
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

    # Extract metadata once to avoid accessing contextual_dir in workers
    first_year = years_to_load[0]
    first_context = contextual_dir[first_year]
    contextual_date_col = first_context.date_col
    contextual_geoid_col = first_context.geoid_col
    contextual_data_col = first_context.data_col

    # Align/aggregate contextual data to the linkage resolution.
    contextual_df = _prepare_contextual_resolution(
        contextual_df,
        contextual_date_col,
        contextual_geoid_col,
        contextual_data_col,
        hrs_data,
    )

    # Check for duplicate date and geoid pairs
    duplicate_mask = contextual_df.duplicated(
        subset=[contextual_date_col, contextual_geoid_col], keep=False
    )
    n_duplicates = duplicate_mask.sum()
    if n_duplicates > 0:
        print(
            f"⚠️  Warning: Found {n_duplicates} duplicate date-geoid pairs in contextual data"
        )
        # Show a few examples
        duplicate_examples = contextual_df[duplicate_mask].head(5)[
            [contextual_date_col, contextual_geoid_col]
        ]
        print(f"  Example duplicates:\n{duplicate_examples}")
        # Remove duplicates, keeping the first occurrence
        contextual_df = contextual_df.drop_duplicates(
            subset=[contextual_date_col, contextual_geoid_col], keep="first"
        )
        print(f"  Removed duplicates. New shape: {contextual_df.shape}")

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

    # Step 4: Process lags in parallel using separate processes
    print(f"⚡ Processing {len(n_days)} lags in parallel...")
    temp_files = []

    mp_context = multiprocessing.get_context("spawn")

    with ProcessPoolExecutor(
        max_workers=max_workers,
        mp_context=mp_context,
        initializer=_init_worker,
        initargs=(
            hrs_data, hrs_with_lags, contextual_df,
            contextual_date_col, contextual_geoid_col, contextual_data_col,
        ),
    ) as executor:
        futures = {
            executor.submit(
                _process_single_lag_worker,
                n=n,
                id_col=id_col,
                temp_dir=temp_dir,
                prefix=prefix,
                include_lag_date=include_lag_date,
                file_format=file_format,
                geoid_col=geoid_col,
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
            # Cooperative cancellation: stop collecting, cancel any pending
            # futures, and unwind. Already-running workers are allowed to finish
            # their current lag but their results are discarded.
            if should_cancel is not None and should_cancel():
                print("  ⛔ Cancellation requested — stopping parallel processing...")
                executor.shutdown(wait=False, cancel_futures=True)
                raise PipelineCancelled("Pipeline run was cancelled by the user.")

            n = futures[fut]
            try:
                result = fut.result()
                if result is not None:
                    temp_files.append(result)
            except Exception as e:
                print(f"  ❌ Error processing lag {n}: {e}")

    print(f"✅ Parallel processing complete! Generated {len(temp_files)} files\n")
    return temp_files


_worker_shared: dict = {}


def _init_worker(hrs_data, precomputed_lag_df, preloaded_contextual_df,
                 contextual_date_col, contextual_geoid_col, contextual_data_col):
    """Store shared read-only data in each worker process's global namespace."""
    _worker_shared['hrs_data'] = hrs_data
    _worker_shared['precomputed_lag_df'] = precomputed_lag_df
    _worker_shared['preloaded_contextual_df'] = preloaded_contextual_df
    _worker_shared['contextual_date_col'] = contextual_date_col
    _worker_shared['contextual_geoid_col'] = contextual_geoid_col
    _worker_shared['contextual_data_col'] = contextual_data_col


def _process_single_lag_worker(n, id_col, temp_dir, prefix,
                               include_lag_date, file_format, geoid_col):
    """Thin wrapper that reads shared data from process globals."""
    return _process_single_lag_internal(
        n=n,
        hrs_data=_worker_shared['hrs_data'],
        id_col=id_col,
        temp_dir=temp_dir,
        prefix=prefix,
        include_lag_date=include_lag_date,
        file_format=file_format,
        geoid_col=geoid_col,
        precomputed_lag_df=_worker_shared['precomputed_lag_df'],
        preloaded_contextual_df=_worker_shared['preloaded_contextual_df'],
        contextual_date_col=_worker_shared['contextual_date_col'],
        contextual_geoid_col=_worker_shared['contextual_geoid_col'],
        contextual_data_col=_worker_shared['contextual_data_col'],
    )


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

            # Align/aggregate contextual data to the linkage resolution.
            contextual_df = _prepare_contextual_resolution(
                contextual_df,
                date_col,
                contextual_geoid_col_name,
                data_col,
                hrs_data,
            )

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

        # Normalize GEOID columns (intermediate string form for later merging)
        if geoid_col is None:
            geoid_col = hrs_data.geoid_col
        temp_geoid_cols = [c for c in out_df.columns if geoid_col in c]
        out_df = convert_geoid_columns(
            out_df,
            temp_geoid_cols,
            treatment=hrs_data.geoid_treatment,
            n_digits=hrs_data.geoid_n_digits,
            numeric_type=hrs_data.geoid_numeric_type,
            final=False,
        )

        filename = f"{prefix}_lag_{n:04d}.{file_format}"
        temp_file = temp_dir / filename

        write_data(out_df, temp_file, index=False)

        return temp_file

    except Exception as e:
        print(f"❌ Error processing lag {n} ({prefix}): {e}")
        return None


#: Basename of the job-args manifest written into every job temp directory.
JOB_ARGS_FILENAME = "job_args.json"

#: Keys that do not describe the linkage configuration and must be excluded
#: from the job signature so they never affect resume matching.
_SIGNATURE_EXCLUDED_KEYS = frozenset({"job_id"})


def _create_job_temp_dir(job_id: Optional[str] = None) -> Path:
    """
    Create a unique, private temporary directory for a single pipeline job.

    The directory is created inside the operating system's temporary location
    (``$TMPDIR`` / ``/tmp`` on Linux/macOS, ``%TEMP%`` on Windows) rather than
    inside the user-visible save directory. This keeps the intermediate lag
    files (which may contain confidential information) out of reach of casual
    users and prevents concurrent jobs from colliding.

    ``tempfile.mkdtemp`` is used because it is cross-platform and creates the
    directory atomically with owner-only permissions (mode ``0o700``) on every
    supported platform.

    Parameters
    ----------
    job_id : str, optional
        Identifier used to scope the temporary directory. If not provided, a
        random identifier is generated so that each job gets its own directory.

    Returns
    -------
    Path
        Path to the newly created, job-scoped temporary directory.
    """
    job_id = job_id or uuid.uuid4().hex
    return Path(tempfile.mkdtemp(prefix=f"stitch_{job_id}_"))


def _job_args_to_dict(args: argparse.Namespace) -> dict:
    """Return the linkage-configuration fields of *args* as a plain dict.

    Volatile keys (see :data:`_SIGNATURE_EXCLUDED_KEYS`) are dropped so they do
    not influence the job signature. Values are left as-is; JSON serialization
    coerces non-primitive values (e.g. ``Path``) via ``default=str``.
    """
    return {
        k: v
        for k, v in vars(args).items()
        if k not in _SIGNATURE_EXCLUDED_KEYS
    }


def _job_signature(args: argparse.Namespace) -> str:
    """Compute a stable content hash identifying a job's configuration.

    Two jobs with identical configuration produce the same signature, which is
    what lets a new run discover and resume a previous job's partially
    populated temp directory.
    """
    canonical = json.dumps(_job_args_to_dict(args), sort_keys=True, default=str)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]


def _write_job_args(
    temp_dir: Path, args: argparse.Namespace, signature_hash: str
) -> None:
    """Write the job-args manifest into *temp_dir* (idempotent).

    The manifest records the job signature and the full configuration so a
    later run can confirm an exact match before resuming into this directory.
    Existing manifests are left untouched (a resumed run keeps the original).
    """
    manifest_path = temp_dir / JOB_ARGS_FILENAME
    if manifest_path.exists():
        return
    payload = {
        "signature_hash": signature_hash,
        "args": _job_args_to_dict(args),
        "n_lags": getattr(args, "n_lags", None),
        "start_lag": getattr(args, "start_lag", 0),
    }
    manifest_path.write_text(
        json.dumps(payload, sort_keys=True, default=str, indent=2),
        encoding="utf-8",
    )


def _find_resumable_temp_dir(
    args: argparse.Namespace, signature_hash: Optional[str] = None
) -> Optional[Path]:
    """Find a previous job temp dir whose configuration matches *args*.

    Scans the OS temp location for ``stitch_*`` directories and returns the
    first one whose ``job_args.json`` records the same signature. Completed
    jobs delete their temp dir, so any match is by construction an incomplete
    run that can be resumed.

    Returns ``None`` when no matching directory exists.
    """
    if signature_hash is None:
        signature_hash = _job_signature(args)

    system_temp = Path(tempfile.gettempdir())
    for candidate in sorted(system_temp.glob("stitch_*")):
        if not candidate.is_dir():
            continue
        manifest_path = candidate / JOB_ARGS_FILENAME
        if not manifest_path.exists():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if manifest.get("signature_hash") == signature_hash:
            return candidate
    return None


def cleanup_stitch_temp_dirs() -> None:
    """Remove every ``stitch_*`` job temp directory under the OS temp location.

    Used to guarantee no build-up of (possibly confidential) intermediate lag
    files across sessions: called on GUI startup and again on quit. Because job
    temp dirs are session-only, wiping all of them here is safe.
    """
    system_temp = Path(tempfile.gettempdir())
    for candidate in system_temp.glob("stitch_*"):
        if candidate.is_dir():
            shutil.rmtree(candidate, ignore_errors=True)


def _merge_lag_averages(
    temp_files: List[Path],
    base_df: pd.DataFrame,
    args: argparse.Namespace,
    read_fn: Callable[[Path], pd.DataFrame],
    should_cancel: Optional[Callable[[], bool]] = None,
) -> pd.DataFrame:
    """Collapse per-lag files into a single strict-mean column per measure.

    Reads one lag file at a time (a running sum plus a count per measure column)
    so peak memory stays flat regardless of how many lags were processed -- no
    list of per-lag frames and no wide concatenation is ever materialized.

    "Strict" averaging: because summation propagates ``NaN``, any participant
    that is missing the value for *any* lag ends up with ``NaN`` for the mean.

    Parameters
    ----------
    temp_files : List[Path]
        Per-lag files, each holding ``id_col`` plus one value column per measure
        named ``{measure}_{date_col}_{n}day_prior``.
    base_df : pd.DataFrame
        The main HRS dataframe the averaged columns are appended to.
    args : argparse.Namespace
        Pipeline configuration (uses ``id_col``, ``data_col``, ``date_col``,
        ``start_lag`` and ``n_lags``).
    read_fn : Callable[[Path], pd.DataFrame]
        Format-aware reader (``pd.read_parquet`` or ``pd.read_csv``).
    should_cancel : Callable[[], bool], optional
        Cooperative cancellation check.

    Returns
    -------
    pd.DataFrame
        ``base_df`` with one appended column ``{measure}_avg_{start}_{end}day_prior``
        per measure.
    """
    id_col = args.id_col
    if isinstance(args.data_col, str):
        data_cols = [c.strip() for c in args.data_col.split(",")]
    else:
        data_cols = list(args.data_col)

    resolution = LinkageResolution.from_str(
        getattr(args, "linkage_resolution", "daily") or "daily"
    )
    unit = resolution.lag_unit

    n_rows = len(base_df)
    ids_ref = base_df[id_col].values

    sums: dict[str, pd.Series] = {}
    counts: dict[str, int] = {}

    for i, f in enumerate(temp_files):
        _raise_if_cancelled(should_cancel)
        if (i + 1) % 100 == 0:
            print(f"  Averaged {i + 1}/{len(temp_files)} files...")

        lag_df = read_fn(f)

        # --- Safety checks (mirror the non-averaged merge path) ---
        if len(lag_df) != n_rows:
            raise ValueError(
                f"Row count mismatch in lag file {f}: "
                f"{len(lag_df)} rows vs {n_rows} in main df"
            )

        ids_right = lag_df[id_col].values
        if not (ids_ref == ids_right).all():
            if set(ids_ref) != set(ids_right):
                raise ValueError(
                    f"ID set mismatch in lag file {f}: "
                    "the lag file has different IDs than the main df"
                )
            raise ValueError(
                f"ID order mismatch in lag file {f}: "
                "IDs match as a set but not row order"
            )
        # --- End safety checks ---

        n = int(f.stem.split("_lag_")[1])
        for col in data_cols:
            val_col = f"{col}_{args.date_col}_{n}{unit}_prior"
            if val_col not in lag_df.columns:
                continue
            values = pd.to_numeric(lag_df[val_col], errors="coerce").reset_index(
                drop=True
            )
            if col not in sums:
                sums[col] = values
                counts[col] = 1
            else:
                # NaN-propagating addition gives the strict-mean semantics.
                sums[col] = sums[col] + values
                counts[col] += 1

        del lag_df

    start_lag = int(getattr(args, "start_lag", 0) or 0)
    max_lag = int(args.n_lags) - 1

    avg_cols: dict[str, pd.Series] = {}
    for col in data_cols:
        if counts.get(col, 0) == 0:
            continue
        avg_name = f"{col}_avg_{start_lag}_{max_lag}{unit}_prior"
        avg_cols[avg_name] = sums[col] / counts[col]

    return pd.concat(
        [base_df.reset_index(drop=True), pd.DataFrame(avg_cols, index=range(n_rows))],
        axis=1,
    )


def run_pipeline(
    args: argparse.Namespace,
    should_cancel: Optional[Callable[[], bool]] = None,
):
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
        - survey_data: Path to HRS/survey Stata file
        - context_dir: Directory containing contextual data files
        - output_name: Output file name
        - save_dir: Directory to save output and temp files
        - id_col: Unique identifier column
        - date_col: Interview date column
        - measure_type: Measurement type (e.g., heat_index, pm25)
        - data_col: Data column name in contextual files
        - geoid_col: GEOID column name in HRS data
        - contextual_geoid_col: GEOID column name in contextual files
        - n_lags: Number of lags to process (exclusive upper bound)
        - start_lag: Lag day to start from, i.e. minimum days prior (optional, default 0)
        - file_extension: File extension for contextual files (optional)
        - parallel: Whether to use parallel processing
        - include_lag_date: Whether to include lag date columns
        - residential_hist: Path to residential history file (optional)
        - res_hist_*: Residential history configuration parameters
    """
    hrs_path = Path(args.survey_data)
    context_dir = Path(args.context_dir)
    out_path = Path(args.save_dir) / Path(args.output_name)

    if not hrs_path.exists():
        raise FileNotFoundError(f"HRS file not found: {hrs_path}")
    if not context_dir.exists():
        raise FileNotFoundError(f"Contextual data directory not found: {context_dir}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    post_lag_average = bool(getattr(args, "post_lag_average", False))
    save_temp_to_output = bool(getattr(args, "save_temp_to_output", False))
    include_lag_date = bool(getattr(args, "include_lag_date", False))

    # Post-lag averaging collapses every lag into a single averaged column per
    # measure, which makes per-lag date columns meaningless. If the user asks for
    # both, averaging wins and include_lag_date is ignored (with a warning) rather
    # than aborting the run.
    if post_lag_average and include_lag_date:
        print(
            "⚠️  include_lag_date=True is ignored because post-lag averaging is "
            "enabled (averaging collapses all lags into a single column)."
        )
        include_lag_date = False

    if post_lag_average:
        print(
            "⚠️  Post-lag averaging uses strict handling: any participant missing a "
            "value for any lag in the selected range will have a missing (NaN) average."
        )

    # When requested, intermediate lag files are written as CSV into the user's
    # output directory (and kept as a deliverable) instead of the default hidden,
    # auto-cleaned Parquet files in a private OS-temp directory.
    file_format = "csv" if save_temp_to_output else "parquet"

    geoid_n_digits = getattr(args, "geoid_n_digits", 11)
    geoid_treatment = getattr(args, "geoid_treatment", "code")
    geoid_numeric_type = getattr(args, "geoid_numeric_type", "int")

    # Linkage temporal resolution and (coarsening) aggregation method.
    resolution = LinkageResolution.from_str(
        getattr(args, "linkage_resolution", "daily") or "daily"
    )
    agg_method = AggMethod.from_str(
        getattr(args, "agg_method", "average") or "average"
    )
    print(f"Linkage resolution: {resolution.value} (lag unit: {resolution.lag_unit})")

    # Load residential history (optional)
    if args.residential_hist:
        print("Loading residential history...")
        residential_hist = ResidentialHistoryHRS(
            filename=Path(args.residential_hist),
            id_col=args.res_hist_id_col,
            date_col=args.res_hist_date_col,
            geoid_col=args.res_hist_geoid_col,
            geoid_n_digits=geoid_n_digits,
            geoid_treatment=geoid_treatment,
            geoid_numeric_type=geoid_numeric_type,
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
        hhidpn=args.id_col,
        geoid_col=args.geoid_col,
        geoid_n_digits=geoid_n_digits,
        geoid_treatment=geoid_treatment,
        geoid_numeric_type=geoid_numeric_type,
        linkage_resolution=resolution,
    )
    # Carry the aggregation method alongside the data so the (possibly
    # out-of-process) lag workers can reconcile coarser-than-data contextual.
    hrs_epi_data.agg_method = agg_method

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
        geoid_n_digits=geoid_n_digits,
        geoid_treatment=geoid_treatment,
        geoid_numeric_type=geoid_numeric_type,
    )

    # Process lags (parallel or batch)
    # Use a unique, private system-temp directory per job so that (1) concurrent
    # jobs never collide and (2) the intermediate lag files (which may contain
    # confidential information) are kept out of the user-visible save directory.
    #
    # If a previous run with the *identical* configuration was interrupted
    # (stopped, failed, or crashed) its temp dir still holds the lag files it
    # managed to write. We discover it by matching the job signature and resume
    # into it, reprocessing only the lags that are still missing. Completed jobs
    # delete their temp dir, so any match is by construction incomplete.
    signature_hash = _job_signature(args)
    if save_temp_to_output:
        # Persist the intermediate lag files (as CSV) next to the output file so
        # the user can inspect/keep them. A rerun resumes via the per-lag file
        # existence check below rather than the OS-temp signature discovery.
        temp_dir = out_path.parent / f"{out_path.stem}_lag_files"
        temp_dir.mkdir(parents=True, exist_ok=True)
        print(
            f"Intermediate lag files will be saved as CSV to the output "
            f"directory: {temp_dir}"
        )
    else:
        temp_dir = _find_resumable_temp_dir(args, signature_hash)
        if temp_dir is not None:
            print(f"Resuming previous job from private directory: {temp_dir}")
        else:
            temp_dir = _create_job_temp_dir(signature_hash)
            print(
                f"Temporary lag files will be saved to a private directory: {temp_dir}"
            )
    _write_job_args(temp_dir, args, signature_hash)

    try:
        _raise_if_cancelled(should_cancel)

        # Resume support: skip lags whose temp file already exists.
        def _lag_file(n: int) -> Path:
            return temp_dir / f"{args.measure_type}_lag_{n:04d}.{file_format}"

        start_lag = int(getattr(args, "start_lag", 0) or 0)
        lags_to_process = [
            n for n in range(start_lag, args.n_lags) if not _lag_file(n).exists()
        ]
        total_lags = args.n_lags - start_lag
        already_done = total_lags - len(lags_to_process)
        if already_done:
            print(
                f"Resuming: {already_done}/{total_lags} lags already processed, "
                f"{len(lags_to_process)} remaining."
            )

        if not lags_to_process:
            print("All lags already processed; skipping straight to merge.")
        elif args.parallel:
            print(f"Using parallel processing for {len(lags_to_process)} lags")
            process_multiple_lags_parallel(
                hrs_data=hrs_epi_data,
                contextual_dir=contextual_data_all,
                n_days=lags_to_process,
                id_col=args.id_col,
                temp_dir=temp_dir,
                prefix=args.measure_type,
                geoid_col=args.geoid_col,
                include_lag_date=include_lag_date,
                file_format=file_format,
                should_cancel=should_cancel,
            )
        else:
            print(f"Using batch processing for {len(lags_to_process)} lags")
            process_multiple_lags_batch(
                hrs_data=hrs_epi_data,
                contextual_dir=contextual_data_all,
                n_days=lags_to_process,
                id_col=args.id_col,
                temp_dir=temp_dir,
                prefix=args.measure_type,
                geoid_col=args.geoid_col,
                include_lag_date=include_lag_date,
                file_format=file_format,
                should_cancel=should_cancel,
            )

        # clean up
        del contextual_data_all

        # Collect every lag file present in the temp dir (previously-resumed and
        # newly-written), filtered to this measure type and sorted by lag.
        temp_files = sorted(
            temp_dir.glob(f"{args.measure_type}_lag_*.{file_format}"),
            key=lambda f: int(f.stem.split("_lag_")[1]),
        )

        # Format-aware reader so the merge composes with the CSV output option.
        read_lag = pd.read_csv if file_format == "csv" else pd.read_parquet

        if post_lag_average:
            # Stream one lag file at a time, accumulating a strict per-measure
            # mean, so memory stays flat regardless of the number of lags.
            print(
                f"Averaging {len(temp_files)} lag outputs into one column per "
                "measure..."
            )
            final_df = _merge_lag_averages(
                temp_files,
                hrs_epi_data.df,
                args,
                read_lag,
                should_cancel=should_cancel,
            )
        else:
            # Merge all lag outputs
            print(f"Merging {len(temp_files)} lag outputs with main HRS data...")
            final_df = hrs_epi_data.df

            # Collect all “lag” columns to concatenate at once
            lag_cols = []

            for i, f in enumerate(temp_files):
                _raise_if_cancelled(should_cancel)
                if (i + 1) % 100 == 0:
                    print(f"  Processed {i + 1}/{len(temp_files)} files...")

                lag_df = read_lag(f)

                # --- Safety checks ---
                # 1. Matching number of rows
                if len(lag_df) != len(final_df):
                    raise ValueError(
                        f"Row count mismatch in lag file {f}: "
                        f"{len(lag_df)} rows vs {len(final_df)} in main df"
                    )

                # 2. Same IDs AND same order
                ids_left = final_df[args.id_col].values
                ids_right = lag_df[args.id_col].values

                if not (ids_left == ids_right).all():
                    # To disambiguate, tell the user whether it's order mismatch or ID mismatch
                    if set(ids_left) != set(ids_right):
                        raise ValueError(
                            f"ID set mismatch in lag file {f}: "
                            "the lag file has different IDs than the main df"
                        )
                    else:
                        raise ValueError(
                            f"ID order mismatch in lag file {f}: "
                            "IDs match as a set but not row order"
                        )

                # --- End safety checks ---

                # Drop the id_col from the lag dataframe; we already have it in final_df
                lag_df = lag_df.drop(columns=[args.id_col], errors="ignore")

                lag_cols.append(lag_df)

            # Now concatenate all collected columns at once horizontally
            final_df = pd.concat(
                [final_df.reset_index(drop=True)]
                + [df.reset_index(drop=True) for df in lag_cols],
                axis=1,
            )

        # Apply final GEOID normalization based on user config
        base_geoid = args.geoid_col
        lag_suffix = f"{resolution.lag_unit}_prior"
        geoid_cols = [
            c
            for c in final_df.columns
            if c == base_geoid
            or (c.startswith(f"{base_geoid}_") and c.endswith(lag_suffix))
        ]
        geoid_treatment = getattr(args, "geoid_treatment", "code")
        geoid_n_digits = getattr(args, "geoid_n_digits", 11)
        geoid_numeric_type = getattr(args, "geoid_numeric_type", "int")
        final_df = convert_geoid_columns(
            final_df,
            geoid_cols,
            treatment=geoid_treatment,
            n_digits=geoid_n_digits,
            numeric_type=geoid_numeric_type,
        )

        # Save final dataset (use centralized writer for dtype conversion/sanitation)
        print(f"Saving final dataset to {out_path}")
        write_data(final_df, out_path, index=False)
        print("Done.")
    except BaseException:
        # On any failure or cancellation, deliberately keep the private temp
        # directory (and its partial lag files) so an identical rerun can resume
        # from where it left off. Leftover dirs are wiped on GUI startup/quit and
        # by the successful completion path below.
        print(
            f"Run did not complete; keeping temp directory for resume: {temp_dir}"
        )
        raise
    else:
        # Only a fully successful run removes the private temp directory (and any
        # confidential lag files it holds). When the user opted to save the
        # intermediate lag files to the output directory, keep them as the
        # requested deliverable instead of deleting them.
        if not save_temp_to_output:
            shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            print(f"Intermediate lag files kept in: {temp_dir}")
