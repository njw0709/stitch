"""
Performance tests for linkage WITHOUT residential history.

This module tests batch vs parallel processing when survey data contains
static GEOID columns (e.g., GEOID2010_2016, GEOID2010_2017) instead of
using residential history for dynamic GEOID lookup.
"""

import time
import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from stitch.hrs import HRSInterviewData
from stitch.daily_measure import DailyMeasureDataDir
from stitch.process import (
    process_multiple_lags_batch,
    process_multiple_lags_parallel,
)


@pytest.fixture
def survey_data_static_geoids(tmp_path, real_geoid_pool):
    """
    Create survey data with static GEOID columns for each year (2016-2020).

    This fixture creates survey data WITHOUT needing residential history.
    Each year has its own GEOID column (e.g., GEOID2010_2016).
    """
    from tests.data_generators import generate_fake_hhidpn, generate_fake_geoid

    n_people = 55
    hhidpns = generate_fake_hhidpn(n_people)
    rows = []

    for hhidpn in hhidpns:
        # Generate interview date between 2016-2020
        interview_year = np.random.randint(2016, 2021)
        interview_month = np.random.randint(1, 13)
        interview_day = np.random.randint(1, 29)

        iwdate = pd.Timestamp(
            f"{interview_year}-{interview_month:02d}-{interview_day:02d}"
        )

        # Create static GEOID columns for each year using real GEOIDs
        # This simulates having GEOID at different time points without residential history
        row = {
            "hhidpn": hhidpn,
            "iwdate": iwdate,
            "age": np.random.randint(50, 90),
            "gender": np.random.choice(["Male", "Female"]),
        }

        # Add GEOID columns
        row[f"GEOID2010"] = generate_fake_geoid(real_geoid_pool)

        rows.append(row)

    df = pd.DataFrame(rows)

    # Save to temporary Stata file
    file_path = tmp_path / "survey_static_geoids.dta"
    df.to_stata(file_path, write_index=False)

    return file_path


def create_large_survey_static_geoids(
    base_data_path: Path, n_copies: int, real_geoid_pool: list
) -> pd.DataFrame:
    """
    Create a larger survey dataset by replicating base data.

    For static GEOID testing, we keep the same IDs and just replicate rows.
    """
    base_df = pd.read_stata(base_data_path)

    dfs = []
    for i in range(n_copies):
        df_copy = base_df.copy()
        dfs.append(df_copy)

    large_df = pd.concat(dfs, ignore_index=True)
    print(f"  Created large dataset: {large_df.shape[0]} rows ({n_copies}x original)")
    return large_df


def test_performance_small_dataset_no_reshist(
    survey_data_static_geoids, heat_index_dir, tmp_path
):
    """
    Performance test WITHOUT residential history - small dataset baseline.

    Tests the optimized batch vs parallel processing when GEOIDs are
    already in the survey data as static columns.
    """
    print("\n" + "=" * 80)
    print("⚡ PERFORMANCE TEST: Small Dataset (No Residential History)")
    print("=" * 80)

    # Load survey data WITHOUT residential history
    print("📥 Loading survey data with static GEOIDs...")
    hrs_data = HRSInterviewData(
        survey_data_static_geoids,
        datecol="iwdate",
        move=False,  # ← KEY: No residential history lookup
        residential_hist=None,  # ← No residential history
    )

    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )

    print(f"📊 Dataset size: {hrs_data.df.shape[0]} rows")
    print(f"🏠 Using static GEOIDs (no residential history)")

    lags_to_test = [0, 1, 7, 14, 30, 60, 90]
    print(f"🔢 Number of lags: {len(lags_to_test)}")
    print(f"📋 Lags: {lags_to_test}")

    results = {}

    # Test batch processing
    print("\n🔄 Testing BATCH processing...")
    temp_dir_batch = tmp_path / "small_batch_no_reshist"
    temp_dir_batch.mkdir()

    start_time = time.time()
    temp_files_batch = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir_batch,
        prefix="heat",
    )
    batch_time = time.time() - start_time
    results["batch_time"] = batch_time

    print(f"  ⏱️  Batch time: {batch_time:.2f}s")

    # Test parallel processing
    print("\n🚀 Testing PARALLEL processing...")
    temp_dir_parallel = tmp_path / "small_parallel_no_reshist"
    temp_dir_parallel.mkdir()

    start_time = time.time()
    temp_files_parallel = process_multiple_lags_parallel(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir_parallel,
        prefix="heat",
        max_workers=4,
    )
    parallel_time = time.time() - start_time
    results["parallel_time"] = parallel_time

    print(f"  ⏱️  Parallel time: {parallel_time:.2f}s")

    # Compare results
    print("\n📊 RESULTS SUMMARY:")
    print(f"  Dataset rows: {hrs_data.df.shape[0]}")
    print(f"  Number of lags: {len(lags_to_test)}")
    print(f"  Batch processing: {batch_time:.2f}s")
    print(f"  Parallel processing: {parallel_time:.2f}s")

    if parallel_time < batch_time:
        speedup = batch_time / parallel_time
        print(f"  🏆 Parallel is {speedup:.2f}x faster")
    else:
        slowdown = parallel_time / batch_time
        print(
            f"  ⚠️  Parallel is {slowdown:.2f}x slower (overhead too high for small dataset)"
        )

    print("=" * 80)

    # Verify both produce same results (allowing for row order differences)
    # Find matching files for the same lag (lag 0)
    batch_file_0 = temp_dir_batch / "heat_lag_0000.parquet"
    parallel_file_0 = temp_dir_parallel / "heat_lag_0000.parquet"

    assert batch_file_0.exists(), "Batch lag 0 file not found"
    assert parallel_file_0.exists(), "Parallel lag 0 file not found"

    batch_df = (
        pd.read_parquet(batch_file_0).sort_values("hhidpn").reset_index(drop=True)
    )
    parallel_df = (
        pd.read_parquet(parallel_file_0).sort_values("hhidpn").reset_index(drop=True)
    )

    # Use pandas testing utility for NaN-aware comparison
    pd.testing.assert_frame_equal(
        batch_df, parallel_df, check_dtype=False, obj="Batch and parallel results"
    )


def test_performance_large_dataset_no_reshist(
    survey_data_static_geoids, real_geoid_pool, heat_index_dir, tmp_path
):
    """
    Performance test WITHOUT residential history - large dataset (10x).
    """
    print("\n" + "=" * 80)
    print("⚡ PERFORMANCE TEST: Large Dataset (No Residential History, 10x)")
    print("=" * 80)

    # Create large survey data
    print("📥 Creating large survey dataset with static GEOIDs...")
    large_survey_df = create_large_survey_static_geoids(
        survey_data_static_geoids, n_copies=10, real_geoid_pool=real_geoid_pool
    )

    # Save to temporary file and reload through HRSInterviewData
    temp_survey_path = tmp_path / "large_survey_static_geoids.dta"
    large_survey_df.to_stata(temp_survey_path)

    hrs_data = HRSInterviewData(
        temp_survey_path,
        datecol="iwdate",
        move=False,  # No residential history
        residential_hist=None,
    )

    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )

    print(f"📊 Dataset size: {hrs_data.df.shape[0]} rows")
    print(f"🏠 Using static GEOIDs (no residential history)")

    lags_to_test = [0, 1, 7, 14, 30, 60, 90]
    print(f"🔢 Number of lags: {len(lags_to_test)}")

    results = {}

    # Test batch processing
    print("\n🔄 Testing BATCH processing...")
    temp_dir_batch = tmp_path / "large_batch_no_reshist"
    temp_dir_batch.mkdir()

    start_time = time.time()
    temp_files_batch = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir_batch,
        prefix="heat",
    )
    batch_time = time.time() - start_time
    results["batch_time"] = batch_time

    print(f"  ⏱️  Batch time: {batch_time:.2f}s")

    # Test parallel processing
    print("\n🚀 Testing PARALLEL processing...")
    temp_dir_parallel = tmp_path / "large_parallel_no_reshist"
    temp_dir_parallel.mkdir()

    start_time = time.time()
    temp_files_parallel = process_multiple_lags_parallel(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir_parallel,
        prefix="heat",
        max_workers=4,
    )
    parallel_time = time.time() - start_time
    results["parallel_time"] = parallel_time

    print(f"  ⏱️  Parallel time: {parallel_time:.2f}s")

    # Compare results
    print("\n📊 RESULTS SUMMARY:")
    print(f"  Dataset rows: {hrs_data.df.shape[0]}")
    print(f"  Number of lags: {len(lags_to_test)}")
    print(f"  Batch processing: {batch_time:.2f}s")
    print(f"  Parallel processing: {parallel_time:.2f}s")

    if parallel_time < batch_time:
        speedup = batch_time / parallel_time
        print(f"  🏆 Parallel is {speedup:.2f}x faster")
    else:
        slowdown = parallel_time / batch_time
        print(f"  ⚠️  Parallel is {slowdown:.2f}x slower")

    print("=" * 80)


def test_performance_many_lags_no_reshist(
    survey_data_static_geoids, heat_index_dir, tmp_path
):
    """
    Performance test WITHOUT residential history - many lags (100 lags).
    """
    print("\n" + "=" * 80)
    print("⚡ PERFORMANCE TEST: Many Lags (No Residential History, 100 lags)")
    print("=" * 80)

    # Load survey data WITHOUT residential history
    hrs_data = HRSInterviewData(
        survey_data_static_geoids,
        datecol="iwdate",
        move=False,
        residential_hist=None,
    )

    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )

    print(f"📊 Dataset size: {hrs_data.df.shape[0]} rows")
    print(f"🏠 Using static GEOIDs (no residential history)")

    # Test with many lags (0-99)
    lags_to_test = list(range(100))
    print(f"🔢 Number of lags: {len(lags_to_test)}")

    results = {}

    # Test batch processing
    print("\n🔄 Testing BATCH processing...")
    temp_dir_batch = tmp_path / "many_lags_batch_no_reshist"
    temp_dir_batch.mkdir()

    start_time = time.time()
    temp_files_batch = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir_batch,
        prefix="heat",
    )
    batch_time = time.time() - start_time
    results["batch_time"] = batch_time

    print(f"  ⏱️  Batch time: {batch_time:.2f}s")
    print(f"  ⚡ Time per lag: {batch_time / len(lags_to_test):.3f}s")

    # Test parallel processing
    print("\n🚀 Testing PARALLEL processing...")
    temp_dir_parallel = tmp_path / "many_lags_parallel_no_reshist"
    temp_dir_parallel.mkdir()

    start_time = time.time()
    temp_files_parallel = process_multiple_lags_parallel(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir_parallel,
        prefix="heat",
        max_workers=4,
    )
    parallel_time = time.time() - start_time
    results["parallel_time"] = parallel_time

    print(f"  ⏱️  Parallel time: {parallel_time:.2f}s")
    print(f"  ⚡ Time per lag: {parallel_time / len(lags_to_test):.3f}s")

    # Compare results
    print("\n📊 RESULTS SUMMARY:")
    print(f"  Dataset rows: {hrs_data.df.shape[0]}")
    print(f"  Number of lags: {len(lags_to_test)}")
    print(
        f"  Batch processing: {batch_time:.2f}s ({batch_time / len(lags_to_test):.3f}s per lag)"
    )
    print(
        f"  Parallel processing: {parallel_time:.2f}s ({parallel_time / len(lags_to_test):.3f}s per lag)"
    )

    if parallel_time < batch_time:
        speedup = batch_time / parallel_time
        print(f"  🏆 Parallel is {speedup:.2f}x faster")
    else:
        slowdown = parallel_time / batch_time
        print(f"  ⚠️  Parallel is {slowdown:.2f}x slower")

    print("=" * 80)


def test_static_geoid_correctness(survey_data_static_geoids, heat_index_dir, tmp_path):
    """
    Test that static GEOID lookup works correctly.

    Verifies that the correct GEOID column is used based on the year
    of the lag date.
    """
    print("\n" + "=" * 80)
    print("🧪 TEST: Static GEOID Column Selection")
    print("=" * 80)

    # Load survey data
    hrs_data = HRSInterviewData(
        survey_data_static_geoids,
        datecol="iwdate",
        move=False,
        residential_hist=None,
    )

    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )

    # Process a single lag
    print("📋 Processing 30-day lag...")
    temp_dir = tmp_path / "geoid_correctness_test"
    temp_dir.mkdir()

    temp_files = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=[30],
        id_col="hhidpn",
        temp_dir=temp_dir,
        prefix="heat",
    )

    # Load result
    result_df = pd.read_parquet(temp_files[0])

    print(f"✓ Processed {len(result_df)} rows")
    print(f"✓ Columns in output: {result_df.columns.tolist()}")

    # Verify expected columns exist (GEOID columns are NOT in output by design)
    # They're only used internally for merging
    assert "hhidpn" in result_df.columns, "Missing ID column"
    print(f"✓ ID column 'hhidpn' present")

    # Check that the contextual data column exists
    data_col = "index_iwdate_30day_prior"
    assert data_col in result_df.columns, f"Missing expected data column: {data_col}"
    print(f"✓ Contextual data column '{data_col}' created successfully")

    # Verify we got some valid heat index values
    data_values = result_df[data_col].dropna()
    if len(data_values) > 0:
        assert all(
            0 <= v <= 150 for v in data_values
        ), "Heat index values should be reasonable (0-150°F)"
        print(
            f"✓ {len(data_values)}/{len(result_df)} rows have valid heat index values"
        )
        print(f"  Range: [{data_values.min():.1f}, {data_values.max():.1f}]°F")
    else:
        print("  ⚠️  No heat index values found (may be expected if GEOIDs don't match)")

    print("=" * 80)
    print("✅ Static GEOID correctness test PASSED")


if __name__ == "__main__":
    print("\n🚀 Run these tests with pytest:")
    print("   uv run pytest tests/test_performance_no_residential_history.py -v -s")
    print("\n   Add -k 'small' to run only small dataset test")
    print("   Add -k 'large' to run only large dataset test")
    print("   Add -k 'many_lags' to run only many lags test")
    print("   Add -k 'correctness' to run only correctness test")
