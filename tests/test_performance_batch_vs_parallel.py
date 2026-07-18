"""
Performance test: Batch vs Parallel processing with larger survey datasets.

This script tests and compares the processing time for batch vs parallel
linkage processing with different dataset sizes and lag counts.
"""

import time
import pytest
import pandas as pd
from pathlib import Path

from stitch.hrs import ResidentialHistoryHRS, HRSInterviewData
from stitch.daily_measure import DailyMeasureDataDir
from stitch.process import (
    process_multiple_lags_batch,
    process_multiple_lags_parallel,
)


def create_large_survey_data(base_data_path: Path, n_copies: int = 10) -> pd.DataFrame:
    """
    Create a larger survey dataset by replicating base data.

    Note: IDs are NOT modified - the same people appear multiple times.
    This is fine for performance testing since we just need more rows to process.
    The residential history will work because the IDs remain the same.

    Args:
        base_data_path: Path to base survey data file
        n_copies: Number of times to replicate the data

    Returns:
        DataFrame with replicated survey data
    """
    base_df = pd.read_stata(base_data_path)

    dfs = []
    for i in range(n_copies):
        df_copy = base_df.copy()
        # Don't modify IDs - residential history needs to match!
        dfs.append(df_copy)

    large_df = pd.concat(dfs, ignore_index=True)
    print(f"  Created large dataset: {large_df.shape[0]} rows ({n_copies}x original)")
    return large_df


def test_performance_small_dataset(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """
    Performance test with small dataset (baseline).
    """
    print("\n" + "=" * 80)
    print("⚡ PERFORMANCE TEST: Small Dataset (Baseline)")
    print("=" * 80)

    # Load data
    residential_hist = ResidentialHistoryHRS(fake_residential_history_file)
    hrs_data = HRSInterviewData(
        survey_data_2016_2020,
        datecol="iwdate",
        move=True,
        residential_hist=residential_hist,
    )
    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )

    print(f"📊 Dataset size: {hrs_data.df.shape[0]} rows")

    lags_to_test = [0, 1, 7, 14, 30, 60, 90]
    print(f"🔢 Number of lags: {len(lags_to_test)}")
    print(f"📋 Lags: {lags_to_test}")

    results = {}

    # Test batch processing
    print("\n🔄 Testing BATCH processing...")
    temp_dir_batch = tmp_path / "small_batch"
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
    temp_dir_parallel = tmp_path / "small_parallel"
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
    # Compare specific lag file (lag 0) to ensure we're comparing the same data
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


def test_performance_large_dataset(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """
    Performance test with larger dataset (10x replicated).
    """
    print("\n" + "=" * 80)
    print("⚡ PERFORMANCE TEST: Large Dataset (10x replicated)")
    print("=" * 80)

    # Load residential history
    residential_hist = ResidentialHistoryHRS(fake_residential_history_file)

    # Create large survey data
    print("📥 Creating large survey dataset...")
    large_survey_df = create_large_survey_data(survey_data_2016_2020, n_copies=10)

    # Save to temporary file and reload through HRSInterviewData
    temp_survey_path = tmp_path / "large_survey.dta"
    large_survey_df.to_stata(temp_survey_path)

    hrs_data = HRSInterviewData(
        temp_survey_path,
        datecol="iwdate",
        move=True,
        residential_hist=residential_hist,
    )

    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )

    print(f"📊 Dataset size: {hrs_data.df.shape[0]} rows")

    lags_to_test = [0, 1, 7, 14, 30, 60, 90]
    print(f"🔢 Number of lags: {len(lags_to_test)}")

    results = {}

    # Test batch processing
    print("\n🔄 Testing BATCH processing...")
    temp_dir_batch = tmp_path / "large_batch"
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
    temp_dir_parallel = tmp_path / "large_parallel"
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


def test_performance_many_lags(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """
    Performance test with many lags (100 lags).
    """
    print("\n" + "=" * 80)
    print("⚡ PERFORMANCE TEST: Many Lags (100 lags)")
    print("=" * 80)

    # Load data
    residential_hist = ResidentialHistoryHRS(fake_residential_history_file)
    hrs_data = HRSInterviewData(
        survey_data_2016_2020,
        datecol="iwdate",
        move=True,
        residential_hist=residential_hist,
    )
    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )

    print(f"📊 Dataset size: {hrs_data.df.shape[0]} rows")

    # Test with many lags (0-99)
    lags_to_test = list(range(100))
    print(f"🔢 Number of lags: {len(lags_to_test)}")

    results = {}

    # Test batch processing
    print("\n🔄 Testing BATCH processing...")
    temp_dir_batch = tmp_path / "many_lags_batch"
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
    temp_dir_parallel = tmp_path / "many_lags_parallel"
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


@pytest.mark.slow
def test_comprehensive_performance_comparison(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """
    Comprehensive performance comparison across different scenarios.
    """
    print("\n" + "=" * 80)
    print("📊 COMPREHENSIVE PERFORMANCE COMPARISON")
    print("=" * 80)

    # Load base data
    residential_hist = ResidentialHistoryHRS(fake_residential_history_file)
    base_hrs_data = HRSInterviewData(
        survey_data_2016_2020,
        datecol="iwdate",
        move=True,
        residential_hist=residential_hist,
    )
    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )

    # Test scenarios: (dataset_multiplier, lag_counts)
    scenarios = [
        (1, [0, 7, 30]),  # Small dataset, few lags
        (1, list(range(50))),  # Small dataset, many lags
        (5, [0, 7, 30]),  # Medium dataset, few lags
        (5, list(range(50))),  # Medium dataset, many lags
    ]

    results_table = []

    for dataset_mult, lags in scenarios:
        print(f"\n{'─' * 80}")
        scenario_name = f"{dataset_mult}x dataset, {len(lags)} lags"
        print(f"🧪 Testing: {scenario_name}")

        # Create dataset
        if dataset_mult == 1:
            hrs_data = base_hrs_data
        else:
            large_survey_df = create_large_survey_data(
                survey_data_2016_2020, n_copies=dataset_mult
            )
            temp_survey_path = tmp_path / f"survey_{dataset_mult}x.dta"
            large_survey_df.to_stata(temp_survey_path)
            hrs_data = HRSInterviewData(
                temp_survey_path,
                datecol="iwdate",
                move=True,
                residential_hist=residential_hist,
            )

        n_rows = hrs_data.df.shape[0]
        print(f"  Rows: {n_rows}, Lags: {len(lags)}")

        # Batch processing
        temp_dir_batch = tmp_path / f"scenario_batch_{dataset_mult}x_{len(lags)}lags"
        temp_dir_batch.mkdir(exist_ok=True)

        start = time.time()
        process_multiple_lags_batch(
            hrs_data=hrs_data,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="hhidpn",
            temp_dir=temp_dir_batch,
            prefix="heat",
        )
        batch_time = time.time() - start

        # Parallel processing
        temp_dir_parallel = (
            tmp_path / f"scenario_parallel_{dataset_mult}x_{len(lags)}lags"
        )
        temp_dir_parallel.mkdir(exist_ok=True)

        start = time.time()
        process_multiple_lags_parallel(
            hrs_data=hrs_data,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="hhidpn",
            temp_dir=temp_dir_parallel,
            prefix="heat",
            max_workers=4,
        )
        parallel_time = time.time() - start

        speedup = batch_time / parallel_time

        results_table.append(
            {
                "scenario": scenario_name,
                "rows": n_rows,
                "lags": len(lags),
                "batch_time": batch_time,
                "parallel_time": parallel_time,
                "speedup": speedup,
            }
        )

        print(
            f"  Batch: {batch_time:.2f}s | Parallel: {parallel_time:.2f}s | Speedup: {speedup:.2f}x"
        )

    # Print summary table
    print(f"\n{'=' * 80}")
    print("📊 FINAL RESULTS SUMMARY")
    print(f"{'=' * 80}")
    print(
        f"{'Scenario':<30} {'Rows':<8} {'Lags':<6} {'Batch':<10} {'Parallel':<10} {'Speedup':<10}"
    )
    print(f"{'-' * 80}")

    for result in results_table:
        print(
            f"{result['scenario']:<30} "
            f"{result['rows']:<8} "
            f"{result['lags']:<6} "
            f"{result['batch_time']:>8.2f}s "
            f"{result['parallel_time']:>8.2f}s "
            f"{result['speedup']:>8.2f}x"
        )

    print(f"{'=' * 80}")

    # Summary insights
    avg_speedup = sum(r["speedup"] for r in results_table) / len(results_table)
    print(f"\n💡 INSIGHTS:")
    print(f"  Average speedup: {avg_speedup:.2f}x")

    best_scenario = max(results_table, key=lambda x: x["speedup"])
    print(
        f"  Best speedup: {best_scenario['speedup']:.2f}x ({best_scenario['scenario']})"
    )

    worst_scenario = min(results_table, key=lambda x: x["speedup"])
    print(
        f"  Worst speedup: {worst_scenario['speedup']:.2f}x ({worst_scenario['scenario']})"
    )

    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    print("\n🚀 Run this script with pytest to execute performance tests:")
    print("   pytest tests/test_performance_batch_vs_parallel.py -v -s")
    print("\n   Add -k 'small' to run only small dataset test")
    print("   Add -k 'large' to run only large dataset test")
    print("   Add -k 'many_lags' to run only many lags test")
    print("   Add -m slow to run comprehensive test")
