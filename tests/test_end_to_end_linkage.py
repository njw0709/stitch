"""
End-to-end integration tests for the complete data linkage workflow.

This module tests the full workflow from step1 script including:
- Loading residential history and survey data
- Loading real heat index data (2016-2020)
- Processing multiple lag periods (sequential and parallel)
- Merging all outputs into final dataset
- Validating correct linkage based on residential moves
"""

import pytest
import pandas as pd

from stitch.hrs import ResidentialHistoryHRS, HRSInterviewData
from stitch.daily_measure import DailyMeasureDataDir
from stitch.process import (
    process_multiple_lags_batch,
    process_multiple_lags_parallel,
)


def test_batch_processing_workflow(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """
    Test the complete end-to-end linkage workflow with batch processing.

    This test validates:
    - Loading residential history and survey data
    - Initializing heat index data
    - Processing multiple lag periods using optimized batch processing
    - Merging lag outputs
    - Final data validation
    """
    print("\n" + "=" * 60)
    print("🧪 Testing Batch Processing Linkage Workflow")
    print("=" * 60)

    # Step 1: Load residential history
    print("📥 Loading residential history...")
    residential_hist = ResidentialHistoryHRS(
        fake_residential_history_file, first_tract_mark="999.0"
    )

    # Step 2: Load survey data
    print("📥 Loading survey data (2016-2020)...")
    hrs_data = HRSInterviewData(
        survey_data_2016_2020,
        datecol="iwdate",
        move=True,
        residential_hist=residential_hist,
    )

    print(f"  Survey data shape: {hrs_data.df.shape}")
    print(
        f"  Date range: {hrs_data.df['iwdate'].min()} to {hrs_data.df['iwdate'].max()}"
    )

    # Step 3: Initialize heat index data
    print("📥 Initializing heat index data...")
    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )
    print(f"  Available years: {heat_data.list_years()}")

    # Step 4: Use batch processing
    lags_to_test = [0, 7, 30]
    print(f"\n🔄 Testing batch processing for lags: {lags_to_test}")

    temp_dir = tmp_path / "batch_lags"
    temp_dir.mkdir()

    temp_files = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir,
        prefix="heat",
    )

    print(f"\n📁 Generated {len(temp_files)} temp files")

    # Step 5: Merge all lag outputs
    print("📎 Merging lag outputs with survey data...")
    final_df = hrs_data.df[["hhidpn"]].copy()

    for f in temp_files:
        lag_df = pd.read_parquet(f)
        final_df = final_df.merge(lag_df, on="hhidpn", how="left")

    print(f"  Final dataset shape: {final_df.shape}")
    print(f"  Final columns: {final_df.columns.tolist()}")

    # Step 6: Validate output
    print("✓ Validating output...")

    # Check all people are present
    assert len(final_df) == len(hrs_data.df), "All people should be in final dataset"

    # Check lag columns were created
    expected_lag_cols = [f"index_iwdate_{n}day_prior" for n in lags_to_test]
    for col in expected_lag_cols:
        assert col in final_df.columns, f"Missing lag column: {col}"
        print(f"  ✓ Found column: {col}")

    # Check heat values are reasonable
    for col in expected_lag_cols:
        non_null_values = final_df[col].dropna()
        if len(non_null_values) > 0:
            assert (
                non_null_values.min() >= 0
            ), f"Heat values should be positive in {col}"
            assert (
                non_null_values.max() <= 150
            ), f"Heat values should be reasonable in {col}"
            print(
                f"  ✓ {col}: {len(non_null_values)} non-null values, "
                f"range [{non_null_values.min():.1f}, {non_null_values.max():.1f}]"
            )

    print("\n✅ Batch processing workflow completed successfully!")
    print("=" * 60)


def test_parallel_processing_workflow(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """
    Test the complete end-to-end linkage workflow with parallel processing.

    This test validates:
    - Loading residential history and survey data
    - Initializing heat index data
    - Processing multiple lag periods using optimized parallel processing
    - Merging lag outputs
    - Final data validation
    """
    print("\n" + "=" * 60)
    print("🚀 Testing Parallel Processing Linkage Workflow")
    print("=" * 60)

    # Step 1: Load residential history
    print("📥 Loading residential history...")
    residential_hist = ResidentialHistoryHRS(
        fake_residential_history_file, first_tract_mark="999.0"
    )

    # Step 2: Load survey data
    print("📥 Loading survey data (2016-2020)...")
    hrs_data = HRSInterviewData(
        survey_data_2016_2020,
        datecol="iwdate",
        move=True,
        residential_hist=residential_hist,
    )

    print(f"  Survey data shape: {hrs_data.df.shape}")

    # Step 3: Initialize heat index data
    print("📥 Initializing heat index data...")
    heat_data = DailyMeasureDataDir(
        heat_index_dir,
        data_col="index",
        measure_type=None,
    )
    print(f"  Available years: {heat_data.list_years()}")

    # Step 4: Use parallel batch processing
    lags_to_test = [0, 7, 30]
    print(f"\n🔄 Testing parallel batch processing for lags: {lags_to_test}")

    temp_dir = tmp_path / "batch_lags_parallel"
    temp_dir.mkdir()

    temp_files = process_multiple_lags_parallel(
        hrs_data=hrs_data,
        contextual_dir=heat_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir,
        prefix="heat",
        max_workers=4,  # Limit to 4 workers for testing
    )

    print(f"\n📁 Generated {len(temp_files)} temp files")

    # Step 5: Merge all lag outputs
    print("📎 Merging lag outputs with survey data...")
    final_df = hrs_data.df[["hhidpn"]].copy()

    for f in temp_files:
        lag_df = pd.read_parquet(f)
        final_df = final_df.merge(lag_df, on="hhidpn", how="left")

    print(f"  Final dataset shape: {final_df.shape}")

    # Step 6: Validate output
    print("✓ Validating output...")

    # Check all people are present
    assert len(final_df) == len(hrs_data.df), "All people should be in final dataset"

    # Check lag columns were created
    expected_lag_cols = [f"index_iwdate_{n}day_prior" for n in lags_to_test]
    for col in expected_lag_cols:
        assert col in final_df.columns, f"Missing lag column: {col}"
        print(f"  ✓ Found column: {col}")

    # Check heat values are reasonable
    for col in expected_lag_cols:
        non_null_values = final_df[col].dropna()
        if len(non_null_values) > 0:
            assert (
                non_null_values.min() >= 0
            ), f"Heat values should be positive in {col}"
            assert (
                non_null_values.max() <= 150
            ), f"Heat values should be reasonable in {col}"
            print(
                f"  ✓ {col}: {len(non_null_values)} non-null values, "
                f"range [{non_null_values.min():.1f}, {non_null_values.max():.1f}]"
            )

    print("\n✅ Parallel processing workflow completed successfully!")
    print("=" * 60)


