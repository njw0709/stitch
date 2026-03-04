"""
Tests for multi-column linkage functionality.

This module tests the ability to link multiple data columns from contextual
data sources simultaneously (e.g., linking both tmax and tmin, or multiple
air quality metrics at once).
"""

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
def multicolumn_weather_dir(tmp_path, real_geoid_pool):
    """
    Create a contextual data directory with multiple weather columns.

    This fixture creates weather data with three columns:
    - tmax: Maximum temperature
    - tmin: Minimum temperature
    - humidity: Relative humidity
    """
    weather_dir = tmp_path / "weather_data"
    weather_dir.mkdir()

    print("\n📊 Creating multi-column weather data...")

    # Sample subset of GEOIDs for testing
    test_geoids = np.random.choice(
        real_geoid_pool, size=min(50, len(real_geoid_pool)), replace=False
    )

    # Create data for years 2016-2020
    for year in range(2016, 2021):
        rows = []

        # Generate daily data for each GEOID
        for geoid in test_geoids:
            # Create ~30 days of data per GEOID per year (to keep test data small)
            for day_offset in range(0, 365, 12):  # Every 12th day
                date = pd.Timestamp(f"{year}-01-01") + pd.Timedelta(days=day_offset)
                if date.year == year:  # Stay within the year
                    # Generate realistic weather values
                    base_temp = 60 + 20 * np.sin(
                        2 * np.pi * day_offset / 365
                    )  # Seasonal variation
                    tmax = base_temp + np.random.uniform(5, 15)
                    tmin = base_temp - np.random.uniform(5, 15)
                    humidity = np.random.uniform(30, 90)

                    rows.append(
                        {
                            "Date": date.strftime("%Y-%m-%d"),
                            "GEOID10": geoid,
                            "tmax": round(tmax, 2),
                            "tmin": round(tmin, 2),
                            "humidity": round(humidity, 2),
                        }
                    )

        # Save to CSV
        df = pd.DataFrame(rows)
        file_path = weather_dir / f"{year}_daily_weather.csv"
        df.to_csv(file_path, index=False)
        print(f"  Created {file_path.name}: {len(df)} rows, {len(test_geoids)} GEOIDs")

    print(f"✓ Created multi-column weather data in {weather_dir}")
    return weather_dir


@pytest.fixture
def survey_data_multicolumn(tmp_path, real_geoid_pool):
    """Create survey data for multi-column linkage testing."""
    from tests.data_generators import generate_fake_hhidpn

    n_people = 30
    hhidpns = generate_fake_hhidpn(n_people)
    rows = []

    # Use subset of GEOIDs that match the weather data
    test_geoids = np.random.choice(
        real_geoid_pool, size=min(50, len(real_geoid_pool)), replace=False
    )

    for hhidpn in hhidpns:
        # Generate interview date between 2016-2020
        interview_year = np.random.randint(2016, 2021)
        interview_month = np.random.randint(1, 13)
        interview_day = np.random.randint(1, 29)

        iwdate = pd.Timestamp(
            f"{interview_year}-{interview_month:02d}-{interview_day:02d}"
        )

        # Use GEOIDs that exist in weather data
        geoid = np.random.choice(test_geoids)

        rows.append(
            {
                "hhidpn": hhidpn,
                "iwdate": iwdate,
                "GEOID2010": geoid,
                "age": np.random.randint(50, 90),
                "gender": np.random.choice(["Male", "Female"]),
            }
        )

    df = pd.DataFrame(rows)

    # Save to temporary Stata file
    file_path = tmp_path / "survey_multicolumn.dta"
    df.to_stata(file_path, write_index=False)

    print(f"\n📋 Created survey data: {len(df)} rows")
    return file_path


def test_multicolumn_linkage_batch(
    survey_data_multicolumn, multicolumn_weather_dir, tmp_path
):
    """
    Test linking multiple columns at once using batch processing.

    This test verifies that multiple data columns (tmax, tmin, humidity)
    can be linked simultaneously from the same contextual data source.
    """
    print("\n" + "=" * 80)
    print("🧪 TEST: Multi-Column Linkage (Batch Processing)")
    print("=" * 80)

    # Load survey data
    print("📥 Loading survey data...")
    hrs_data = HRSInterviewData(
        survey_data_multicolumn,
        datecol="iwdate",
        move=False,
        residential_hist=None,
    )
    print(f"  Survey data: {hrs_data.df.shape[0]} rows")

    # Load weather data with multiple columns
    print("📥 Initializing multi-column weather data...")
    weather_data = DailyMeasureDataDir(
        multicolumn_weather_dir,
        data_col=["tmax", "tmin", "humidity"],  # ← Multiple columns!
        measure_type=None,
    )
    print(f"  Data columns: {weather_data['2016'].data_col}")
    print(f"  Available years: {weather_data.list_years()}")

    # Process multiple lags
    lags_to_test = [0, 7, 30]
    print(f"\n🔄 Processing lags: {lags_to_test}")
    print(
        f"  Expected output: {len(lags_to_test) * 3} columns (3 data cols × {len(lags_to_test)} lags)"
    )

    temp_dir = tmp_path / "multicolumn_batch"
    temp_dir.mkdir()

    temp_files = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=weather_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir,
        prefix="weather",
    )

    print(f"\n📁 Generated {len(temp_files)} temp files")

    # Merge all lag outputs
    print("📎 Merging lag outputs...")
    final_df = hrs_data.df[["hhidpn"]].copy()

    for f in temp_files:
        lag_df = pd.read_parquet(f)
        final_df = final_df.merge(lag_df, on="hhidpn", how="left")

    print(f"  Final dataset shape: {final_df.shape}")
    print(f"  Columns: {sorted([c for c in final_df.columns if c != 'hhidpn'])}")

    # Validate output
    print("\n✓ Validating multi-column linkage...")

    # Check that all expected columns were created
    expected_columns = []
    for lag in lags_to_test:
        for col in ["tmax", "tmin", "humidity"]:
            expected_columns.append(f"{col}_iwdate_{lag}day_prior")

    print(f"  Expected {len(expected_columns)} data columns")
    for col in expected_columns:
        assert col in final_df.columns, f"Missing expected column: {col}"
        print(f"    ✓ {col}")

    # Verify data ranges are reasonable
    print("\n✓ Validating data ranges...")

    # Check temperature columns
    for lag in lags_to_test:
        tmax_col = f"tmax_iwdate_{lag}day_prior"
        tmin_col = f"tmin_iwdate_{lag}day_prior"

        tmax_vals = final_df[tmax_col].dropna()
        tmin_vals = final_df[tmin_col].dropna()

        if len(tmax_vals) > 0:
            assert tmax_vals.min() >= 0, f"Temperature should be positive in {tmax_col}"
            assert (
                tmax_vals.max() <= 150
            ), f"Temperature should be reasonable in {tmax_col}"
            print(
                f"  ✓ {tmax_col}: {len(tmax_vals)} values, range [{tmax_vals.min():.1f}, {tmax_vals.max():.1f}]°F"
            )

        if len(tmin_vals) > 0:
            assert tmin_vals.min() >= 0, f"Temperature should be positive in {tmin_col}"
            assert (
                tmin_vals.max() <= 150
            ), f"Temperature should be reasonable in {tmin_col}"
            print(
                f"  ✓ {tmin_col}: {len(tmin_vals)} values, range [{tmin_vals.min():.1f}, {tmin_vals.max():.1f}]°F"
            )

    # Check humidity columns
    for lag in lags_to_test:
        humidity_col = f"humidity_iwdate_{lag}day_prior"
        humidity_vals = final_df[humidity_col].dropna()

        if len(humidity_vals) > 0:
            assert (
                humidity_vals.min() >= 0
            ), f"Humidity should be >= 0 in {humidity_col}"
            assert (
                humidity_vals.max() <= 100
            ), f"Humidity should be <= 100 in {humidity_col}"
            print(
                f"  ✓ {humidity_col}: {len(humidity_vals)} values, range [{humidity_vals.min():.1f}, {humidity_vals.max():.1f}]%"
            )

    print("\n✅ Multi-column batch linkage test PASSED!")
    print("=" * 80)


def test_multicolumn_linkage_parallel(
    survey_data_multicolumn, multicolumn_weather_dir, tmp_path
):
    """
    Test linking multiple columns at once using parallel processing.

    This test verifies that multi-column linkage works correctly with
    parallel processing.
    """
    print("\n" + "=" * 80)
    print("🚀 TEST: Multi-Column Linkage (Parallel Processing)")
    print("=" * 80)

    # Load survey data
    print("📥 Loading survey data...")
    hrs_data = HRSInterviewData(
        survey_data_multicolumn,
        datecol="iwdate",
        move=False,
        residential_hist=None,
    )

    # Load weather data with multiple columns
    print("📥 Initializing multi-column weather data...")
    weather_data = DailyMeasureDataDir(
        multicolumn_weather_dir,
        data_col=["tmax", "tmin", "humidity"],  # ← Multiple columns!
        measure_type=None,
    )

    # Process multiple lags in parallel
    lags_to_test = [0, 7, 30]
    print(f"\n🚀 Parallel processing lags: {lags_to_test}")

    temp_dir = tmp_path / "multicolumn_parallel"
    temp_dir.mkdir()

    temp_files = process_multiple_lags_parallel(
        hrs_data=hrs_data,
        contextual_dir=weather_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir,
        prefix="weather",
        max_workers=4,
    )

    print(f"\n📁 Generated {len(temp_files)} temp files")

    # Merge all lag outputs
    print("📎 Merging lag outputs...")
    final_df = hrs_data.df[["hhidpn"]].copy()

    for f in temp_files:
        lag_df = pd.read_parquet(f)
        final_df = final_df.merge(lag_df, on="hhidpn", how="left")

    print(f"  Final dataset shape: {final_df.shape}")

    # Validate output
    print("\n✓ Validating multi-column linkage...")

    # Check that all expected columns were created
    expected_columns = []
    for lag in lags_to_test:
        for col in ["tmax", "tmin", "humidity"]:
            expected_columns.append(f"{col}_iwdate_{lag}day_prior")

    for col in expected_columns:
        assert col in final_df.columns, f"Missing expected column: {col}"

    print(f"  ✓ All {len(expected_columns)} expected columns present")

    print("\n✅ Multi-column parallel linkage test PASSED!")
    print("=" * 80)


def test_multicolumn_batch_vs_parallel_consistency(
    survey_data_multicolumn, multicolumn_weather_dir, tmp_path
):
    """
    Test that batch and parallel processing produce identical results for multi-column linkage.
    """
    print("\n" + "=" * 80)
    print("🔍 TEST: Multi-Column Batch vs Parallel Consistency")
    print("=" * 80)

    # Load data
    hrs_data = HRSInterviewData(
        survey_data_multicolumn,
        datecol="iwdate",
        move=False,
        residential_hist=None,
    )

    weather_data = DailyMeasureDataDir(
        multicolumn_weather_dir,
        data_col=["tmax", "tmin", "humidity"],
        measure_type=None,
    )

    lags_to_test = [0, 7]

    # Batch processing
    print("🔄 Running batch processing...")
    temp_dir_batch = tmp_path / "consistency_batch"
    temp_dir_batch.mkdir()

    temp_files_batch = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=weather_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir_batch,
        prefix="weather",
    )

    # Parallel processing
    print("🚀 Running parallel processing...")
    temp_dir_parallel = tmp_path / "consistency_parallel"
    temp_dir_parallel.mkdir()

    temp_files_parallel = process_multiple_lags_parallel(
        hrs_data=hrs_data,
        contextual_dir=weather_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir_parallel,
        prefix="weather",
        max_workers=4,
    )

    # Compare results for each lag
    print("\n🔍 Comparing batch vs parallel results...")

    for batch_file, parallel_file in zip(
        sorted(temp_files_batch), sorted(temp_files_parallel)
    ):
        print(f"  Comparing {batch_file.name} vs {parallel_file.name}")

        batch_df = (
            pd.read_parquet(batch_file).sort_values("hhidpn").reset_index(drop=True)
        )
        parallel_df = (
            pd.read_parquet(parallel_file).sort_values("hhidpn").reset_index(drop=True)
        )

        # Use pandas testing utility for NaN-aware comparison
        pd.testing.assert_frame_equal(
            batch_df,
            parallel_df,
            check_dtype=False,
            obj=f"Batch vs parallel for {batch_file.name}",
        )
        print(f"    ✓ Results are identical")

    print("\n✅ Batch and parallel processing produce identical results!")
    print("=" * 80)


def test_multicolumn_single_vs_multi_comparison(
    survey_data_multicolumn, multicolumn_weather_dir, tmp_path
):
    """
    Test that linking multiple columns at once produces the same results
    as linking each column separately.
    """
    print("\n" + "=" * 80)
    print("🔍 TEST: Single-Column vs Multi-Column Linkage Comparison")
    print("=" * 80)

    hrs_data = HRSInterviewData(
        survey_data_multicolumn,
        datecol="iwdate",
        move=False,
        residential_hist=None,
    )

    lag_to_test = 7

    # Multi-column approach: Link all columns at once
    print("\n📊 Approach 1: Link all columns at once...")
    weather_data_multi = DailyMeasureDataDir(
        multicolumn_weather_dir,
        data_col=["tmax", "tmin", "humidity"],
        measure_type=None,
    )

    temp_dir_multi = tmp_path / "multi_approach"
    temp_dir_multi.mkdir()

    temp_files_multi = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=weather_data_multi,
        n_days=[lag_to_test],
        id_col="hhidpn",
        temp_dir=temp_dir_multi,
        prefix="weather",
    )

    df_multi = pd.read_parquet(temp_files_multi[0])

    # Single-column approach: Link each column separately
    print("\n📊 Approach 2: Link each column separately...")
    dfs_single = []

    for col in ["tmax", "tmin", "humidity"]:
        print(f"  Linking {col}...")
        weather_data_single = DailyMeasureDataDir(
            multicolumn_weather_dir,
            data_col=col,  # Single column
            measure_type=None,
        )

        temp_dir_single = tmp_path / f"single_{col}"
        temp_dir_single.mkdir()

        temp_files_single = process_multiple_lags_batch(
            hrs_data=hrs_data,
            contextual_dir=weather_data_single,
            n_days=[lag_to_test],
            id_col="hhidpn",
            temp_dir=temp_dir_single,
            prefix="weather",
        )

        dfs_single.append(pd.read_parquet(temp_files_single[0]))

    # Merge single-column results
    df_single_merged = dfs_single[0]
    for df in dfs_single[1:]:
        df_single_merged = df_single_merged.merge(df, on="hhidpn", how="outer")

    # Compare results
    print("\n🔍 Comparing results...")

    # Sort both dataframes by ID
    df_multi_sorted = df_multi.sort_values("hhidpn").reset_index(drop=True)
    df_single_sorted = df_single_merged.sort_values("hhidpn").reset_index(drop=True)

    # Ensure columns are in the same order
    columns_sorted = sorted(df_multi_sorted.columns)
    df_multi_sorted = df_multi_sorted[columns_sorted]
    df_single_sorted = df_single_sorted[columns_sorted]

    print(f"  Multi-column result shape: {df_multi_sorted.shape}")
    print(f"  Single-column merged shape: {df_single_sorted.shape}")

    # Compare
    pd.testing.assert_frame_equal(
        df_multi_sorted,
        df_single_sorted,
        check_dtype=False,
        obj="Multi-column vs single-column approach",
    )

    print("  ✓ Results are identical!")
    print(
        "\n✅ Multi-column linkage produces same results as separate single-column linkages!"
    )
    print("=" * 80)


def test_multicolumn_with_residential_history(
    fake_residential_history_file,
    survey_data_multicolumn,
    multicolumn_weather_dir,
    tmp_path,
):
    """
    Test multi-column linkage with residential history support.

    This ensures that multi-column linkage works correctly when participants
    have moved between different geographic locations.
    """
    print("\n" + "=" * 80)
    print("🏠 TEST: Multi-Column Linkage with Residential History")
    print("=" * 80)

    from stitch.hrs import ResidentialHistoryHRS

    # Load residential history
    print("📥 Loading residential history...")
    residential_hist = ResidentialHistoryHRS(
        fake_residential_history_file, first_tract_mark="999.0"
    )

    # Load survey data with residential history
    print("📥 Loading survey data with residential history...")
    hrs_data = HRSInterviewData(
        survey_data_multicolumn,
        datecol="iwdate",
        move=True,  # ← Enable residential history
        residential_hist=residential_hist,
    )

    # Load weather data with multiple columns
    print("📥 Initializing multi-column weather data...")
    weather_data = DailyMeasureDataDir(
        multicolumn_weather_dir,
        data_col=["tmax", "tmin", "humidity"],
        measure_type=None,
    )

    # Process lags
    lags_to_test = [0, 30]
    print(f"\n🔄 Processing lags with residential history: {lags_to_test}")

    temp_dir = tmp_path / "multicolumn_with_reshist"
    temp_dir.mkdir()

    temp_files = process_multiple_lags_batch(
        hrs_data=hrs_data,
        contextual_dir=weather_data,
        n_days=lags_to_test,
        id_col="hhidpn",
        temp_dir=temp_dir,
        prefix="weather",
    )

    print(f"\n📁 Generated {len(temp_files)} temp files")

    # Merge outputs
    final_df = hrs_data.df[["hhidpn"]].copy()
    for f in temp_files:
        lag_df = pd.read_parquet(f)
        final_df = final_df.merge(lag_df, on="hhidpn", how="left")

    print(f"  Final dataset shape: {final_df.shape}")

    # Validate
    print("\n✓ Validating output...")

    expected_columns = []
    for lag in lags_to_test:
        for col in ["tmax", "tmin", "humidity"]:
            expected_columns.append(f"{col}_iwdate_{lag}day_prior")

    for col in expected_columns:
        assert col in final_df.columns, f"Missing expected column: {col}"

    print(f"  ✓ All {len(expected_columns)} expected columns present")

    print("\n✅ Multi-column linkage with residential history test PASSED!")
    print("=" * 80)


if __name__ == "__main__":
    print("\n🚀 Run these tests with pytest:")
    print("   uv run pytest tests/test_multicolumn_linkage.py -v -s")
    print("\n   Add -k 'batch' to run only batch tests")
    print("   Add -k 'parallel' to run only parallel tests")
    print("   Add -k 'consistency' to run consistency tests")
    print("   Add -k 'residential' to run residential history tests")
