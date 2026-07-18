"""
Test integer normalization for hhidpn in ResidentialHistoryHRS and HRSInterviewData.

This module tests that PIDs are properly normalized to integers regardless of how
they're loaded (as float, string, or int), ensuring consistent key lookups.
"""

import pandas as pd
import pytest
from pathlib import Path
import tempfile

from stitch.hrs import ResidentialHistoryHRS, HRSInterviewData


@pytest.fixture
def sample_geoid_pool():
    """Sample GEOIDs for testing."""
    return [
        "06037930401",
        "17031081403",
        "36047023900",
        "48201253513",
        "12086003602",
    ]


@pytest.fixture
def residential_data_with_float_pids(tmp_path, sample_geoid_pool):
    """Create residential history data with float PIDs (e.g., 10000001.0)."""
    data = []
    base_pid = 10000001

    for i in range(5):
        pid = float(base_pid + i)  # Explicitly float
        # Survey entry
        data.append(
            {
                "hhidpn": pid,
                "move_date": "2010-01-15",
                "GEOID": sample_geoid_pool[i],
            }
        )
        # One move
        data.append(
            {
                "hhidpn": pid,
                "move_date": "2015-06-10",
                "GEOID": sample_geoid_pool[(i + 1) % len(sample_geoid_pool)],
            }
        )

    df = pd.DataFrame(data)
    file_path = tmp_path / "residential_float_pids.csv"
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def residential_data_with_string_pids(tmp_path, sample_geoid_pool):
    """Create residential history data with string PIDs (e.g., '10000001')."""
    data = []
    base_pid = 10000001

    for i in range(5):
        pid = str(base_pid + i)  # Explicitly string
        # Survey entry
        data.append(
            {
                "hhidpn": pid,
                "move_date": "2010-01-15",
                "GEOID": sample_geoid_pool[i],
            }
        )
        # One move
        data.append(
            {
                "hhidpn": pid,
                "move_date": "2015-06-10",
                "GEOID": sample_geoid_pool[(i + 1) % len(sample_geoid_pool)],
            }
        )

    df = pd.DataFrame(data)
    file_path = tmp_path / "residential_string_pids.csv"
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def residential_data_with_int_pids(tmp_path, sample_geoid_pool):
    """Create residential history data with int PIDs (e.g., 10000001)."""
    data = []
    base_pid = 10000001

    for i in range(5):
        pid = int(base_pid + i)  # Explicitly int
        # Survey entry
        data.append(
            {
                "hhidpn": pid,
                "move_date": "2010-01-15",
                "GEOID": sample_geoid_pool[i],
            }
        )
        # One move
        data.append(
            {
                "hhidpn": pid,
                "move_date": "2015-06-10",
                "GEOID": sample_geoid_pool[(i + 1) % len(sample_geoid_pool)],
            }
        )

    df = pd.DataFrame(data)
    file_path = tmp_path / "residential_int_pids.csv"
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def survey_data_with_float_pids(tmp_path, sample_geoid_pool):
    """Create survey data with float PIDs."""
    data = []
    base_pid = 10000001

    for i in range(5):
        pid = float(base_pid + i)
        data.append(
            {
                "hhidpn": pid,
                "bcdate": f"2017-{(i % 12) + 1:02d}-15",
                "GEOID2010": sample_geoid_pool[i],
            }
        )

    df = pd.DataFrame(data)
    df["bcdate"] = pd.to_datetime(df["bcdate"])
    file_path = tmp_path / "survey_float_pids.csv"
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def survey_data_with_string_pids(tmp_path, sample_geoid_pool):
    """Create survey data with string PIDs."""
    data = []
    base_pid = 10000001

    for i in range(5):
        pid = str(base_pid + i)
        data.append(
            {
                "hhidpn": pid,
                "bcdate": f"2017-{(i % 12) + 1:02d}-15",
                "GEOID2010": sample_geoid_pool[i],
            }
        )

    df = pd.DataFrame(data)
    df["bcdate"] = pd.to_datetime(df["bcdate"])
    file_path = tmp_path / "survey_string_pids.csv"
    df.to_csv(file_path, index=False)
    return file_path


# Test 1: Loading residential history with float PIDs
def test_load_residential_history_with_float_pids(residential_data_with_float_pids):
    """Test that residential history correctly loads and normalizes float PIDs."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    # Check that hhidpn column is Int64 dtype
    assert (
        res_hist.df["hhidpn"].dtype == "Int64"
    ), f"Expected Int64, got {res_hist.df['hhidpn'].dtype}"

    # Check that values are properly converted
    assert res_hist.df["hhidpn"].iloc[0] == 10000001

    # Check no NaN values were introduced
    assert res_hist.df["hhidpn"].notna().all()


# Test 2: Loading residential history with string PIDs
def test_load_residential_history_with_string_pids(residential_data_with_string_pids):
    """Test that residential history correctly loads and normalizes string PIDs."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_string_pids
    )

    # Check that hhidpn column is Int64 dtype
    assert res_hist.df["hhidpn"].dtype == "Int64"

    # Check that values are properly converted
    assert res_hist.df["hhidpn"].iloc[0] == 10000001

    # Check no NaN values were introduced
    assert res_hist.df["hhidpn"].notna().all()


# Test 3: Loading residential history with int PIDs
def test_load_residential_history_with_int_pids(residential_data_with_int_pids):
    """Test that residential history correctly loads and normalizes int PIDs."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_int_pids
    )

    # Check that hhidpn column is Int64 dtype
    assert res_hist.df["hhidpn"].dtype == "Int64"

    # Check that values are properly converted
    assert res_hist.df["hhidpn"].iloc[0] == 10000001

    # Check no NaN values were introduced
    assert res_hist.df["hhidpn"].notna().all()


# Test 4: _move_info has integer keys
def test_move_info_has_integer_keys(residential_data_with_float_pids):
    """Test that _move_info dict uses integer keys regardless of input type."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    # Check that all keys are Python ints
    for key in res_hist._move_info.keys():
        assert isinstance(key, int), f"Expected int key, got {type(key)}: {key}"

    # Check expected keys are present
    assert 10000001 in res_hist._move_info
    assert 10000002 in res_hist._move_info
    assert 10000005 in res_hist._move_info

    # Use debug method to inspect
    debug_info = res_hist.debug_move_info()
    assert debug_info["key_count"] == 5
    assert "int" in debug_info["key_types"]
    assert len(debug_info["key_types"]) == 1  # Only int type


# Test 5: create_geoid_based_on_date with various input types
def test_create_geoid_with_float_input(residential_data_with_float_pids):
    """Test create_geoid_based_on_date with float PIDs in input series."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    # Create input series with float PIDs
    pids = pd.Series([10000001.0, 10000002.0, 10000003.0])
    dates = pd.Series(
        [
            pd.Timestamp("2016-01-01"),
            pd.Timestamp("2016-06-15"),
            pd.Timestamp("2017-01-01"),
        ]
    )

    result = res_hist.create_geoid_based_on_date(pids, dates, debug=True)

    # All should be found and return non-None GEOIDs
    assert result.notna().all(), f"Expected all non-None, got: {result.tolist()}"
    assert len(result) == 3


def test_create_geoid_with_string_input(residential_data_with_string_pids):
    """Test create_geoid_based_on_date with string PIDs in input series."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_string_pids
    )

    # Create input series with string PIDs
    pids = pd.Series(["10000001", "10000002", "10000003"])
    dates = pd.Series(
        [
            pd.Timestamp("2016-01-01"),
            pd.Timestamp("2016-06-15"),
            pd.Timestamp("2017-01-01"),
        ]
    )

    result = res_hist.create_geoid_based_on_date(pids, dates, debug=True)

    # All should be found and return non-None GEOIDs
    assert result.notna().all(), f"Expected all non-None, got: {result.tolist()}"
    assert len(result) == 3


def test_create_geoid_with_int_input(residential_data_with_int_pids):
    """Test create_geoid_based_on_date with int PIDs in input series."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_int_pids
    )

    # Create input series with int PIDs
    pids = pd.Series([10000001, 10000002, 10000003])
    dates = pd.Series(
        [
            pd.Timestamp("2016-01-01"),
            pd.Timestamp("2016-06-15"),
            pd.Timestamp("2017-01-01"),
        ]
    )

    result = res_hist.create_geoid_based_on_date(pids, dates, debug=True)

    # All should be found and return non-None GEOIDs
    assert result.notna().all(), f"Expected all non-None, got: {result.tolist()}"
    assert len(result) == 3


def test_create_geoid_with_mixed_input(residential_data_with_float_pids):
    """Test create_geoid_based_on_date with mixed-type PIDs in input (as strings from CSV)."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    # Simulate PIDs as they might come from a CSV (mixed numeric types)
    pids = pd.Series([10000001, "10000002", 10000003.0])
    dates = pd.Series(
        [
            pd.Timestamp("2016-01-01"),
            pd.Timestamp("2016-06-15"),
            pd.Timestamp("2017-01-01"),
        ]
    )

    result = res_hist.create_geoid_based_on_date(pids, dates, debug=True)

    # All should be found and return non-None GEOIDs
    assert result.notna().all(), f"Expected all non-None, got: {result.tolist()}"
    assert len(result) == 3


# Test 6: End-to-end GEOID lookup returns non-None values
def test_end_to_end_float_to_float(
    residential_data_with_float_pids, survey_data_with_float_pids
):
    """Test end-to-end: float PIDs in both residential and survey data."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    survey = HRSInterviewData(
        survey_data_with_float_pids,
        datecol="bcdate",
        move=True,
        residential_hist=res_hist,
    )

    # Get GEOIDs based on survey dates
    result = survey.get_geoid_based_on_date(survey.df["bcdate"])

    # All PIDs should be found and return non-None GEOIDs
    non_null_count = result.notna().sum()
    assert non_null_count == len(
        result
    ), f"Expected all {len(result)} GEOIDs to be non-None, got {non_null_count}"


def test_end_to_end_string_to_string(
    residential_data_with_string_pids, survey_data_with_string_pids
):
    """Test end-to-end: string PIDs in both residential and survey data."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_string_pids
    )

    survey = HRSInterviewData(
        survey_data_with_string_pids,
        datecol="bcdate",
        move=True,
        residential_hist=res_hist,
    )

    # Get GEOIDs based on survey dates
    result = survey.get_geoid_based_on_date(survey.df["bcdate"])

    # All PIDs should be found and return non-None GEOIDs
    non_null_count = result.notna().sum()
    assert non_null_count == len(
        result
    ), f"Expected all {len(result)} GEOIDs to be non-None, got {non_null_count}"


def test_end_to_end_float_to_string(
    residential_data_with_float_pids, survey_data_with_string_pids
):
    """Test end-to-end: float PIDs in residential, string PIDs in survey (mixed types)."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    survey = HRSInterviewData(
        survey_data_with_string_pids,
        datecol="bcdate",
        move=True,
        residential_hist=res_hist,
    )

    # Get GEOIDs based on survey dates
    result = survey.get_geoid_based_on_date(survey.df["bcdate"])

    # All PIDs should be found and return non-None GEOIDs
    non_null_count = result.notna().sum()
    assert non_null_count == len(
        result
    ), f"Expected all {len(result)} GEOIDs to be non-None, got {non_null_count}"


def test_end_to_end_string_to_float(
    residential_data_with_string_pids, survey_data_with_float_pids
):
    """Test end-to-end: string PIDs in residential, float PIDs in survey (mixed types)."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_string_pids
    )

    survey = HRSInterviewData(
        survey_data_with_float_pids,
        datecol="bcdate",
        move=True,
        residential_hist=res_hist,
    )

    # Get GEOIDs based on survey dates
    result = survey.get_geoid_based_on_date(survey.df["bcdate"])

    # All PIDs should be found and return non-None GEOIDs
    non_null_count = result.notna().sum()
    assert non_null_count == len(
        result
    ), f"Expected all {len(result)} GEOIDs to be non-None, got {non_null_count}"


# Test 7: Edge cases - NA PIDs and mismatched PIDs
def test_edge_case_na_pids(residential_data_with_float_pids):
    """Test that NA PIDs in input series return None GEOIDs."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    # Create input with some NA PIDs
    pids = pd.Series([10000001, None, 10000003, pd.NA])
    dates = pd.Series(
        [
            pd.Timestamp("2016-01-01"),
            pd.Timestamp("2016-06-15"),
            pd.Timestamp("2017-01-01"),
            pd.Timestamp("2018-01-01"),
        ]
    )

    result = res_hist.create_geoid_based_on_date(pids, dates)

    # First and third should be found, second and fourth should be None
    assert result.iloc[0] is not None
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] is not None
    assert pd.isna(result.iloc[3])


def test_edge_case_mismatched_pids(residential_data_with_float_pids):
    """Test that PIDs not in residential history return None GEOIDs."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    # PIDs 9999998 and 9999999 don't exist in residential history
    pids = pd.Series([10000001, 9999998, 10000003, 9999999])
    dates = pd.Series(
        [
            pd.Timestamp("2016-01-01"),
            pd.Timestamp("2016-06-15"),
            pd.Timestamp("2017-01-01"),
            pd.Timestamp("2018-01-01"),
        ]
    )

    result = res_hist.create_geoid_based_on_date(pids, dates, debug=True)

    # First and third should be found, second and fourth should be None
    assert result.iloc[0] is not None
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] is not None
    assert pd.isna(result.iloc[3])


# Test 8: Verify GEOID values are correct
def test_geoid_values_correctness(residential_data_with_float_pids, sample_geoid_pool):
    """Test that returned GEOIDs are the correct ones based on dates."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    # PID 10000001: survey entry 2010-01-15 with geoid[0], move 2015-06-10 (noon) with geoid[1]
    pids = pd.Series([10000001, 10000001, 10000001])
    dates = pd.Series(
        [
            pd.Timestamp("2012-01-01"),  # Before move -> should get geoid[0]
            pd.Timestamp("2015-06-15"),  # After move -> should get geoid[1]
            pd.Timestamp("2018-01-01"),  # Well after move -> should get geoid[1]
        ]
    )

    result = res_hist.create_geoid_based_on_date(pids, dates)

    # All should be non-None
    assert result.notna().all()

    # Check specific values
    assert (
        result.iloc[0] == sample_geoid_pool[0]
    ), f"Expected {sample_geoid_pool[0]}, got {result.iloc[0]}"
    assert (
        result.iloc[1] == sample_geoid_pool[1]
    ), f"Expected {sample_geoid_pool[1]}, got {result.iloc[1]}"
    assert (
        result.iloc[2] == sample_geoid_pool[1]
    ), f"Expected {sample_geoid_pool[1]}, got {result.iloc[2]}"


# Test 9: Test debug method
def test_debug_move_info(residential_data_with_float_pids):
    """Test the debug_move_info method provides useful information."""
    res_hist = ResidentialHistoryHRS(
        residential_data_with_float_pids
    )

    debug_info = res_hist.debug_move_info(n_samples=3)

    # Check structure
    assert "key_count" in debug_info
    assert "key_types" in debug_info
    assert "sample_keys" in debug_info
    assert "sample_entries" in debug_info

    # Check values
    assert debug_info["key_count"] == 5
    assert "int" in debug_info["key_types"]
    assert len(debug_info["sample_keys"]) == 3
    assert len(debug_info["sample_entries"]) == 3

    # Check entry structure
    first_key = debug_info["sample_keys"][0]
    first_entry = debug_info["sample_entries"][first_key]
    assert "num_dates" in first_entry
    assert "first_date" in first_entry
    assert "first_geoid" in first_entry
    assert first_entry["num_dates"] == 2  # First tract + one move
