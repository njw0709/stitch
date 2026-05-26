"""
Integration tests for HRS classes.

This module tests the integration between ResidentialHistoryHRS and HRSInterviewData
classes, including the HRSContextLinker functionality.
"""

import pandas as pd
import pytest
from pathlib import Path

from stitch.hrs import ResidentialHistoryHRS, HRSInterviewData, HRSContextLinker
from stitch.daily_measure import DailyMeasureDataDir


def test_residential_history_integration(residential_history_hrs):
    """Test comprehensive residential history functionality."""
    # Test that we have the expected number of people
    unique_people = residential_history_hrs.df["hhidpn"].nunique()
    assert unique_people >= 50

    # Test move info structure
    move_info = residential_history_hrs._move_info
    assert len(move_info) == unique_people

    # Test that we have people with different move patterns
    move_counts = [len(dates) for dates, _ in move_info.values()]
    assert max(move_counts) > 1  # At least one person with moves
    assert min(move_counts) == 1  # At least one person with no moves


def test_survey_data_integration(survey_data_hrs):
    """Test comprehensive survey data functionality."""
    # Test basic structure
    assert len(survey_data_hrs.df) >= 50
    assert "hhidpn" in survey_data_hrs.df.columns
    assert "bcdate" in survey_data_hrs.df.columns

    # Test date range
    dates = survey_data_hrs.df["bcdate"]
    assert dates.min() >= pd.Timestamp("2015-01-01")
    assert dates.max() <= pd.Timestamp("2020-12-31")

    # Test GEOID formatting
    geoid_cols = [c for c in survey_data_hrs.df.columns if "GEOID" in c]
    for col in geoid_cols:
        geoid_lengths = survey_data_hrs.df[col].astype(str).str.len()
        assert geoid_lengths.eq(11).all()


def test_combined_data_integration(survey_with_residential_history):
    """Test integration between survey data and residential history."""
    # Test that we can get GEOIDs based on dates
    n_people = len(survey_with_residential_history.df)
    test_dates = pd.Series([pd.Timestamp("2015-06-15")] * n_people)

    result = survey_with_residential_history.get_geoid_based_on_date(test_dates)

    assert len(result) == len(test_dates)
    assert result.dtype == "string"

    # Test that some results are not None (people with valid moves)
    non_null_count = result.notna().sum()
    assert non_null_count > 0


def test_hrs_context_linker_n_day_prior(survey_data_hrs):
    """Test HRSContextLinker n-day prior functionality."""
    # Test creating n-day prior columns
    colname = HRSContextLinker.make_n_day_prior_cols(survey_data_hrs, 7)

    assert colname == "bcdate_7day_prior"
    assert colname in survey_data_hrs.df.columns

    # Test that dates are correctly calculated
    original_dates = survey_data_hrs.df["bcdate"]
    prior_dates = survey_data_hrs.df[colname]

    # Check that prior dates are 7 days earlier
    date_diff = (original_dates - prior_dates).dt.days
    assert date_diff.eq(7).all()


def test_hrs_context_linker_geoid_assignment(survey_with_residential_history):
    """Test HRSContextLinker GEOID assignment functionality."""
    # First create a n-day prior column
    colname = HRSContextLinker.make_n_day_prior_cols(
        survey_with_residential_history, 14
    )

    # Test GEOID assignment for the lagged date
    geoid_colname = HRSContextLinker.make_geoid_day_prior(
        survey_with_residential_history, colname
    )

    assert geoid_colname == "GEOID2010_14day_prior"
    assert geoid_colname in survey_with_residential_history.df.columns

    # Test that GEOIDs are properly formatted
    geoid_lengths = (
        survey_with_residential_history.df[geoid_colname].astype(str).str.len()
    )
    # Some might be None, so check only non-null values
    non_null_lengths = geoid_lengths[geoid_lengths.notna()]
    if len(non_null_lengths) > 0:
        assert non_null_lengths.eq(11).all()


def test_lag_column_naming_with_underscore_datecol(tmp_path):
    """Lag suffix recovery must work when datecol itself contains underscores."""
    df = pd.DataFrame(
        {
            "hhidpn": [1, 2],
            "interview_date": pd.to_datetime(["2020-01-15", "2020-02-15"]),
            "GEOID2010": ["01001000100", "01001000200"],
        }
    )
    path = tmp_path / "survey.dta"
    df.to_stata(path, write_index=False)

    hrs = HRSInterviewData(path, datecol="interview_date", move=False)

    date_col = HRSContextLinker.make_n_day_prior_cols(hrs, 30)
    assert date_col == "interview_date_30day_prior"

    geoid_col = HRSContextLinker.make_geoid_day_prior(hrs, date_col)
    assert geoid_col == "GEOID2010_30day_prior"

    batch_df = HRSContextLinker.prepare_lag_columns_batch(hrs, [7, 30])
    assert "interview_date_7day_prior" in batch_df.columns
    assert "GEOID2010_7day_prior" in batch_df.columns
    assert "interview_date_30day_prior" in batch_df.columns
    assert "GEOID2010_30day_prior" in batch_df.columns


def test_data_consistency(residential_history_hrs, survey_data_hrs):
    """Test that residential history and survey data are consistent."""
    # Get person IDs from both datasets
    residential_ids = set(residential_history_hrs.df["hhidpn"].unique())
    survey_ids = set(survey_data_hrs.df["hhidpn"].unique())

    # They should have the same person IDs
    assert residential_ids == survey_ids

    # Test that dates are reasonable relative to move history
    for hhidpn in list(residential_ids)[:5]:  # Test first 5 people
        person_moves = residential_history_hrs.df[
            residential_history_hrs.df["hhidpn"] == hhidpn
        ]
        person_survey = survey_data_hrs.df[survey_data_hrs.df["hhidpn"] == hhidpn]

        if len(person_survey) > 0:
            interview_date = person_survey["bcdate"].iloc[0]

            # Interview date should be after first tract (2010)
            assert interview_date >= pd.Timestamp("2010-01-01")

            # Interview date should be reasonable (not too far in future)
            assert interview_date <= pd.Timestamp("2025-01-01")


def test_edge_cases(residential_history_hrs):
    """Test edge cases in residential history parsing."""
    move_info = residential_history_hrs._move_info

    # Test person with no moves
    no_move_person = None
    for hhidpn, (dates, geoids) in move_info.items():
        if len(dates) == 1:  # Only first tract
            no_move_person = hhidpn
            break

    if no_move_person:
        dates, geoids = move_info[no_move_person]

        # Test lookup for date before first tract
        early_date = dates[0] - pd.Timedelta(days=365)
        result = residential_history_hrs._find_geoid_for_date(early_date, dates, geoids)
        assert result is None

        # Test lookup for date after first tract
        late_date = dates[0] + pd.Timedelta(days=365)
        result = residential_history_hrs._find_geoid_for_date(late_date, dates, geoids)
        assert result == geoids[0]
