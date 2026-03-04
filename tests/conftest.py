"""
Shared pytest fixtures for HRS testing.

This module provides common fixtures that can be used across multiple test modules.
All fixtures are session-scoped for efficiency.
"""

import os
import sys

# Set Qt platform to offscreen in headless environments BEFORE importing PyQt
if sys.platform == "linux" and "DISPLAY" not in os.environ:
    os.environ["QT_QPA_PLATFORM"] = "offscreen"

import pytest
from pathlib import Path
from stitch.hrs import ResidentialHistoryHRS, HRSInterviewData
from .data_generators import (
    create_residential_history_data,
    create_survey_data,
    get_real_geoids_sample,
)
import pandas as pd
import numpy as np


@pytest.fixture(scope="session")
def test_data_dir():
    """Get the test data directory path."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def heat_index_dir(test_data_dir):
    """Return path to real heat index data directory."""
    return test_data_dir / "heat_index"


@pytest.fixture(scope="session")
def real_geoid_pool(heat_index_dir):
    """Extract real GEOIDs from heat data for test generation."""
    print("\n🔍 Extracting real GEOIDs from heat data...")
    geoid_pool = get_real_geoids_sample(heat_index_dir, sample_size=500)
    print(f"  Loaded {len(geoid_pool)} real GEOIDs for testing")
    return geoid_pool


@pytest.fixture(scope="session")
def fake_residential_history_file(tmp_path_factory, real_geoid_pool):
    """
    Create a fake residential history Stata file using real GEOIDs.

    Returns
    -------
    Path
        Path to the generated .dta file
    """
    import pandas as pd

    # Get data from standalone generator
    data_rows = create_residential_history_data(n_people=55, geoid_pool=real_geoid_pool)

    # Convert to DataFrame
    df = pd.DataFrame(data_rows)

    # Convert appropriate columns to proper types
    df["mvyear"] = pd.to_numeric(df["mvyear"], errors="coerce")
    df["mvmonth"] = pd.to_numeric(df["mvmonth"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["trmove_tr"] = df["trmove_tr"].astype(str)

    # Save to temporary file
    tmp_path = tmp_path_factory.mktemp("data")
    file_path = tmp_path / "fake_residential_history.dta"
    df.to_stata(file_path, write_index=False)

    return file_path


@pytest.fixture(scope="session")
def residential_history_hrs(fake_residential_history_file):
    """
    Create a ResidentialHistoryHRS instance from fake data.

    Returns
    -------
    ResidentialHistoryHRS
        Initialized instance with fake data
    """
    return ResidentialHistoryHRS(
        fake_residential_history_file, first_tract_mark="999.0"
    )


@pytest.fixture(scope="session")
def fake_survey_file(tmp_path_factory):
    """
    Create a fake survey/interview Stata file.

    Returns
    -------
    Path
        Path to the generated .dta file
    """
    import pandas as pd

    # Get data from standalone generator
    data_rows = create_survey_data(n_people=55)

    # Convert to DataFrame
    df = pd.DataFrame(data_rows)

    # Convert bcdate to datetime
    df["bcdate"] = pd.to_datetime(df["bcdate"])

    # Save to temporary file
    tmp_path = tmp_path_factory.mktemp("data")
    file_path = tmp_path / "fake_survey_data.dta"
    df.to_stata(file_path, write_index=False)

    return file_path


@pytest.fixture(scope="session")
def survey_data_hrs(fake_survey_file):
    """
    Create an HRSInterviewData instance from fake data.

    Returns
    -------
    HRSInterviewData
        Initialized instance with fake data
    """
    return HRSInterviewData(fake_survey_file)


@pytest.fixture(scope="session")
def survey_with_residential_history(survey_data_hrs, residential_history_hrs):
    """
    Create HRSInterviewData with linked residential history.

    Returns
    -------
    HRSInterviewData
        Instance with residential history linked
    """
    survey_data_hrs.residential_hist = residential_history_hrs
    return survey_data_hrs


@pytest.fixture
def survey_data_2016_2020(tmp_path, real_geoid_pool):
    """Create survey data with interview dates only in 2016-2020 using real GEOIDs."""
    from .data_generators import generate_fake_hhidpn, generate_fake_geoid

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

        # Create static GEOID columns using real GEOIDs
        geoid_2010 = generate_fake_geoid(real_geoid_pool)
        geoid_2015 = generate_fake_geoid(real_geoid_pool)
        geoid_2020 = generate_fake_geoid(real_geoid_pool)

        rows.append(
            {
                "hhidpn": hhidpn,
                "iwdate": iwdate,  # Use iwdate to match step1 script
                "GEOID2010_2010": geoid_2010,
                "GEOID2010_2015": geoid_2015,
                "GEOID2010_2020": geoid_2020,
                "age": np.random.randint(50, 90),
                "gender": np.random.choice(["Male", "Female"]),
            }
        )

    df = pd.DataFrame(rows)

    # Save to temporary Stata file
    file_path = tmp_path / "survey_2016_2020.dta"
    df.to_stata(file_path, write_index=False)

    return file_path
