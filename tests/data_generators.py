"""
Utility functions for generating fake HRS data.

This module provides standalone functions for generating fake data without
importing pandas, to avoid NumPy compatibility issues.
"""

import numpy as np
from typing import List
from pathlib import Path


def generate_fake_hhidpn(n_people: int) -> List[int]:
    """Generate fake HRS person IDs in realistic format."""
    # HRS IDs are typically 8-digit numbers
    return list(range(10000001, 10000001 + n_people))


def get_real_geoids_sample(heat_index_dir: Path, sample_size: int = 500) -> List[str]:
    """
    Extract a sample of real GEOIDs from heat index CSV files.
    Reads first file to get actual GEOIDs used in the dataset.

    Parameters
    ----------
    heat_index_dir : Path
        Directory containing heat index CSV files
    sample_size : int
        Number of GEOIDs to sample (default 500)

    Returns
    -------
    List[str]
        List of real GEOID strings
    """
    import pandas as pd

    # Read first available CSV file
    csv_files = sorted(heat_index_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {heat_index_dir}")

    print(f"  Extracting real GEOIDs from {csv_files[0].name}...")
    # Read small sample to get GEOIDs
    df_sample = pd.read_csv(csv_files[0], nrows=100000)

    if "GEOID10" in df_sample.columns:
        geoid_col = "GEOID10"
    elif "GEOID" in df_sample.columns:
        geoid_col = "GEOID"
    else:
        raise ValueError(f"No GEOID column found in {csv_files[0]}")

    # Get unique GEOIDs and sample
    unique_geoids = df_sample[geoid_col].astype(str).str.zfill(11).unique()
    sampled = np.random.choice(
        unique_geoids, size=min(sample_size, len(unique_geoids)), replace=False
    )

    print(f"  Sampled {len(sampled)} GEOIDs from {len(unique_geoids)} unique values")
    return sampled.tolist()


def generate_fake_geoid(geoid_pool: List[str] = None) -> str:
    """
    Generate a realistic 11-digit Census tract GEOID.

    If geoid_pool is provided, randomly selects from it.
    Otherwise generates a random GEOID.
    """
    if geoid_pool is not None and len(geoid_pool) > 0:
        return np.random.choice(geoid_pool)

    # Format: SSCCCTTTTTT (State-County-Census Tract)
    state = np.random.randint(1, 57)  # 50 states + DC + territories
    county = np.random.randint(1, 1000)
    tract = np.random.randint(1, 10000)

    return f"{state:02d}{county:03d}{tract:06d}"


def create_residential_history_data(
    n_people: int = 55, geoid_pool: List[str] = None
) -> List[dict]:
    """
    Create fake residential history data with varied move patterns, in the
    simplified long format: one row per residence with ``hhidpn``,
    ``move_date``, and ``GEOID`` columns. The earliest entry per person is
    their residence at survey entry (2010). Move dates mix representations
    (full date, year-month, year-only) to exercise date format inference.

    Parameters
    ----------
    n_people : int
        Number of people to generate (default 55)
    geoid_pool : List[str], optional
        Pool of real GEOIDs to sample from. If None, generates random GEOIDs.

    Returns
    -------
    List[dict]
        List of dictionaries representing residential history rows
    """
    hhidpns = generate_fake_hhidpn(n_people)
    rows = []

    for i, hhidpn in enumerate(hhidpns):
        # Determine move pattern for this person
        if i < 20:  # ~20 people with no moves
            n_moves = 0
        elif i < 40:  # ~20 people with 1 move
            n_moves = 1
        else:  # ~15 people with 2-4 moves
            n_moves = np.random.randint(2, 5)

        # Survey entry (2010) — full date
        entry_month = np.random.randint(1, 13)
        entry_day = np.random.randint(1, 29)
        rows.append(
            {
                "hhidpn": hhidpn,
                "move_date": f"2010-{entry_month:02d}-{entry_day:02d}",
                "GEOID": generate_fake_geoid(geoid_pool),
            }
        )

        # Add moves if any (move years strictly increase, so mixing
        # year-only / year-month / full-date formats keeps chronology)
        current_year = 2010
        for move_num in range(n_moves):
            # Move year: 2011-2019, ensuring chronological order
            move_year = np.random.randint(current_year + 1, min(current_year + 3, 2020))
            move_month = np.random.randint(1, 13)
            move_day = np.random.randint(1, 29)

            if move_num % 3 == 0:
                move_date = f"{move_year}-{move_month:02d}"  # year-month
            elif move_num % 3 == 1:
                move_date = f"{move_year}-{move_month:02d}-{move_day:02d}"
            else:
                move_date = str(move_year)  # year only

            rows.append(
                {
                    "hhidpn": hhidpn,
                    "move_date": move_date,
                    "GEOID": generate_fake_geoid(geoid_pool),
                }
            )

            current_year = move_year

    return rows


def create_survey_data(n_people: int = 55, geoid_pool: List[str] = None) -> List[dict]:
    """
    Create fake survey/interview data matching the residential history IDs.

    Parameters
    ----------
    n_people : int
        Number of people to generate (default 55)
    geoid_pool : List[str], optional
        Pool of real GEOIDs to sample from. If None, generates random GEOIDs.

    Returns
    -------
    List[dict]
        List of dictionaries representing survey data rows
    """
    hhidpns = generate_fake_hhidpn(n_people)
    rows = []

    for hhidpn in hhidpns:
        # Generate interview date between 2015-2020
        interview_year = np.random.randint(2015, 2021)
        interview_month = np.random.randint(1, 13)
        interview_day = np.random.randint(1, 29)  # Avoid day 30/31 issues

        bcdate = f"{interview_year}-{interview_month:02d}-{interview_day:02d}"

        # Create static GEOID columns for different years (2010, 2015, 2020)
        geoid_2010 = generate_fake_geoid(geoid_pool)
        geoid_2015 = generate_fake_geoid(geoid_pool)
        geoid_2020 = generate_fake_geoid(geoid_pool)

        rows.append(
            {
                "hhidpn": hhidpn,
                "bcdate": bcdate,
                "GEOID2010_2010": geoid_2010,
                "GEOID2010_2015": geoid_2015,
                "GEOID2010_2020": geoid_2020,
                # Add some additional survey variables
                "age": np.random.randint(50, 90),
                "gender": np.random.choice(["Male", "Female"]),
                "education": np.random.choice(
                    ["Less than HS", "HS", "Some College", "College+"]
                ),
            }
        )

    return rows


def write_csv_file(data: List[dict], filename: str):
    """Write data to CSV file."""
    if not data:
        return

    # Get column names from first row
    columns = list(data[0].keys())

    with open(filename, "w") as f:
        # Write header
        f.write(",".join(columns) + "\n")

        # Write data rows
        for row in data:
            values = []
            for col in columns:
                value = row[col]
                # Check for NaN (using numpy's isnan for float values)
                if isinstance(value, float) and np.isnan(value):
                    values.append("")
                elif isinstance(value, str) and "," in value:
                    values.append(f'"{value}"')  # Quote strings with commas
                else:
                    values.append(str(value))
            f.write(",".join(values) + "\n")


def main():
    """Generate test data files."""
    from pathlib import Path

    # Create test data directory
    test_data_dir = Path(__file__).parent / "test_data"
    test_data_dir.mkdir(parents=True, exist_ok=True)

    print("Generating residential history data...")
    residential_data = create_residential_history_data()
    residential_file = test_data_dir / "fake_residential_history.csv"
    write_csv_file(residential_data, residential_file)
    print(f"Created {residential_file} with {len(residential_data)} rows")

    print("Generating survey data...")
    survey_data = create_survey_data()
    survey_file = test_data_dir / "fake_survey_data.csv"
    write_csv_file(survey_data, survey_file)
    print(f"Created {survey_file} with {len(survey_data)} rows")

    print("\nData summary:")
    print(f"Residential data rows: {len(residential_data)}")
    print(f"Survey data rows: {len(survey_data)}")

    # Count unique people
    residential_people = set(row["hhidpn"] for row in residential_data)
    survey_people = set(row["hhidpn"] for row in survey_data)
    print(f"Unique people in residential: {len(residential_people)}")
    print(f"Unique people in survey: {len(survey_people)}")


if __name__ == "__main__":
    main()
