#!/usr/bin/env python3
"""
Convert CSV test data files to Stata format.

This script converts the generated CSV files to .dta format required by the HRS classes.
Run this after generating the CSV files with generate_test_data_csv.py
"""

import sys
from pathlib import Path

# Add the project root to the path so we can import pandas
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import pandas as pd

    print("Successfully imported pandas")
except ImportError as e:
    print(f"Error importing pandas: {e}")
    print("Please install pandas: pip install pandas")
    sys.exit(1)


def convert_csv_to_stata():
    """Convert CSV files to Stata format."""
    test_data_dir = Path(__file__).parent / "test_data"

    # Convert residential history
    csv_file = test_data_dir / "fake_residential_history.csv"
    dta_file = test_data_dir / "fake_residential_history.dta"

    if csv_file.exists():
        print(f"Converting {csv_file} to {dta_file}")
        df = pd.read_csv(csv_file)

        # move_date stays as a string — its format is inferred at load time
        df["move_date"] = df["move_date"].astype(str)

        df.to_stata(dta_file, write_index=False)
        print(f"Created {dta_file} with {len(df)} rows")
    else:
        print(f"CSV file not found: {csv_file}")

    # Convert survey data
    csv_file = test_data_dir / "fake_survey_data.csv"
    dta_file = test_data_dir / "fake_survey_data.dta"

    if csv_file.exists():
        print(f"Converting {csv_file} to {dta_file}")
        df = pd.read_csv(csv_file)

        # Convert bcdate to datetime
        df["bcdate"] = pd.to_datetime(df["bcdate"])

        df.to_stata(dta_file, write_index=False)
        print(f"Created {dta_file} with {len(df)} rows")
    else:
        print(f"CSV file not found: {csv_file}")


def verify_stata_files():
    """Verify that the Stata files can be read correctly."""
    test_data_dir = Path(__file__).parent / "test_data"

    # Test residential history
    dta_file = test_data_dir / "fake_residential_history.dta"
    if dta_file.exists():
        print(f"\nVerifying {dta_file}")
        df = pd.read_stata(dta_file)
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Unique people: {df['personid'].nunique()}")
        print("Sample data:")
        print(df.head())

    # Test survey data
    dta_file = test_data_dir / "fake_survey_data.dta"
    if dta_file.exists():
        print(f"\nVerifying {dta_file}")
        df = pd.read_stata(dta_file)
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Unique people: {df['hhidpn'].nunique()}")
        print("Sample data:")
        print(df.head())


if __name__ == "__main__":
    print("Converting CSV files to Stata format...")
    convert_csv_to_stata()

    print("\nVerifying Stata files...")
    verify_stata_files()

    print("\nConversion complete!")
