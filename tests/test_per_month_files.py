"""
Tests for per-month contextual file support in ``DailyMeasureDataDir``.

Contextual data may be saved either per year (filename contains a 4-digit year,
e.g. ``2010_daily_heat_index.csv``) or per month (filename contains
``YYYY_MM``, e.g. ``2010_10_heat_index.csv``). These tests cover discovery,
parsing, concatenation across months, uniqueness enforcement, and backward
compatibility with the per-year layout.
"""

import pandas as pd
import pytest

from stitch.daily_measure import DailyMeasureDataDir


GEOIDS = ["01001020100", "01001020200", "01001020300"]


def _write_month_file(dir_path, year, month, measure="heat_index", value=1.0):
    """Write a small per-month long-format file for a single month."""
    days = pd.date_range(f"{year}-{month:02d}-01", periods=3, freq="D")
    df = pd.DataFrame(
        {
            "Date": [d.strftime("%Y-%m-%d") for d in days for _ in GEOIDS],
            "GEOID10": GEOIDS * len(days),
            "HeatIndex": value,
        }
    )
    path = dir_path / f"{year}_{month:02d}_{measure}.csv"
    df.to_csv(path, index=False)
    return path


def _write_year_file(dir_path, year, measure="heat_index", value=1.0):
    """Write a small per-year long-format file spanning a couple of months."""
    days = pd.date_range(f"{year}-01-01", periods=5, freq="D")
    df = pd.DataFrame(
        {
            "Date": [d.strftime("%Y-%m-%d") for d in days for _ in GEOIDS],
            "GEOID10": GEOIDS * len(days),
            "HeatIndex": value,
        }
    )
    path = dir_path / f"{year}_daily_{measure}.csv"
    df.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Period parsing
# ---------------------------------------------------------------------------


def test_parse_period_month():
    assert DailyMeasureDataDir._parse_period("2010_10_heat_index.csv") == ("2010", 10)
    assert DailyMeasureDataDir._parse_period("pm25_2016_03.parquet") == ("2016", 3)


def test_parse_period_year_only():
    assert DailyMeasureDataDir._parse_period("2010_daily_heat_index.csv") == (
        "2010",
        None,
    )
    # 8-digit YYYYMMDD is not a per-month token (no underscore) -> year only.
    assert DailyMeasureDataDir._parse_period("data_20100301.csv") == ("2010", None)


def test_parse_period_invalid_month_falls_back_to_year():
    # Month 13 is not valid; treat as per-year.
    assert DailyMeasureDataDir._parse_period("2010_13_heat_index.csv") == (
        "2010",
        None,
    )


def test_parse_period_no_year_raises():
    with pytest.raises(ValueError, match="Could not extract year"):
        DailyMeasureDataDir._parse_period("heat_index.csv")


# ---------------------------------------------------------------------------
# Discovery / grouping
# ---------------------------------------------------------------------------


def test_per_month_files_grouped_by_year(tmp_path):
    for month in (1, 2, 3):
        _write_month_file(tmp_path, 2010, month)
    _write_month_file(tmp_path, 2011, 1)

    ddir = DailyMeasureDataDir(
        dir_name=tmp_path, data_col="HeatIndex", measure_type=None
    )

    assert ddir.list_years() == ["2010", "2011"]
    assert len(ddir.year_to_files["2010"]) == 3
    assert len(ddir.year_to_files["2011"]) == 1


def test_backward_compat_per_year_file(tmp_path):
    _write_year_file(tmp_path, 2010)
    _write_year_file(tmp_path, 2011)

    ddir = DailyMeasureDataDir(
        dir_name=tmp_path, data_col="HeatIndex", measure_type=None
    )

    assert ddir.list_years() == ["2010", "2011"]
    assert len(ddir.year_to_files["2010"]) == 1


# ---------------------------------------------------------------------------
# Loading / concatenation
# ---------------------------------------------------------------------------


def test_multiple_months_concatenated_on_load(tmp_path):
    _write_month_file(tmp_path, 2010, 1, value=10.0)
    _write_month_file(tmp_path, 2010, 2, value=20.0)
    _write_month_file(tmp_path, 2010, 3, value=30.0)

    ddir = DailyMeasureDataDir(
        dir_name=tmp_path, data_col="HeatIndex", measure_type=None
    )
    data = ddir[2010]

    # 3 months x 3 days x 3 geoids = 27 rows
    assert len(data.df) == 27
    assert set(data.df["Date"].dt.month.unique()) == {1, 2, 3}
    # Values preserved per month.
    assert set(data.df["HeatIndex"].round().unique()) == {10.0, 20.0, 30.0}


def test_concat_preserves_date_geoid_uniqueness(tmp_path):
    _write_month_file(tmp_path, 2010, 1)
    _write_month_file(tmp_path, 2010, 2)

    ddir = DailyMeasureDataDir(
        dir_name=tmp_path, data_col="HeatIndex", measure_type=None
    )
    data = ddir[2010]
    dup = data.df.duplicated(subset=["Date", "GEOID10"]).any()
    assert not dup


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_duplicate_month_rejected(tmp_path):
    _write_month_file(tmp_path, 2010, 5, measure="heat_index")
    # Second file, same period, different measure substring so both are found.
    _write_month_file(tmp_path, 2010, 5, measure="heat_index_dup")

    with pytest.raises(ValueError, match="Duplicate period 2010_05"):
        DailyMeasureDataDir(dir_name=tmp_path, data_col="HeatIndex", measure_type=None)


def test_mixing_year_and_month_files_rejected(tmp_path):
    _write_year_file(tmp_path, 2010)
    _write_month_file(tmp_path, 2010, 6)

    with pytest.raises(ValueError, match="mixes a per-year file with per-month"):
        DailyMeasureDataDir(dir_name=tmp_path, data_col="HeatIndex", measure_type=None)


def test_duplicate_year_rejected(tmp_path):
    _write_year_file(tmp_path, 2010, measure="heat_index")
    _write_year_file(tmp_path, 2010, measure="heat_index_dup")

    with pytest.raises(ValueError, match="Duplicate year 2010"):
        DailyMeasureDataDir(dir_name=tmp_path, data_col="HeatIndex", measure_type=None)
