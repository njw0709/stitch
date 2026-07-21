"""
Tests for the ordered "Linkage resolution" feature (hourly / daily / monthly).

Covers:
- ``infer_temporal_resolution`` on hourly / daily / monthly samples.
- Resolution-aware lag date generation (hours / days / months) and canonical
  period-key flooring / column-suffix naming.
- Coarse interview-date anchoring (year / year-month) via ``infer_datetime_series``.
- Contextual aggregation (average / midpoint) when coarsening.
- An end-to-end monthly linkage over daily contextual data (batch path).
"""

import types

import pandas as pd
import pytest

from stitch.hrs import HRSInterviewData, HRSContextLinker
from stitch.daily_measure import (
    DailyMeasureDataDir,
    aggregate_contextual_to_resolution,
)
from stitch.temporal import (
    AggMethod,
    LinkageResolution,
    infer_temporal_resolution,
)
from stitch.process import (
    _prepare_contextual_resolution,
    process_multiple_lags_batch,
)


# ---------------------------------------------------------------------------
# Resolution inference
# ---------------------------------------------------------------------------


def test_infer_resolution_daily():
    s = pd.Series(pd.date_range("2020-01-01", periods=40, freq="D"))
    assert infer_temporal_resolution(s) is LinkageResolution.DAILY


def test_infer_resolution_monthly():
    s = pd.Series(pd.date_range("2020-01-01", periods=6, freq="MS"))
    assert infer_temporal_resolution(s) is LinkageResolution.MONTHLY


def test_infer_resolution_hourly():
    s = pd.Series(pd.date_range("2020-01-01", periods=48, freq="h"))
    assert infer_temporal_resolution(s) is LinkageResolution.HOURLY


def test_infer_resolution_empty_uses_filename_hint():
    empty = pd.Series([], dtype="datetime64[ns]")
    assert (
        infer_temporal_resolution(empty, ["2010_03_pm25.csv"])
        is LinkageResolution.MONTHLY
    )
    assert infer_temporal_resolution(empty) is LinkageResolution.DAILY


# ---------------------------------------------------------------------------
# Lag date generation math
# ---------------------------------------------------------------------------


def _make_survey(tmp_path, iwdate_values, geoids=None):
    n = len(iwdate_values)
    geoids = geoids or ["01001020100"] * n
    df = pd.DataFrame(
        {
            "hhidpn": list(range(1, n + 1)),
            "iwdate": iwdate_values,
            "GEOID2010": geoids,
        }
    )
    path = tmp_path / "survey.dta"
    df.to_stata(path, write_index=False)
    return path


def test_daily_lag_suffix_and_math(tmp_path):
    path = _make_survey(tmp_path, [pd.Timestamp("2020-03-15")])
    hrs = HRSInterviewData(
        path, datecol="iwdate", move=False, linkage_resolution="daily"
    )
    col = HRSContextLinker.make_n_day_prior_cols(hrs, 7)
    assert col == "iwdate_7day_prior"
    assert hrs.df[col].iloc[0] == pd.Timestamp("2020-03-08")


def test_monthly_lag_suffix_and_math(tmp_path):
    path = _make_survey(tmp_path, [pd.Timestamp("2020-03-15")])
    hrs = HRSInterviewData(
        path, datecol="iwdate", move=False, linkage_resolution="monthly"
    )
    col = HRSContextLinker.make_n_day_prior_cols(hrs, 2)
    assert col == "iwdate_2month_prior"
    # 2020-03-15 minus 2 months, floored to month start -> 2020-01-01.
    assert hrs.df[col].iloc[0] == pd.Timestamp("2020-01-01")


def test_hourly_lag_suffix_and_math(tmp_path):
    path = _make_survey(tmp_path, [pd.Timestamp("2020-03-15 10:30:00")])
    hrs = HRSInterviewData(
        path, datecol="iwdate", move=False, linkage_resolution="hourly"
    )
    col = HRSContextLinker.make_n_day_prior_cols(hrs, 5)
    assert col == "iwdate_5hour_prior"
    # 10:30 minus 5 hours = 05:30, floored to the hour -> 05:00.
    assert hrs.df[col].iloc[0] == pd.Timestamp("2020-03-15 05:00:00")


def test_batch_monthly_column_names(tmp_path):
    path = _make_survey(
        tmp_path,
        [pd.Timestamp("2020-03-15"), pd.Timestamp("2020-06-20")],
        geoids=["01001020100", "01001020200"],
    )
    hrs = HRSInterviewData(
        path, datecol="iwdate", move=False, linkage_resolution="monthly"
    )
    batch = HRSContextLinker.prepare_lag_columns_batch(hrs, [0, 1])
    assert "iwdate_0month_prior" in batch.columns
    assert "iwdate_1month_prior" in batch.columns
    assert "GEOID2010_1month_prior" in batch.columns


# ---------------------------------------------------------------------------
# Coarse interview-date anchoring
# ---------------------------------------------------------------------------


def test_interview_date_year_month_anchored_to_midmonth(tmp_path):
    # String year-month dates require a text-based source (CSV keeps them raw).
    df = pd.DataFrame(
        {"hhidpn": [1, 2], "iwdate": ["2013-03", "2013"], "GEOID2010": ["1", "2"]}
    )
    path = tmp_path / "survey.csv"
    df.to_csv(path, index=False)

    hrs = HRSInterviewData(path, datecol="iwdate", move=False)
    # 2013-03 -> mid-month; 2013 -> mid-year (matches residential-history rules).
    assert hrs.df["iwdate"].iloc[0] == pd.Timestamp("2013-03-16 12:00:00")
    assert hrs.df["iwdate"].iloc[1] == pd.Timestamp("2013-07-02 12:00:00")


def test_monthly_floor_of_anchored_interview_date(tmp_path):
    df = pd.DataFrame({"hhidpn": [1], "iwdate": ["2013-03"], "GEOID2010": ["1"]})
    path = tmp_path / "survey.csv"
    df.to_csv(path, index=False)
    hrs = HRSInterviewData(
        path, datecol="iwdate", move=False, linkage_resolution="monthly"
    )
    col = HRSContextLinker.make_n_day_prior_cols(hrs, 0)
    # Mid-month anchor floored to the month key.
    assert hrs.df[col].iloc[0] == pd.Timestamp("2013-03-01")


# ---------------------------------------------------------------------------
# Contextual aggregation
# ---------------------------------------------------------------------------


def _daily_frame(year_month_days, geoid="01001020100", start=0.0):
    ts = pd.date_range(f"{year_month_days}-01", periods=30, freq="D")
    return pd.DataFrame(
        {
            "Date": list(ts),
            "GEOID10": geoid,
            "v": [start + i for i in range(len(ts))],
        }
    )


def test_aggregate_daily_to_monthly_average():
    df = _daily_frame("2020-01")
    out = aggregate_contextual_to_resolution(
        df,
        date_col="Date",
        geoid_col="GEOID10",
        data_cols="v",
        resolution=LinkageResolution.MONTHLY,
        method=AggMethod.AVERAGE,
    )
    assert len(out) == 1
    assert out["Date"].iloc[0] == pd.Timestamp("2020-01-01")
    # mean of 0..29
    assert out["v"].iloc[0] == pytest.approx(14.5)


def test_aggregate_hourly_to_daily_median():
    # 23 small values plus one large outlier: median is robust, mean is not.
    ts = pd.date_range("2020-01-01 00:00", periods=24, freq="h")
    values = [float(i) for i in range(23)] + [1000.0]
    df = pd.DataFrame({"Date": list(ts), "GEOID10": "01001020100", "v": values})
    out = aggregate_contextual_to_resolution(
        df,
        date_col="Date",
        geoid_col="GEOID10",
        data_cols="v",
        resolution=LinkageResolution.DAILY,
        method=AggMethod.MEDIAN,
    )
    assert len(out) == 1
    assert out["Date"].iloc[0] == pd.Timestamp("2020-01-01")
    # median of [0..22, 1000] -> the 12th of 24 sorted values = 11.5,
    # distinct from the outlier-inflated mean.
    assert out["v"].iloc[0] == pytest.approx(11.5)
    assert out["v"].iloc[0] != pytest.approx(float(sum(values)) / len(values))


def test_prepare_contextual_rejects_finer_than_data():
    monthly = pd.DataFrame(
        {
            "Date": pd.date_range("2020-01-01", periods=3, freq="MS"),
            "GEOID10": "01001020100",
            "v": [1.0, 2.0, 3.0],
        }
    )
    fake_hrs = types.SimpleNamespace(
        linkage_resolution=LinkageResolution.DAILY, agg_method=AggMethod.AVERAGE
    )
    with pytest.raises(ValueError, match="finer than"):
        _prepare_contextual_resolution(monthly, "Date", "GEOID10", ["v"], fake_hrs)


# ---------------------------------------------------------------------------
# End-to-end monthly linkage over daily contextual data
# ---------------------------------------------------------------------------


def test_monthly_linkage_end_to_end(survey_data_2016_2020, heat_index_dir, tmp_path):
    hrs = HRSInterviewData(
        survey_data_2016_2020,
        datecol="iwdate",
        move=False,
        geoid_col="GEOID2010_2010",
        linkage_resolution="monthly",
    )
    hrs.agg_method = AggMethod.AVERAGE

    heat = DailyMeasureDataDir(heat_index_dir, data_col="index", measure_type=None)

    temp_dir = tmp_path / "monthly_lags"
    temp_dir.mkdir()

    temp_files = process_multiple_lags_batch(
        hrs_data=hrs,
        contextual_dir=heat,
        n_days=[0, 1, 2],
        id_col="hhidpn",
        temp_dir=temp_dir,
        prefix="heat",
        geoid_col="GEOID2010_2010",
    )
    assert len(temp_files) == 3

    final = hrs.df[["hhidpn"]].copy()
    for f in temp_files:
        final = final.merge(pd.read_parquet(f), on="hhidpn", how="left")

    for n in (0, 1, 2):
        col = f"index_iwdate_{n}month_prior"
        assert col in final.columns
    # At least the 0-month-prior linkage should produce some matches.
    assert final["index_iwdate_0month_prior"].notna().any()
