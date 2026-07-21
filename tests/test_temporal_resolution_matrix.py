"""
Enumerate temporal resolutions for every data source and verify behavior.

Three independent inputs carry a temporal resolution:

1. **Survey interview date** (``HRSInterviewData.datecol``): may be yearly,
   monthly, daily, or hourly (plus numeric ``YYYY`` / ``YYYYMM`` / ``YYYYMMDD``).
   Coarse values are anchored to the period midpoint.
2. **Contextual data** (``DailyMeasureDataDir``): inferred as hourly / daily /
   monthly. This is the *finest* linkage resolution allowed.
3. **Residential-history move date**: same inference/anchoring as the survey
   date; drives which GEOID a person occupied at a given (lagged) time.

Plus the **linkage resolution** the user chooses (hourly / daily / monthly),
which must not be finer than the contextual data. When coarser than the data,
the data is aggregated up (average / midpoint).

The tests below walk every resolution of each source and every valid/invalid
(contextual x linkage) combination.
"""

import numpy as np
import pandas as pd
import pytest

from stitch.hrs import HRSInterviewData, ResidentialHistoryHRS
from stitch.daily_measure import DailyMeasureDataDir
from stitch.temporal import AggMethod, LinkageResolution, infer_temporal_resolution
from stitch.process import process_multiple_lags_batch


G = "01001020100"  # single 11-digit GEOID shared across sources


# ===========================================================================
# 1. SURVEY INTERVIEW-DATE RESOLUTIONS
# ===========================================================================
#
# Each supported representation is parsed and anchored to the midpoint of the
# period it spans (finer components that are absent are filled at the midpoint).


def _survey_file(tmp_path, iwdate_value):
    path = tmp_path / "survey.csv"
    pd.DataFrame(
        {"personid": [1], "GEOID": [G], "iwdate": [iwdate_value]}
    ).to_csv(path, index=False)
    return path


def _load_survey(tmp_path, iwdate_value, resolution="daily"):
    return HRSInterviewData(
        str(_survey_file(tmp_path, iwdate_value)),
        datecol="iwdate",
        move=False,
        hhidpn="personid",
        geoid_col="GEOID",
        linkage_resolution=resolution,
    )


@pytest.mark.parametrize(
    "iwdate_value, expected",
    [
        # yearly (string + numeric) -> mid-year
        ("2015", pd.Timestamp("2015-07-02 12:00:00")),
        (2015, pd.Timestamp("2015-07-02 12:00:00")),
        # monthly (string, month-name, numeric YYYYMM) -> mid-month
        ("2015-03", pd.Timestamp("2015-03-16 12:00:00")),
        ("March 2015", pd.Timestamp("2015-03-16 12:00:00")),
        (201503, pd.Timestamp("2015-03-16 12:00:00")),
        # daily (ISO, stata-style, numeric YYYYMMDD) -> noon
        ("2015-03-10", pd.Timestamp("2015-03-10 12:00:00")),
        ("10mar2015", pd.Timestamp("2015-03-10 12:00:00")),
        (20150310, pd.Timestamp("2015-03-10 12:00:00")),
        # hourly (explicit time) -> preserved exactly
        ("2015-03-10 08:30:00", pd.Timestamp("2015-03-10 08:30:00")),
    ],
)
def test_survey_interview_date_resolution_anchoring(tmp_path, iwdate_value, expected):
    hrs = _load_survey(tmp_path, iwdate_value)
    assert hrs.df["iwdate"].iloc[0] == expected


@pytest.mark.parametrize(
    "iwdate_value, resolution, expected_key",
    [
        # Coarse survey date + monthly linkage -> floored to month start.
        ("2015", "monthly", pd.Timestamp("2015-07-01")),
        ("2015-03", "monthly", pd.Timestamp("2015-03-01")),
        # Daily survey date + daily linkage -> floored to midnight.
        ("2015-03-10", "daily", pd.Timestamp("2015-03-10")),
        # Hourly survey date + hourly linkage -> floored to the hour.
        ("2015-03-10 08:30:00", "hourly", pd.Timestamp("2015-03-10 08:00:00")),
    ],
)
def test_survey_date_floored_to_linkage_period_key(
    tmp_path, iwdate_value, resolution, expected_key
):
    from stitch.hrs import HRSContextLinker

    hrs = _load_survey(tmp_path, iwdate_value, resolution=resolution)
    col = HRSContextLinker.make_n_day_prior_cols(hrs, 0)
    assert hrs.df[col].iloc[0] == expected_key


# ===========================================================================
# 2. CONTEXTUAL DATA RESOLUTIONS
# ===========================================================================


def _ctx_hourly(tmp_path):
    """Two days of hourly data; value encodes day-offset*100 + hour."""
    d = tmp_path / "ctx_hourly"
    d.mkdir()
    rows = []
    for day_off, day in enumerate(["2016-06-14", "2016-06-15"]):
        for h in range(24):
            rows.append(
                {"Date": f"{day} {h:02d}:00:00", "GEOID10": G, "val": day_off * 100 + h}
            )
    pd.DataFrame(rows).to_csv(d / "2016_hourly_measure.csv", index=False)
    return d


def _ctx_daily(tmp_path):
    """Daily data for May–June 2016; value is the day-of-year."""
    d = tmp_path / "ctx_daily"
    d.mkdir()
    dates = pd.date_range("2016-05-01", "2016-06-30", freq="D")
    pd.DataFrame(
        {
            "Date": dates.strftime("%Y-%m-%d"),
            "GEOID10": G,
            "val": dates.dayofyear.astype(float),
        }
    ).to_csv(d / "2016_daily_measure.csv", index=False)
    return d


def _ctx_monthly(tmp_path):
    """Per-month (YYYY_MM) files for Apr/May/Jun 2016; value is the month."""
    d = tmp_path / "ctx_monthly"
    d.mkdir()
    for m in (4, 5, 6):
        pd.DataFrame(
            {"Date": [f"2016-{m:02d}-01"], "GEOID10": [G], "val": [float(m)]}
        ).to_csv(d / f"2016_{m:02d}_measure.csv", index=False)
    return d


CTX_BUILDERS = {
    LinkageResolution.HOURLY: _ctx_hourly,
    LinkageResolution.DAILY: _ctx_daily,
    LinkageResolution.MONTHLY: _ctx_monthly,
}


@pytest.mark.parametrize(
    "builder, expected",
    [
        (_ctx_hourly, LinkageResolution.HOURLY),
        (_ctx_daily, LinkageResolution.DAILY),
        (_ctx_monthly, LinkageResolution.MONTHLY),
    ],
)
def test_contextual_resolution_inference(tmp_path, builder, expected):
    ctx_dir = builder(tmp_path)
    cdir = DailyMeasureDataDir(ctx_dir, data_col="val", measure_type=None, geoid_col="GEOID10")
    combined = pd.concat([cdir[y].df for y in cdir.list_years()], axis=0)
    date_col = cdir[cdir.list_years()[0]].date_col
    assert infer_temporal_resolution(combined[date_col]) is expected


def test_contextual_monthly_filename_hint_when_sparse(tmp_path):
    """A single-day-per-month layout still infers monthly, aided by YYYY_MM names."""
    ctx_dir = _ctx_monthly(tmp_path)
    filenames = [p.name for p in ctx_dir.glob("*.csv")]
    empty = pd.Series([], dtype="datetime64[ns]")
    assert infer_temporal_resolution(empty, filenames) is LinkageResolution.MONTHLY


# ===========================================================================
# 3. RESIDENTIAL-HISTORY MOVE-DATE RESOLUTIONS
# ===========================================================================


def _reshist(tmp_path, rows):
    path = tmp_path / "reshist.csv"
    pd.DataFrame(rows, columns=["hhidpn", "move_date", "GEOID"]).to_csv(
        path, index=False
    )
    return ResidentialHistoryHRS(path)


@pytest.mark.parametrize(
    "move_date, expected_anchor",
    [
        ("2015", pd.Timestamp("2015-07-02 12:00:00")),  # yearly
        ("2015-03", pd.Timestamp("2015-03-16 12:00:00")),  # monthly
        ("2015-03-10", pd.Timestamp("2015-03-10 12:00:00")),  # daily
        ("2015-03-10 08:30:00", pd.Timestamp("2015-03-10 08:30:00")),  # hourly
    ],
)
def test_residential_history_move_date_anchoring(tmp_path, move_date, expected_anchor):
    rh = _reshist(tmp_path, [(1, move_date, G)])
    dates, geoids = rh._move_info[1]
    assert dates[0] == expected_anchor
    assert geoids[0] == G


def test_residential_history_mixed_resolution_ordering_and_lookup(tmp_path):
    """Move dates given at different resolutions still sort and look up correctly."""
    rh = _reshist(
        tmp_path,
        [
            (1, "2015", "11111111111"),          # yearly  -> 2015-07-02
            (1, "2018-06", "22222222222"),        # monthly -> 2018-06-16
            (1, "2019-03-15", "33333333333"),     # daily   -> 2019-03-15 noon
        ],
    )
    pids = pd.Series([1, 1, 1, 1])
    queries = pd.Series(
        [
            pd.Timestamp("2014-01-01"),  # before first move -> None
            pd.Timestamp("2016-01-01"),  # after 2015 anchor -> first geoid
            pd.Timestamp("2018-07-01"),  # after 2018-06 anchor -> second geoid
            pd.Timestamp("2019-04-01"),  # after 2019-03 anchor -> third geoid
        ]
    )
    result = rh.create_geoid_based_on_date(pids, queries)
    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == "11111111111"
    assert result.iloc[2] == "22222222222"
    assert result.iloc[3] == "33333333333"


# ===========================================================================
# 4. CROSS-RESOLUTION LINKAGE MATRIX (contextual x linkage)
# ===========================================================================
#
# Valid when linkage resolution is not finer than the contextual data. When
# coarser, the contextual data is aggregated up (average/midpoint).


def _run_linkage(tmp_path, ctx_builder, iwdate, resolution, agg="average", lags=(0, 1)):
    ctx_dir = ctx_builder(tmp_path)
    hrs = _load_survey(tmp_path, iwdate, resolution=resolution)
    hrs.agg_method = AggMethod.from_str(agg)
    cdir = DailyMeasureDataDir(
        ctx_dir, data_col="val", measure_type=None, geoid_col="GEOID10"
    )
    temp_dir = tmp_path / "lags"
    temp_dir.mkdir()
    files = process_multiple_lags_batch(
        hrs_data=hrs,
        contextual_dir=cdir,
        n_days=list(lags),
        id_col="personid",
        temp_dir=temp_dir,
        prefix="m",
        geoid_col="GEOID",
    )
    out = hrs.df[["personid"]].copy()
    for f in files:
        out = out.merge(pd.read_parquet(f), on="personid", how="left")
    return out


def _val(out, n, unit):
    return out[f"val_iwdate_{n}{unit}_prior"].iloc[0]


# ---- contextual HOURLY -----------------------------------------------------


def test_hourly_ctx_hourly_link(tmp_path):
    out = _run_linkage(tmp_path, _ctx_hourly, "2016-06-15 05:00:00", "hourly")
    assert _val(out, 0, "hour") == 105.0  # day-offset 1 * 100 + hour 5
    assert _val(out, 1, "hour") == 104.0


def test_hourly_ctx_daily_link_average(tmp_path):
    out = _run_linkage(tmp_path, _ctx_hourly, "2016-06-15", "daily", agg="average")
    assert _val(out, 0, "day") == np.mean(range(100, 124))  # 111.5
    assert _val(out, 1, "day") == np.mean(range(0, 24))  # 11.5


def test_hourly_ctx_daily_link_midpoint(tmp_path):
    out = _run_linkage(tmp_path, _ctx_hourly, "2016-06-15", "daily", agg="midpoint")
    assert _val(out, 0, "day") == 112.0  # noon of day 15 -> 100 + 12
    assert _val(out, 1, "day") == 12.0  # noon of day 14 -> 12


def test_hourly_ctx_monthly_link_average(tmp_path):
    out = _run_linkage(tmp_path, _ctx_hourly, "2016-06", "monthly", agg="average")
    # All 48 hourly values fall in June -> mean of {0..23} and {100..123}.
    expected = np.mean(list(range(0, 24)) + list(range(100, 124)))  # 61.5
    assert _val(out, 0, "month") == expected
    assert pd.isna(_val(out, 1, "month"))  # May has no data


# ---- contextual DAILY ------------------------------------------------------


def test_daily_ctx_daily_link(tmp_path):
    out = _run_linkage(tmp_path, _ctx_daily, "2016-06-15", "daily")
    assert _val(out, 0, "day") == float(pd.Timestamp("2016-06-15").dayofyear)
    assert _val(out, 1, "day") == float(pd.Timestamp("2016-06-14").dayofyear)


def test_daily_ctx_monthly_link_average(tmp_path):
    out = _run_linkage(tmp_path, _ctx_daily, "2016-06", "monthly", agg="average")
    dates = pd.date_range("2016-05-01", "2016-06-30", freq="D")
    doy = dates.dayofyear.to_numpy()
    month = dates.month.to_numpy()
    june = doy[month == 6].mean()
    may = doy[month == 5].mean()
    assert _val(out, 0, "month") == pytest.approx(june)
    assert _val(out, 1, "month") == pytest.approx(may)


# ---- contextual MONTHLY ----------------------------------------------------


def test_monthly_ctx_monthly_link(tmp_path):
    out = _run_linkage(tmp_path, _ctx_monthly, "2016-06", "monthly")
    assert _val(out, 0, "month") == 6.0
    assert _val(out, 1, "month") == 5.0


# ---- invalid combinations (linkage finer than contextual) ------------------


@pytest.mark.parametrize(
    "ctx_builder, resolution",
    [
        (_ctx_daily, "hourly"),    # daily data, hourly request
        (_ctx_monthly, "daily"),   # monthly data, daily request
        (_ctx_monthly, "hourly"),  # monthly data, hourly request
    ],
)
def test_linkage_finer_than_contextual_raises(tmp_path, ctx_builder, resolution):
    with pytest.raises(ValueError, match="finer than"):
        _run_linkage(tmp_path, ctx_builder, "2016-06-15", resolution)
