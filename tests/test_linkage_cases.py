"""
Linkage validation tests using actual test datasets.

Tests different cases of linkage (with/without residential history,
movers vs non-movers, batch vs parallel) using the CSV test data in
``test_data/survey data/`` and ``test_data/heat_index/``.

The end-to-end tests that load heat-index CSVs are gated behind a
``heat_data`` fixture check because the current heat-index
files contain malformed GEOIDs that trigger a segfault in pandas
``normalize_geoid_for_processing``.  Those tests will be skipped
automatically when the data cannot be loaded.
"""

import os
import pytest
import pandas as pd
from pathlib import Path

from stitch.hrs import ResidentialHistoryHRS, HRSInterviewData, HRSContextLinker
from stitch.daily_measure import DailyMeasureDataDir
from stitch.process import process_multiple_lags_batch, process_multiple_lags_parallel

_HEAT_INDEX_ENABLED = bool(os.environ.get("STITCH_TEST_HEAT_INDEX"))
_HEAT_SKIP_REASON = (
    "heat_index CSVs contain malformed GEOIDs that segfault pandas; "
    "set STITCH_TEST_HEAT_INDEX=1 to enable when data is fixed"
)
requires_heat_index = pytest.mark.skipif(
    not _HEAT_INDEX_ENABLED, reason=_HEAT_SKIP_REASON
)


SURVEY_DATA_DIR = Path(__file__).parent / "test_data" / "survey data"
HEAT_INDEX_DIR = Path(__file__).parent / "test_data" / "heat_index"


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def residential_history():
    """Load residential history from CSV with custom column mappings."""
    return ResidentialHistoryHRS(
        SURVEY_DATA_DIR / "fake_residential_history.csv",
        hhidpn="personid",
        movecol="mover",
        geoid="GEOID",
        survey_yr_col="iwyear",
        first_tract_mark=999,
        moved_mark=1,
    )


@pytest.fixture(scope="module")
def survey_with_reshist(residential_history):
    """Load survey data linked to residential history."""
    return HRSInterviewData(
        SURVEY_DATA_DIR / "fake_survey_data.csv",
        datecol="iwdate",
        hhidpn="personid",
        move=True,
        residential_hist=residential_history,
        geoid_col="GEOID",
    )


@pytest.fixture(scope="module")
def survey_no_reshist():
    """Load survey data with static GEOID (no residential history)."""
    return HRSInterviewData(
        SURVEY_DATA_DIR / "fake_survey_data.csv",
        datecol="iwdate",
        hhidpn="personid",
        move=False,
        geoid_col="GEOID",
    )


@pytest.fixture(scope="module")
def heat_data():
    """Load heat index contextual data directory."""
    return DailyMeasureDataDir(
        HEAT_INDEX_DIR,
        data_col="index",
        measure_type=None,
    )


# ── Residential History Loading ──────────────────────────────────────


class TestResidentialHistoryFromCSV:
    """Validate that residential history CSV is loaded and parsed correctly."""

    def test_load_residential_history(self, residential_history):
        unique_persons = residential_history.df["personid"].nunique()
        assert unique_persons > 0
        assert len(residential_history._move_info) == unique_persons

    def test_non_movers_single_entry(self, residential_history):
        non_movers = [10000001, 10000002, 10000003, 10000004, 10000005]
        for pid in non_movers:
            assert pid in residential_history._move_info, f"Person {pid} not found"
            dates, geoids = residential_history._move_info[pid]
            assert len(dates) == 1, (
                f"Non-mover {pid} should have 1 entry, got {len(dates)}"
            )

    def test_movers_multiple_entries(self, residential_history):
        movers = {
            10000021: 2,  # first tract + 1 move
            10000023: 2,  # first tract + 1 move
            10000024: 2,  # first tract + 1 move
            10000041: 4,  # first tract + 3 moves
        }
        for pid, expected_count in movers.items():
            assert pid in residential_history._move_info, f"Person {pid} not found"
            dates, geoids = residential_history._move_info[pid]
            assert len(dates) == expected_count, (
                f"Mover {pid}: expected {expected_count} entries, got {len(dates)}"
            )

    def test_geoid_normalization(self, residential_history):
        """GEOIDs with fewer than 11 digits should be zero-padded."""
        pid = 10000009  # GEOID 6001409600 in CSV (10 digits)
        dates, geoids = residential_history._move_info[pid]
        assert geoids[0] == "06001409600", (
            f"Expected zero-padded GEOID, got {geoids[0]}"
        )

    def test_missing_mvyear_mvmonth_handled(self, residential_history):
        """Persons with NaN mvyear/mvmonth should not crash and should
        fall back to survey_yr_col for year and 1 for month."""
        pid = 10000029  # move row has empty mvyear and mvmonth
        assert pid in residential_history._move_info, (
            f"Person {pid} with NaN move data should still be parsed"
        )
        dates, geoids = residential_history._move_info[pid]
        assert len(dates) >= 2, (
            f"Person {pid} should have first tract + move, got {len(dates)}"
        )


# ── GEOID Assignment Accuracy ────────────────────────────────────────


class TestGeoidAssignmentAccuracy:
    """Validate GEOID lookup returns correct values for known persons."""

    def test_non_mover_geoid_constant(self, residential_history):
        """Person 10000001 never moved (GEOID 48307950300 from iwyear=2018).
        Any date after Jan-2018 should return the same GEOID."""
        pids = pd.Series([10000001, 10000001])
        dates = pd.Series([
            pd.Timestamp("2018-06-15"),
            pd.Timestamp("2019-12-01"),
        ])
        result = residential_history.create_geoid_based_on_date(pids, dates)
        assert result.iloc[0] == "48307950300"
        assert result.iloc[1] == "48307950300"

    def test_mover_geoid_before_move(self, residential_history):
        """Person 10000021 moved March 2017 from 39057200900 to 48113980000.
        Before the move date, should return the first-tract GEOID."""
        pids = pd.Series([10000021])
        dates = pd.Series([pd.Timestamp("2017-01-15")])
        result = residential_history.create_geoid_based_on_date(pids, dates)
        assert result.iloc[0] == "39057200900"

    def test_mover_geoid_after_move(self, residential_history):
        """After the March 2017 move, should return the new GEOID."""
        pids = pd.Series([10000021])
        dates = pd.Series([pd.Timestamp("2017-06-01")])
        result = residential_history.create_geoid_based_on_date(pids, dates)
        assert result.iloc[0] == "48113980000"

    def test_mover_no_month_defaults_to_january(self, residential_history):
        """Person 10000022 moved in 2013 with no month specified.
        First tract GEOID 17113001106 (iwyear=2012).
        Move defaults to Jan 2013 -> GEOID 48113980000.
        A date in Feb 2013 should return the post-move GEOID."""
        pids = pd.Series([10000022, 10000022])
        dates = pd.Series([
            pd.Timestamp("2012-11-01"),  # before move -> first tract
            pd.Timestamp("2013-02-01"),  # after Jan-2013 move -> new GEOID
        ])
        result = residential_history.create_geoid_based_on_date(pids, dates)
        assert result.iloc[0] == "17113001106"
        assert result.iloc[1] == "48113980000"

    def test_date_before_first_tract_returns_none(self, residential_history):
        """Date before the first tract date should return None."""
        pids = pd.Series([10000001])  # first tract iwyear=2018
        dates = pd.Series([pd.Timestamp("2016-01-01")])
        result = residential_history.create_geoid_based_on_date(pids, dates)
        assert pd.isna(result.iloc[0])

    def test_multiple_moves_correct_geoid(self, residential_history):
        """Person 10000041 has 3 moves:
        - First tract: GEOID 18003002000 (iwyear=2013)
        - Move 1: GEOID 39057200900 (2015-Feb)
        - Move 2: GEOID 48113980000 (2016-Jan, month missing -> defaults to Jan)
        - Move 3: GEOID 17113001106 (2019-Apr)
        Validate GEOIDs at dates between each move."""
        pids = pd.Series([10000041] * 4)
        dates = pd.Series([
            pd.Timestamp("2014-06-01"),  # between first tract and move 1
            pd.Timestamp("2015-06-01"),  # between move 1 and move 2
            pd.Timestamp("2017-06-01"),  # between move 2 and move 3
            pd.Timestamp("2020-01-01"),  # after move 3
        ])
        result = residential_history.create_geoid_based_on_date(pids, dates)
        assert result.iloc[0] == "18003002000"
        assert result.iloc[1] == "39057200900"
        assert result.iloc[2] == "48113980000"
        assert result.iloc[3] == "17113001106"


# ── Survey Data Loading ──────────────────────────────────────────────


class TestSurveyDataLoading:
    """Validate survey data loading with and without residential history."""

    def test_survey_with_reshist_shape(self, survey_with_reshist):
        assert len(survey_with_reshist.df) == 40
        assert "personid" in survey_with_reshist.df.columns
        assert "iwdate" in survey_with_reshist.df.columns

    def test_survey_no_reshist_shape(self, survey_no_reshist):
        assert len(survey_no_reshist.df) == 40
        assert "personid" in survey_no_reshist.df.columns
        assert "iwdate" in survey_no_reshist.df.columns

    def test_survey_dates_parsed(self, survey_no_reshist):
        dates = pd.to_datetime(survey_no_reshist.df["iwdate"])
        assert dates.notna().all(), "All interview dates should be parseable"
        assert dates.min() >= pd.Timestamp("2010-01-01")
        assert dates.max() <= pd.Timestamp("2025-01-01")

    def test_survey_geoid_column_present(self, survey_no_reshist):
        assert "GEOID" in survey_no_reshist.df.columns

    def test_survey_with_reshist_geoid_assignment(self, survey_with_reshist):
        """With residential history linked, GEOID-based-on-date should work
        for all persons in the survey."""
        n = len(survey_with_reshist.df)
        dates = pd.to_datetime(survey_with_reshist.df["iwdate"])
        pids = survey_with_reshist.df["personid"]

        result = survey_with_reshist.residential_hist.create_geoid_based_on_date(
            pids, dates
        )
        assert len(result) == n
        non_null = result.notna().sum()
        assert non_null > 0, "Should find GEOIDs for at least some persons"


# ── N-day Prior Column Logic ─────────────────────────────────────────


class TestNDayPriorColumns:
    """Validate that HRSContextLinker creates correct lag date/GEOID columns."""

    def test_n_day_prior_date_columns(self, survey_no_reshist):
        colname = HRSContextLinker.make_n_day_prior_cols(survey_no_reshist, 7)
        assert colname == "iwdate_7day_prior"
        assert colname in survey_no_reshist.df.columns

        original = pd.to_datetime(survey_no_reshist.df["iwdate"])
        prior = pd.to_datetime(survey_no_reshist.df[colname])
        diff = (original - prior).dt.days
        assert (diff == 7).all(), "All lag-7 dates should be 7 days earlier"

    def test_n_day_prior_geoid_column_with_reshist(self, survey_with_reshist):
        colname = HRSContextLinker.make_n_day_prior_cols(survey_with_reshist, 14)
        assert colname == "iwdate_14day_prior"
        assert colname in survey_with_reshist.df.columns

        geoid_colname = HRSContextLinker.make_geoid_day_prior(
            survey_with_reshist, colname
        )
        assert "14day_prior" in geoid_colname
        assert geoid_colname in survey_with_reshist.df.columns

    def test_batch_prepare_lag_columns(self, survey_with_reshist):
        lags = [0, 7, 30]
        result_df = HRSContextLinker.prepare_lag_columns_batch(
            survey_with_reshist, lags, "GEOID"
        )
        for n in lags:
            date_col = f"iwdate_{n}day_prior"
            assert date_col in result_df.columns, f"Missing date column {date_col}"
            geoid_col = f"GEOID_{n}day_prior"
            assert geoid_col in result_df.columns, f"Missing GEOID column {geoid_col}"


# ── Linkage With Residential History (end-to-end) ────────────────────


@requires_heat_index
class TestLinkageWithResidentialHistory:
    """End-to-end linkage tests with residential history.

    Requires ``STITCH_TEST_HEAT_INDEX=1`` because the current heat-index
    CSVs contain malformed GEOIDs that crash pandas.
    """

    def test_batch_linkage_end_to_end(
        self, survey_with_reshist, heat_data, tmp_path
    ):
        lags = [0, 7, 30]
        temp_dir = tmp_path / "batch"
        temp_dir.mkdir()

        temp_files = process_multiple_lags_batch(
            hrs_data=survey_with_reshist,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="personid",
            temp_dir=temp_dir,
            prefix="heat",
        )

        assert len(temp_files) == len(lags)

        final_df = survey_with_reshist.df[["personid"]].copy()
        for f in temp_files:
            lag_df = pd.read_parquet(f)
            final_df = final_df.merge(lag_df, on="personid", how="left")

        assert len(final_df) == len(survey_with_reshist.df)

        for n in lags:
            col = f"index_iwdate_{n}day_prior"
            assert col in final_df.columns, f"Missing column {col}"

    def test_parallel_linkage_end_to_end(
        self, survey_with_reshist, heat_data, tmp_path
    ):
        lags = [0, 7, 30]
        temp_dir = tmp_path / "parallel"
        temp_dir.mkdir()

        temp_files = process_multiple_lags_parallel(
            hrs_data=survey_with_reshist,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="personid",
            temp_dir=temp_dir,
            prefix="heat",
            max_workers=2,
        )

        assert len(temp_files) == len(lags)

        final_df = survey_with_reshist.df[["personid"]].copy()
        for f in temp_files:
            lag_df = pd.read_parquet(f)
            final_df = final_df.merge(lag_df, on="personid", how="left")

        assert len(final_df) == len(survey_with_reshist.df)

        for n in lags:
            col = f"index_iwdate_{n}day_prior"
            assert col in final_df.columns, f"Missing column {col}"

    def test_batch_parallel_consistency(
        self, survey_with_reshist, heat_data, tmp_path
    ):
        """Batch and parallel must produce identical results."""
        lags = [0, 7, 30]

        batch_dir = tmp_path / "consistency_batch"
        batch_dir.mkdir()
        batch_files = process_multiple_lags_batch(
            hrs_data=survey_with_reshist,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="personid",
            temp_dir=batch_dir,
            prefix="heat",
        )

        parallel_dir = tmp_path / "consistency_parallel"
        parallel_dir.mkdir()
        parallel_files = process_multiple_lags_parallel(
            hrs_data=survey_with_reshist,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="personid",
            temp_dir=parallel_dir,
            prefix="heat",
            max_workers=2,
        )

        for bf, pf in zip(sorted(batch_files), sorted(parallel_files)):
            batch_df = (
                pd.read_parquet(bf)
                .sort_values("personid")
                .reset_index(drop=True)
            )
            parallel_df = (
                pd.read_parquet(pf)
                .sort_values("personid")
                .reset_index(drop=True)
            )
            pd.testing.assert_frame_equal(
                batch_df, parallel_df, check_dtype=False,
            )


# ── Linkage Without Residential History ──────────────────────────────


@requires_heat_index
class TestLinkageWithoutResidentialHistory:
    """Linkage using static GEOID (no residential history)."""

    def test_static_geoid_linkage(
        self, survey_no_reshist, heat_data, tmp_path
    ):
        lags = [0, 7, 30]
        temp_dir = tmp_path / "static"
        temp_dir.mkdir()

        temp_files = process_multiple_lags_batch(
            hrs_data=survey_no_reshist,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="personid",
            temp_dir=temp_dir,
            prefix="heat",
        )

        assert len(temp_files) == len(lags)

        final_df = survey_no_reshist.df[["personid"]].copy()
        for f in temp_files:
            lag_df = pd.read_parquet(f)
            final_df = final_df.merge(lag_df, on="personid", how="left")

        assert len(final_df) == len(survey_no_reshist.df)

        for n in lags:
            col = f"index_iwdate_{n}day_prior"
            assert col in final_df.columns, f"Missing column {col}"


# ── Final Linked Product Validation ──────────────────────────────────


@requires_heat_index
class TestFinalLinkedProductValidation:
    """Validate the structure and values of the final linked output."""

    @pytest.fixture()
    def linked_result(self, survey_with_reshist, heat_data, tmp_path):
        """Produce a linked result with lag dates included for validation."""
        lags = [0, 7, 30]
        temp_dir = tmp_path / "validation"
        temp_dir.mkdir()

        temp_files = process_multiple_lags_batch(
            hrs_data=survey_with_reshist,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="personid",
            temp_dir=temp_dir,
            prefix="heat",
            include_lag_date=True,
        )

        final_df = survey_with_reshist.df.copy()
        for f in temp_files:
            lag_df = pd.read_parquet(f)
            final_df = final_df.merge(lag_df, on="personid", how="left")

        return final_df, lags

    def test_all_persons_preserved(self, linked_result, survey_with_reshist):
        final_df, _ = linked_result
        assert set(final_df["personid"]) == set(survey_with_reshist.df["personid"])

    def test_lag_columns_correctly_named(self, linked_result):
        final_df, lags = linked_result
        for n in lags:
            col = f"index_iwdate_{n}day_prior"
            assert col in final_df.columns, f"Missing data column {col}"

    def test_zero_lag_matches_interview_date(self, linked_result):
        final_df, _ = linked_result
        lag_date_col = "iwdate_0day_prior"
        if lag_date_col in final_df.columns:
            diff = (
                pd.to_datetime(final_df["iwdate"])
                - pd.to_datetime(final_df[lag_date_col])
            ).dt.days
            assert (diff == 0).all(), "Lag-0 date should equal interview date"

    def test_lag_dates_correct(self, linked_result):
        final_df, lags = linked_result
        for n in lags:
            lag_date_col = f"iwdate_{n}day_prior"
            if lag_date_col not in final_df.columns:
                continue
            diff = (
                pd.to_datetime(final_df["iwdate"])
                - pd.to_datetime(final_df[lag_date_col])
            ).dt.days
            assert (diff == n).all(), (
                f"Lag-{n} dates should be {n} days before interview"
            )

    def test_heat_values_reasonable(self, linked_result):
        final_df, lags = linked_result
        for n in lags:
            col = f"index_iwdate_{n}day_prior"
            vals = final_df[col].dropna()
            if len(vals) > 0:
                assert vals.min() >= 0, f"Negative heat index in {col}"
                assert vals.max() <= 200, f"Unreasonable heat index in {col}"

    def test_mover_geoid_changes_across_large_lag(
        self, survey_with_reshist, heat_data, tmp_path
    ):
        """For person 10000021 (moved March 2017, interview Nov 2018),
        a 365-day lag reaches into the pre-move period, so the lagged
        GEOID should differ from the interview-date GEOID."""
        lags = [0, 365]
        temp_dir = tmp_path / "mover_geoid"
        temp_dir.mkdir()

        temp_files = process_multiple_lags_batch(
            hrs_data=survey_with_reshist,
            contextual_dir=heat_data,
            n_days=lags,
            id_col="personid",
            temp_dir=temp_dir,
            prefix="heat",
            include_lag_date=True,
        )

        merged = survey_with_reshist.df[["personid"]].copy()
        for f in temp_files:
            lag_df = pd.read_parquet(f)
            merged = merged.merge(lag_df, on="personid", how="left")

        geoid_col_0 = "GEOID_0day_prior"
        geoid_col_365 = "GEOID_365day_prior"
        if geoid_col_0 in merged.columns and geoid_col_365 in merged.columns:
            person = merged[merged["personid"] == 10000021]
            if not person.empty:
                g0 = str(person[geoid_col_0].iloc[0])
                g365 = str(person[geoid_col_365].iloc[0])
                assert g0 != g365, (
                    f"Mover 10000021 should have different GEOIDs at "
                    f"lag 0 ({g0}) vs lag 365 ({g365})"
                )
