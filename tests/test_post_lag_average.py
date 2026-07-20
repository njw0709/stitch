"""
Tests for post-lag averaging and the save-temp-to-output option.

These verify that ``run_pipeline``:

1. Collapses the per-lag measure columns into a single strict-mean column per
   measure when ``post_lag_average`` is set, matching a NaN-strict row-wise mean
   of the equivalent non-averaged run.
2. Resolves the incompatibility with ``include_lag_date`` by warning and ignoring
   it (averaging wins) rather than aborting.
3. Persists the intermediate lag files as CSV in the output directory (and keeps
   them) when ``save_temp_to_output`` is set.
"""

import argparse
import tempfile

import numpy as np
import pandas as pd
import pytest

from stitch.process import cleanup_stitch_temp_dirs, run_pipeline


@pytest.fixture(autouse=True)
def _isolated_temp_root(tmp_path, monkeypatch):
    """Redirect the OS temp location so private temp dirs stay per-test."""
    temp_root = tmp_path / "ostemp"
    temp_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))
    yield
    cleanup_stitch_temp_dirs()


def _make_args(
    *,
    survey_data,
    context_dir,
    save_dir,
    residential_hist,
    output_name="linked_data.dta",
    n_lags=3,
    start_lag=0,
    parallel=False,
    include_lag_date=False,
    post_lag_average=False,
    save_temp_to_output=False,
):
    """Build an argparse.Namespace mirroring the CLI defaults for run_pipeline."""
    return argparse.Namespace(
        survey_data=str(survey_data),
        context_dir=str(context_dir),
        output_name=output_name,
        save_dir=str(save_dir),
        id_col="hhidpn",
        date_col="iwdate",
        measure_type="heat",
        data_col="index",
        geoid_col="GEOID2010",
        contextual_geoid_col="GEOID10",
        context_date_col="Date",
        file_extension=".csv",
        residential_hist=str(residential_hist),
        res_hist_id_col="hhidpn",
        res_hist_date_col="move_date",
        res_hist_geoid_col="GEOID",
        n_lags=n_lags,
        start_lag=start_lag,
        parallel=parallel,
        include_lag_date=include_lag_date,
        post_lag_average=post_lag_average,
        save_temp_to_output=save_temp_to_output,
        geoid_treatment="code",
        geoid_n_digits=11,
        geoid_numeric_type="int",
    )


def test_post_lag_average_matches_strict_rowwise_mean(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
):
    """The averaged column equals the NaN-strict row-wise mean of per-lag columns."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    n_lags = 3

    # Non-averaged run: keeps one column per lag.
    normal_args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="normal.dta",
        n_lags=n_lags,
    )
    run_pipeline(normal_args)
    normal_df = pd.read_stata(save_dir / "normal.dta").set_index("hhidpn")

    per_lag_cols = [f"index_iwdate_{n}day_prior" for n in range(n_lags)]
    for col in per_lag_cols:
        assert col in normal_df.columns, f"missing per-lag column {col}"

    # Averaged run: collapses lags into one column per measure.
    avg_args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="avg.dta",
        n_lags=n_lags,
        post_lag_average=True,
    )
    run_pipeline(avg_args)
    avg_df = pd.read_stata(save_dir / "avg.dta").set_index("hhidpn")

    avg_col = f"index_avg_0_{n_lags - 1}day_prior"
    assert avg_col in avg_df.columns

    # Per-lag columns must be gone in the averaged output.
    for col in avg_df.columns:
        assert not col.startswith("index_iwdate_"), (
            f"unexpected per-lag column left in averaged output: {col}"
        )

    # Strict mean (NaN if any lag missing), aligned by participant.
    expected = normal_df[per_lag_cols].mean(axis=1, skipna=False)
    got = avg_df[avg_col].reindex(expected.index)

    np.testing.assert_allclose(
        got.to_numpy(dtype=float),
        expected.to_numpy(dtype=float),
        equal_nan=True,
    )
    # Sanity: there is at least one real (non-NaN) averaged value.
    assert got.notna().any()


def test_include_lag_date_ignored_when_averaging(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    capsys,
):
    """Setting both flags warns and ignores include_lag_date (no exception)."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="both.dta",
        n_lags=3,
        include_lag_date=True,
        post_lag_average=True,
    )

    run_pipeline(args)

    out = capsys.readouterr().out
    assert "ignored" in out.lower()

    df = pd.read_stata(save_dir / "both.dta")
    assert "index_avg_0_2day_prior" in df.columns
    # include_lag_date was ignored: no per-lag date columns present.
    assert not any(c.startswith("iwdate_") and c.endswith("day_prior") for c in df.columns)


def test_save_temp_to_output_keeps_csv_lag_files(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
):
    """CSV lag files are written under the output dir and kept after success."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="kept.dta",
        n_lags=3,
        save_temp_to_output=True,
    )

    run_pipeline(args)

    assert (save_dir / "kept.dta").exists()

    lag_dir = save_dir / "kept_lag_files"
    assert lag_dir.is_dir()
    csv_files = sorted(lag_dir.glob("heat_lag_*.csv"))
    assert len(csv_files) == 3, f"expected 3 CSV lag files, found {csv_files}"
