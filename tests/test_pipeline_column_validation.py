"""
End-to-end tests for the configuration guards in ``run_pipeline``.

Each case is a misconfiguration that used to fail far from its cause — with a
cryptic ``TypeError``, by silently overwriting an input file, or (in parallel
mode, where per-lag exceptions are swallowed) by reporting success with nothing
linked. They must now be rejected up front, before anything is read or written.
"""

import argparse
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from stitch.io_utils import read_data
from stitch.process import cleanup_stitch_temp_dirs, run_pipeline


@pytest.fixture(autouse=True)
def _isolated_temp_root(tmp_path, monkeypatch):
    """Redirect the OS temp location so leaked job dirs are visible per test."""
    temp_root = tmp_path / "ostemp"
    temp_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))
    yield temp_root
    cleanup_stitch_temp_dirs()


def _make_args(*, survey_data, context_dir, save_dir, residential_hist=None, **overrides):
    """Build a valid argparse.Namespace for run_pipeline; override to break it."""
    args = argparse.Namespace(
        survey_data=str(survey_data),
        context_dir=str(context_dir),
        output_name="linked_data.dta",
        save_dir=str(save_dir),
        id_col="hhidpn",
        date_col="iwdate",
        measure_type="heat",
        data_col="index",
        geoid_col="GEOID2010",
        contextual_geoid_col="GEOID10",
        context_date_col="Date",
        file_extension=".csv",
        residential_hist=None if residential_hist is None else str(residential_hist),
        res_hist_id_col="hhidpn",
        res_hist_date_col="move_date",
        res_hist_geoid_col="GEOID",
        n_lags=2,
        start_lag=0,
        parallel=False,
        include_lag_date=False,
        geoid_treatment="code",
        geoid_n_digits=11,
        geoid_numeric_type="int",
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _job_temp_dirs(temp_root: Path):
    return [p for p in temp_root.iterdir() if p.name.startswith("stitch_")]


def test_valid_config_still_runs(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """Control: the guards must not block a well-formed job."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
    )

    run_pipeline(args)

    out_df = read_data(save_dir / args.output_name)
    assert "index_iwdate_0day_prior" in out_df.columns


def test_id_col_equal_to_date_col_is_rejected(
    survey_data_2016_2020, heat_index_dir, tmp_path, _isolated_temp_root
):
    """The reported bug: one column picked for both ID and date.

    Used to surface ~200 lines later as
    ``TypeError: unsupported operand type(s) for -: 'numpy.int64' and 'Timedelta'``
    after the job temp directory had already been created.
    """
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        id_col="iwdate",
    )

    with pytest.raises(ValueError, match="both set to 'iwdate'"):
        run_pipeline(args)

    assert not (save_dir / args.output_name).exists()
    assert _job_temp_dirs(_isolated_temp_root) == []


def test_geoid_col_equal_to_id_col_is_rejected(
    survey_data_2016_2020, heat_index_dir, tmp_path
):
    """Used to rewrite the ID column as GEOIDs, then fail blaming the data."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        geoid_col="hhidpn",
    )

    with pytest.raises(ValueError, match="Survey data"):
        run_pipeline(args)


def test_residential_history_collision_is_rejected(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """Res-hist move-date column doubling as its GEOID column: silent all-NaN."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        res_hist_geoid_col="move_date",
    )

    with pytest.raises(ValueError, match="Residential history"):
        run_pipeline(args)


def test_contextual_collision_is_rejected(
    survey_data_2016_2020, heat_index_dir, tmp_path
):
    """Measure column doubling as the contextual date column."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        data_col="Date",
    )

    with pytest.raises(ValueError, match="Contextual data"):
        run_pipeline(args)


def test_output_path_may_not_overwrite_the_survey_input(
    survey_data_2016_2020, heat_index_dir, tmp_path
):
    """The survey is fully in memory by write time, so this used to succeed."""
    survey_path = Path(survey_data_2016_2020)
    args = _make_args(
        survey_data=survey_path,
        context_dir=heat_index_dir,
        save_dir=survey_path.parent,
        output_name=survey_path.name,
    )
    before = survey_path.stat().st_size

    with pytest.raises(ValueError, match="same file as the survey data file"):
        run_pipeline(args)

    assert survey_path.stat().st_size == before


def test_rerunning_on_a_previous_output_is_rejected(
    fake_residential_history_file, survey_data_2016_2020, heat_index_dir, tmp_path
):
    """Feeding a linked file back in as the survey input.

    The lag builder concatenates its columns onto the survey frame, so the
    pre-existing names produce duplicate labels. In parallel mode that failure
    is swallowed per lag and the run reports success with nothing linked.
    """
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    first = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
    )
    run_pipeline(first)
    linked = save_dir / first.output_name
    assert "index_iwdate_0day_prior" in read_data(linked).columns

    second = _make_args(
        survey_data=linked,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="linked_again.dta",
    )

    with pytest.raises(ValueError, match="previous STITCH run") as excinfo:
        run_pipeline(second)
    assert "index_iwdate_0day_prior" in str(excinfo.value)
    assert not (save_dir / "linked_again.dta").exists()


def test_survey_column_check_runs_before_any_output(
    survey_data_2016_2020, heat_index_dir, tmp_path, _isolated_temp_root
):
    """A generated-name collision must not leave a job temp directory behind."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    df = read_data(survey_data_2016_2020)
    df["index_iwdate_1day_prior"] = 1.0
    survey_path = tmp_path / "already_linked.dta"
    df.to_stata(survey_path, write_index=False)

    args = _make_args(
        survey_data=survey_path, context_dir=heat_index_dir, save_dir=save_dir
    )

    with pytest.raises(ValueError, match="index_iwdate_1day_prior"):
        run_pipeline(args)

    assert _job_temp_dirs(_isolated_temp_root) == []
    assert not (save_dir / args.output_name).exists()


def test_duplicate_data_columns_from_the_cli_are_rejected(
    survey_data_2016_2020, heat_index_dir, tmp_path
):
    """``--data-col "index,index"`` reaches the readers as a duplicated usecols."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        data_col="index,index",
    )

    with pytest.raises(ValueError, match="listed 2 times"):
        run_pipeline(args)
