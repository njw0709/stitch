"""
Tests for temp lag-file isolation, hidden storage, and guaranteed cleanup.

These tests verify the confidentiality/concurrency guarantees of the linkage
pipeline's intermediate ("lag") files:

1. Each job gets its own unique, private temp directory located in the OS temp
   location (outside the user-visible save directory) with owner-only perms.
2. The temp directory (and any confidential lag files it holds) is always
   removed after the merge -- both on success and on failure.
3. Concurrent jobs writing into the same ``save_dir`` never collide and never
   leave temp files behind.
"""

import argparse
import os
import stat
import tempfile
from pathlib import Path

import pandas as pd
import pytest

import stitch.process
from stitch.process import _create_job_temp_dir, run_pipeline


def _make_args(
    *,
    survey_data,
    context_dir,
    save_dir,
    residential_hist,
    output_name="linked_data.dta",
    n_lags=3,
    parallel=False,
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
        parallel=parallel,
        include_lag_date=False,
        geoid_treatment="code",
        geoid_n_digits=11,
        geoid_numeric_type="int",
    )


def _spy_on_temp_dirs(monkeypatch):
    """Patch _create_job_temp_dir to record every temp dir the pipeline creates."""
    created = []
    original = stitch.process._create_job_temp_dir

    def _spy(job_id=None):
        path = original(job_id)
        created.append(path)
        return path

    monkeypatch.setattr(stitch.process, "_create_job_temp_dir", _spy)
    return created


def test_job_temp_dir_isolated_and_hidden(tmp_path):
    """Each job dir is unique, lives in the OS temp area, and is owner-only."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    dir_a = _create_job_temp_dir("jobA")
    dir_b = _create_job_temp_dir("jobB")
    # Two calls without an explicit id must also differ.
    dir_c = _create_job_temp_dir()
    dir_d = _create_job_temp_dir()

    try:
        # Distinct directories -> no collision between concurrent jobs.
        assert len({dir_a, dir_b, dir_c, dir_d}) == 4

        system_temp = Path(tempfile.gettempdir()).resolve()
        for d in (dir_a, dir_b, dir_c, dir_d):
            assert d.exists() and d.is_dir()
            # Hidden from the user's save directory: located under the OS temp root.
            assert system_temp in d.resolve().parents or d.resolve().parent == system_temp
            assert save_dir.resolve() not in d.resolve().parents

        # The job id is reflected in the directory name.
        assert "jobA" in dir_a.name
        assert "jobB" in dir_b.name

        # Owner-only permissions on POSIX (mkdtemp guarantees 0o700).
        if os.name == "posix":
            for d in (dir_a, dir_b, dir_c, dir_d):
                mode = stat.S_IMODE(d.stat().st_mode)
                assert mode == 0o700, f"expected 0o700, got {oct(mode)}"
    finally:
        for d in (dir_a, dir_b, dir_c, dir_d):
            if d.exists():
                d.rmdir()


def test_run_pipeline_cleans_up_temp_dir(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """A successful run produces output and leaves no temp files anywhere."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    created = _spy_on_temp_dirs(monkeypatch)

    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
    )
    run_pipeline(args)

    # Output was written.
    out_path = save_dir / args.output_name
    assert out_path.exists()

    # Exactly one job temp dir was created, and it is gone afterwards.
    assert len(created) == 1
    assert not created[0].exists()

    # No temp lag directories leaked into the user-visible save dir.
    leftovers = [p for p in save_dir.iterdir() if p.is_dir()]
    assert leftovers == [], f"unexpected leftover directories: {leftovers}"
    assert not (save_dir / "temp_lag_files").exists()


def test_run_pipeline_cleans_up_on_failure(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """The private temp dir is removed even when the merge step raises."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    created = _spy_on_temp_dirs(monkeypatch)

    # Force the merge (which reads back the lag parquet files) to fail.
    def _boom(*_args, **_kwargs):
        raise RuntimeError("simulated merge failure")

    monkeypatch.setattr(stitch.process.pd, "read_parquet", _boom)

    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
    )

    with pytest.raises(RuntimeError, match="simulated merge failure"):
        run_pipeline(args)

    # Even on failure, the confidential temp dir must be cleaned up.
    assert len(created) == 1
    assert not created[0].exists()

    # And nothing leaked into the save dir.
    leftovers = [p for p in save_dir.iterdir() if p.is_dir()]
    assert leftovers == [], f"unexpected leftover directories: {leftovers}"


def test_concurrent_jobs_do_not_collide(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """Two jobs sharing a save_dir get separate temp dirs and independent output."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    created = _spy_on_temp_dirs(monkeypatch)

    args1 = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="job1.dta",
    )
    args2 = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="job2.dta",
    )

    run_pipeline(args1)
    run_pipeline(args2)

    out1 = save_dir / "job1.dta"
    out2 = save_dir / "job2.dta"
    assert out1.exists()
    assert out2.exists()

    # Each job used its own distinct private temp dir, all cleaned up.
    assert len(created) == 2
    assert created[0] != created[1]
    assert not created[0].exists()
    assert not created[1].exists()

    # Both outputs are complete and independent (same participants).
    df1 = pd.read_stata(out1)
    df2 = pd.read_stata(out2)
    assert len(df1) == len(df2)
    assert set(df1["hhidpn"]) == set(df2["hhidpn"])

    # No temp directories left behind in the shared save dir.
    leftovers = [p for p in save_dir.iterdir() if p.is_dir()]
    assert leftovers == [], f"unexpected leftover directories: {leftovers}"
