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
import json
import os
import stat
import tempfile
from pathlib import Path

import pandas as pd
import pytest

import stitch.process
from stitch.process import (
    PipelineCancelled,
    _create_job_temp_dir,
    _job_signature,
    cleanup_stitch_temp_dirs,
    run_pipeline,
)


@pytest.fixture(autouse=True)
def _isolated_temp_root(tmp_path, monkeypatch):
    """Redirect the OS temp location to a fresh per-test directory.

    Job temp dirs, resume discovery, and cleanup all key off
    ``tempfile.gettempdir()``. Isolating it per test prevents a leftover
    (intentionally kept) temp dir from one test from being resumed by another.
    """
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


def test_run_pipeline_keeps_temp_dir_on_failure(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """On failure the temp dir PERSISTS (for resume) and cleanup removes it."""
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

    # The temp dir is intentionally kept so an identical rerun can resume.
    assert len(created) == 1
    temp_dir = created[0]
    assert temp_dir.exists()
    # The job-args manifest and the partial lag files were left behind.
    assert (temp_dir / "job_args.json").exists()
    assert list(temp_dir.glob("heat_lag_*.parquet"))

    # Nothing leaked into the user-visible save dir.
    leftovers = [p for p in save_dir.iterdir() if p.is_dir()]
    assert leftovers == [], f"unexpected leftover directories: {leftovers}"

    # Explicit cleanup (as run on GUI startup/quit) wipes it.
    cleanup_stitch_temp_dirs()
    assert not temp_dir.exists()


def test_job_args_manifest_written_and_matches(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """The job_args.json manifest records the signature and configuration."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    created = _spy_on_temp_dirs(monkeypatch)

    def _boom(*_args, **_kwargs):
        raise RuntimeError("stop before completion")

    monkeypatch.setattr(stitch.process.pd, "read_parquet", _boom)

    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
    )
    with pytest.raises(RuntimeError):
        run_pipeline(args)

    manifest = json.loads((created[0] / "job_args.json").read_text())
    assert manifest["signature_hash"] == _job_signature(args)
    assert manifest["n_lags"] == args.n_lags
    assert manifest["args"]["measure_type"] == "heat"


def test_resume_reuses_temp_dir_and_processes_only_missing_lags(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """A rerun of an identical job resumes into the same dir and only
    reprocesses the lags whose files are missing."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    created = _spy_on_temp_dirs(monkeypatch)
    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
    )

    # --- First run fails during merge, leaving the lag files behind. ---
    real_read_parquet = stitch.process.pd.read_parquet

    def _boom(*_args, **_kwargs):
        raise RuntimeError("simulated merge failure")

    monkeypatch.setattr(stitch.process.pd, "read_parquet", _boom)
    with pytest.raises(RuntimeError, match="simulated merge failure"):
        run_pipeline(args)

    assert len(created) == 1
    temp_dir = created[0]
    lag_files = sorted(temp_dir.glob("heat_lag_*.parquet"))
    assert lag_files, "expected partial lag files from the interrupted run"

    # Remove exactly one lag file so the resume must reprocess only that lag.
    removed = lag_files[0]
    removed_n = int(removed.stem.split("_lag_")[1])
    removed.unlink()

    # --- Second run: restore real reader, spy on the batch processor. ---
    monkeypatch.setattr(stitch.process.pd, "read_parquet", real_read_parquet)

    processed_lags = []
    real_batch = stitch.process.process_multiple_lags_batch

    def _spy_batch(*a, **k):
        processed_lags.append(list(k.get("n_days")))
        return real_batch(*a, **k)

    monkeypatch.setattr(stitch.process, "process_multiple_lags_batch", _spy_batch)

    run_pipeline(args)

    # No new temp dir was created: the resume reused the existing one.
    assert len(created) == 1
    # Only the missing lag was reprocessed.
    assert processed_lags == [[removed_n]]
    # The output was produced and the completed job cleaned up its temp dir.
    assert (save_dir / args.output_name).exists()
    assert not temp_dir.exists()


def test_resume_after_midrun_cancel(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """Cancelling mid-run keeps the partial lag files; a rerun resumes them.

    This mirrors the GUI Stop button: cancellation is requested while lags are
    still being processed, so some lag files have been written and some have
    not. The rerun must reuse the same temp dir and reprocess only the lags
    that are still missing.
    """
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    created = _spy_on_temp_dirs(monkeypatch)
    args = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        n_lags=3,
        parallel=False,
    )

    # Cooperative cancel: request a stop as soon as the first lag file lands, so
    # processing unwinds partway through (like the user clicking Stop mid-run).
    def should_cancel():
        return bool(created) and any(created[0].glob("heat_lag_*.parquet"))

    with pytest.raises(PipelineCancelled):
        run_pipeline(args, should_cancel=should_cancel)

    assert len(created) == 1
    temp_dir = created[0]
    assert temp_dir.exists()
    done_before = sorted(temp_dir.glob("heat_lag_*.parquet"))
    assert 1 <= len(done_before) < args.n_lags, (
        "expected a partial set of lag files after mid-run cancel"
    )
    done_lags = {int(p.stem.split("_lag_")[1]) for p in done_before}
    expected_remaining = [n for n in range(args.n_lags) if n not in done_lags]

    # Rerun with no cancellation; spy on the processor to confirm which lags run.
    processed_lags = []
    real_batch = stitch.process.process_multiple_lags_batch

    def _spy_batch(*a, **k):
        processed_lags.append(list(k.get("n_days")))
        return real_batch(*a, **k)

    monkeypatch.setattr(stitch.process, "process_multiple_lags_batch", _spy_batch)

    run_pipeline(args)

    # Reused the same temp dir and only reprocessed the lags left unfinished.
    assert len(created) == 1
    assert processed_lags == [expected_remaining]
    assert (save_dir / args.output_name).exists()
    assert not temp_dir.exists()


def test_different_args_do_not_resume(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """A job with different configuration never resumes an unrelated temp dir."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    created = _spy_on_temp_dirs(monkeypatch)

    real_read_parquet = stitch.process.pd.read_parquet

    def _boom(*_args, **_kwargs):
        raise RuntimeError("simulated merge failure")

    monkeypatch.setattr(stitch.process.pd, "read_parquet", _boom)

    args_a = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="job_a.dta",
    )
    with pytest.raises(RuntimeError):
        run_pipeline(args_a)

    assert len(created) == 1
    dir_a = created[0]
    assert dir_a.exists()

    # A different job (distinct output_name -> distinct signature) succeeds and
    # must create its own dir rather than resuming dir_a.
    monkeypatch.setattr(stitch.process.pd, "read_parquet", real_read_parquet)
    args_b = _make_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="job_b.dta",
    )
    run_pipeline(args_b)

    assert len(created) == 2
    assert created[1] != dir_a
    # A's incomplete dir is untouched by B; B's dir is cleaned up on success.
    assert dir_a.exists()
    assert not created[1].exists()
    assert (save_dir / "job_b.dta").exists()


def test_cleanup_stitch_temp_dirs_removes_only_stitch_dirs(tmp_path):
    """cleanup_stitch_temp_dirs removes stitch_* dirs and nothing else."""
    temp_root = Path(tempfile.gettempdir())
    stitch_a = _create_job_temp_dir("aaaa")
    stitch_b = _create_job_temp_dir("bbbb")
    other = temp_root / "unrelated_dir"
    other.mkdir()

    assert stitch_a.exists() and stitch_b.exists() and other.exists()

    cleanup_stitch_temp_dirs()

    assert not stitch_a.exists()
    assert not stitch_b.exists()
    assert other.exists()


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
