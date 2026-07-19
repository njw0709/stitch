"""
Tests for the multi-job dashboard UI (StitchMainWindow).

Covers the new capabilities:
- Adding multiple jobs to the queue (via the config wizard flow).
- Editing an existing job (wizard prefill round-trip + status reset).
- Opening the selected job's output directory.
- Running all queued jobs sequentially through the shared execution panel
  (real end-to-end pipeline integration).

These tests use Qt's offscreen platform in headless environments (configured in
conftest.py) and pytest-qt's ``qtbot``/``qapp`` fixtures.
"""

import argparse

import pytest

# Skip the whole module if the GUI stack is unavailable.
pytest.importorskip("PyQt6")

from PyQt6.QtWidgets import QWizard
from PyQt6.QtGui import QDesktopServices

from stitch.gui.main_window import StitchMainWindow, JobConfigWizard, STATUS_COLORS
from stitch.gui.pages.execution_page import ExecutionDialog
from stitch.gui.job import (
    build_args_from_wizard,
    STATUS_PENDING,
    STATUS_DONE,
    STATUS_FAILED,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job_args(
    *,
    survey_data,
    context_dir,
    save_dir,
    residential_hist,
    output_name="linked_data.dta",
    n_lags=3,
    parallel=False,
):
    """Build a fully-populated args namespace matching the config wizard fields."""
    return argparse.Namespace(
        survey_data=str(survey_data),
        context_dir=str(context_dir),
        output_name=output_name,
        save_dir=str(save_dir),
        id_col="hhidpn",
        date_col="iwdate",
        measure_type="heat",
        data_col="index",
        geoid_col="GEOID2010_2010",
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


def _add_job_via_ui(window, args, monkeypatch):
    """Drive the real "New Job" flow, prefilling the wizard from *args*.

    Patches the wizard's ``exec`` so it accepts immediately after loading the
    given args; the dashboard then rebuilds the job via ``build_args_from_wizard``,
    exercising add + prefill + arg extraction together.
    """

    def fake_exec(self):
        self.load_args(args)
        return QWizard.DialogCode.Accepted.value

    monkeypatch.setattr(JobConfigWizard, "exec", fake_exec)
    window._on_new_job()


def _item_bg(window, index):
    return window.jobs_list.item(index).background().color().name().lower()


# ---------------------------------------------------------------------------
# Adding multiple jobs
# ---------------------------------------------------------------------------


def test_add_multiple_jobs(
    qtbot,
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """Adding several jobs queues them all as pending with distinct names."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    window = StitchMainWindow()
    qtbot.addWidget(window)

    # Empty queue: Run All / Edit / Remove disabled.
    assert window.run_all_button.isEnabled() is False
    assert window.edit_button.isEnabled() is False
    assert window.remove_button.isEnabled() is False

    for i in range(3):
        args = _make_job_args(
            survey_data=survey_data_2016_2020,
            context_dir=heat_index_dir,
            save_dir=save_dir,
            residential_hist=fake_residential_history_file,
            output_name=f"job_{i}.dta",
        )
        _add_job_via_ui(window, args, monkeypatch)

    assert len(window.jobs) == 3
    assert window.jobs_list.count() == 3

    # All pending, uniquely named, gray-highlighted.
    names = set()
    for i, job in enumerate(window.jobs):
        assert job.status == STATUS_PENDING
        assert window.jobs_list.item(i).text().startswith("[Pending]")
        assert _item_bg(window, i) == STATUS_COLORS[STATUS_PENDING][0].lower()
        names.add(job.name)
    assert len(names) == 3, "Job names should be unique"

    # With pending jobs, action buttons are enabled.
    assert window.run_all_button.isEnabled() is True
    assert window.edit_button.isEnabled() is True
    assert window.remove_button.isEnabled() is True

    # The rebuilt args survived the wizard round-trip.
    job0 = window.jobs[0]
    assert job0.args.survey_data == str(survey_data_2016_2020)
    assert job0.args.context_dir == str(heat_index_dir)
    assert job0.args.measure_type == "heat"
    assert job0.args.data_col == "index"
    assert job0.args.output_name == "job_0.dta"
    assert job0.args.residential_hist == str(fake_residential_history_file)


def test_remove_selected_job(
    qtbot,
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """Removing a selected job drops it from the queue and the list."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    window = StitchMainWindow()
    qtbot.addWidget(window)

    for i in range(2):
        args = _make_job_args(
            survey_data=survey_data_2016_2020,
            context_dir=heat_index_dir,
            save_dir=save_dir,
            residential_hist=fake_residential_history_file,
            output_name=f"job_{i}.dta",
        )
        _add_job_via_ui(window, args, monkeypatch)

    window.jobs_list.setCurrentRow(0)
    kept_name = window.jobs[1].name
    window._on_remove_selected()

    assert len(window.jobs) == 1
    assert window.jobs_list.count() == 1
    assert window.jobs[0].name == kept_name


# ---------------------------------------------------------------------------
# Editing jobs
# ---------------------------------------------------------------------------


def test_edit_job_prefill_round_trip(
    qtbot,
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
):
    """Loading a job's args into a fresh wizard and rebuilding reproduces them."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    args = _make_job_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="edited.dta",
        n_lags=5,
    )

    wizard = JobConfigWizard()
    qtbot.addWidget(wizard)
    wizard.load_args(args)

    rebuilt = build_args_from_wizard(wizard)

    for attr in (
        "survey_data",
        "context_dir",
        "output_name",
        "save_dir",
        "id_col",
        "date_col",
        "measure_type",
        "data_col",
        "geoid_col",
        "contextual_geoid_col",
        "context_date_col",
        "file_extension",
        "residential_hist",
        "res_hist_id_col",
        "res_hist_date_col",
        "res_hist_geoid_col",
        "n_lags",
        "parallel",
        "include_lag_date",
        "geoid_treatment",
        "geoid_n_digits",
        "geoid_numeric_type",
    ):
        assert getattr(rebuilt, attr) == getattr(args, attr), (
            f"{attr}: {getattr(rebuilt, attr)!r} != {getattr(args, attr)!r}"
        )


def test_edit_selected_updates_job_and_resets_status(
    qtbot,
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """Editing a completed job updates its args and returns it to Pending."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    window = StitchMainWindow()
    qtbot.addWidget(window)

    args = _make_job_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="orig.dta",
        n_lags=3,
    )
    _add_job_via_ui(window, args, monkeypatch)

    # Pretend it already ran successfully.
    window.jobs[0].status = STATUS_DONE
    window._refresh_job_item(0)
    assert _item_bg(window, 0) == STATUS_COLORS[STATUS_DONE][0].lower()

    # Edit: change n_lags via a prefilled-then-modified wizard.
    edited_args = _make_job_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="orig.dta",
        n_lags=7,
    )

    def fake_exec(self):
        self.load_args(edited_args)
        return QWizard.DialogCode.Accepted.value

    monkeypatch.setattr(JobConfigWizard, "exec", fake_exec)
    window.jobs_list.setCurrentRow(0)
    window._on_edit_selected()

    assert window.jobs[0].args.n_lags == 7
    assert window.jobs[0].status == STATUS_PENDING
    assert window.jobs_list.item(0).text().startswith("[Pending]")
    assert _item_bg(window, 0) == STATUS_COLORS[STATUS_PENDING][0].lower()


# ---------------------------------------------------------------------------
# Open output directory (selected job)
# ---------------------------------------------------------------------------


def test_open_output_opens_selected_job_dir(qtbot, tmp_path, monkeypatch):
    """The Open Output button opens the *selected* job's save_dir."""
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    window = StitchMainWindow()
    qtbot.addWidget(window)

    from stitch.gui.job import Job

    window.jobs.append(
        Job(name="A", args=argparse.Namespace(save_dir=str(dir_a)))
    )
    window.jobs.append(
        Job(name="B", args=argparse.Namespace(save_dir=str(dir_b)))
    )
    for job in window.jobs:
        window._add_job_item(job)
    window._refresh_buttons()

    opened = []
    monkeypatch.setattr(
        QDesktopServices,
        "openUrl",
        staticmethod(lambda url: opened.append(url.toLocalFile()) or True),
    )

    window.jobs_list.setCurrentRow(1)
    window._on_open_output()
    assert opened[-1] == str(dir_b)

    window.jobs_list.setCurrentRow(0)
    window._on_open_output()
    assert opened[-1] == str(dir_a)


# ---------------------------------------------------------------------------
# Sequential run integration (modal ExecutionDialog + single worker thread)
# ---------------------------------------------------------------------------


def _run_pending_via_dialog(window, qtbot, timeout=240000):
    """Run the window's pending jobs through an ExecutionDialog without blocking.

    Mirrors ``StitchMainWindow._on_run_all`` but drives the dialog with
    ``start()`` + ``qtbot.waitUntil`` instead of the blocking ``exec()``.
    """
    run_items = [
        (i, job)
        for i, job in enumerate(window.jobs)
        if job.status == STATUS_PENDING
    ]
    dialog = ExecutionDialog(run_items, window)
    dialog.job_status_changed.connect(window._on_job_status_changed)
    qtbot.addWidget(dialog)
    dialog.start()
    qtbot.waitUntil(dialog.is_finished, timeout=timeout)
    window._refresh_buttons()
    return dialog


def test_run_all_sequential_integration(
    qtbot,
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """Run all queued jobs sequentially through the real pipeline in one thread."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    window = StitchMainWindow()
    qtbot.addWidget(window)

    output_names = ["job_a.dta", "job_b.dta"]
    for name in output_names:
        args = _make_job_args(
            survey_data=survey_data_2016_2020,
            context_dir=heat_index_dir,
            save_dir=save_dir,
            residential_hist=fake_residential_history_file,
            output_name=name,
            n_lags=2,
            parallel=False,
        )
        _add_job_via_ui(window, args, monkeypatch)

    assert len(window.jobs) == 2

    dialog = _run_pending_via_dialog(window, qtbot)

    # The dialog ran every job and reported them all as succeeded.
    assert dialog._total == 2
    assert dialog._succeeded == 2
    assert dialog._failed == 0
    assert dialog.close_button.isEnabled() is True

    # Both jobs succeeded, are colored green, and produced output files.
    for i, name in enumerate(output_names):
        assert window.jobs[i].status == STATUS_DONE, window.jobs[i].status
        assert _item_bg(window, i) == STATUS_COLORS[STATUS_DONE][0].lower()
        assert (save_dir / name).exists(), f"missing output for {name}"

    # No pending jobs remain, so Run All is disabled again.
    assert window.run_all_button.isEnabled() is False


def test_run_all_marks_failed_job(
    qtbot,
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """A bad job is marked Failed (red) while the rest of the queue still runs."""
    save_dir = tmp_path / "save"
    save_dir.mkdir()

    window = StitchMainWindow()
    qtbot.addWidget(window)

    # First job: valid.
    good = _make_job_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="good.dta",
        n_lags=2,
    )
    _add_job_via_ui(window, good, monkeypatch)

    # Second job: points at a non-existent contextual directory -> run_pipeline raises.
    bad = _make_job_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="bad.dta",
        n_lags=2,
    )
    bad.context_dir = str(tmp_path / "does_not_exist")
    from stitch.gui.job import Job, default_job_name

    window._job_counter += 1
    window.jobs.append(Job(name=default_job_name(bad, window._job_counter), args=bad))
    window._add_job_item(window.jobs[-1])
    window._refresh_buttons()

    dialog = _run_pending_via_dialog(window, qtbot)

    assert dialog._succeeded == 1
    assert dialog._failed == 1
    assert window.jobs[0].status == STATUS_DONE
    assert window.jobs[1].status == STATUS_FAILED
    assert _item_bg(window, 0) == STATUS_COLORS[STATUS_DONE][0].lower()
    assert _item_bg(window, 1) == STATUS_COLORS[STATUS_FAILED][0].lower()
