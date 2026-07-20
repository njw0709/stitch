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
    Job,
    build_args_from_wizard,
    STATUS_PENDING,
    STATUS_RUNNING,
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
    start_lag=0,
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
        start_lag=start_lag,
        parallel=parallel,
        include_lag_date=False,
        post_lag_average=False,
        save_temp_to_output=False,
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

    # Empty queue: Edit / Remove disabled; Run All stays enabled so it can warn.
    assert window.run_all_button.isEnabled() is True
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
        "start_lag",
        "parallel",
        "include_lag_date",
        "post_lag_average",
        "save_temp_to_output",
        "geoid_treatment",
        "geoid_n_digits",
        "geoid_numeric_type",
    ):
        assert getattr(rebuilt, attr) == getattr(
            args, attr
        ), f"{attr}: {getattr(rebuilt, attr)!r} != {getattr(args, attr)!r}"


def test_pipeline_config_lag_range_validation(qtbot, tmp_path):
    """validatePage rejects an inverted range and the helper reflects the count."""
    from stitch.gui.pages.pipeline_config_page import PipelineConfigPage

    page = PipelineConfigPage()
    qtbot.addWidget(page)

    # The "Add Job" button stays enabled regardless of field state.
    assert page.isComplete() is True

    # Satisfy the other requirements (valid save dir + output name).
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    page.save_dir_picker.set_path(str(save_dir))
    page.output_name_edit.setText("out.dta")

    # Inverted range is invalid: validatePage fails and highlights the spinboxes.
    page.start_lag_spin.setValue(10)
    page.end_lag_spin.setValue(5)
    assert page.validatePage() is False
    assert page.start_lag_spin.styleSheet() == PipelineConfigPage.ERROR_STYLE
    assert page.end_lag_spin.styleSheet() == PipelineConfigPage.ERROR_STYLE

    # Valid inclusive range clears the errors and passes; helper shows the count.
    page.start_lag_spin.setValue(2)
    page.end_lag_spin.setValue(6)
    assert page.validatePage() is True
    assert page.start_lag_spin.styleSheet() == ""
    assert page.lag_count_label.text() == "(5 lags)"


def test_post_lag_average_and_include_lag_date_mutually_exclusive(qtbot):
    """Checking averaging disables/unchecks include-lag-date and vice versa."""
    from stitch.gui.pages.pipeline_config_page import PipelineConfigPage

    page = PipelineConfigPage()
    qtbot.addWidget(page)

    # The info icon exposes the strict-NaN explanation on hover.
    assert page.post_lag_average_info.toolTip().strip() != ""

    # Both start enabled and unchecked.
    assert page.post_lag_average_checkbox.isEnabled()
    assert page.include_lag_date_checkbox.isEnabled()

    # Enabling averaging unchecks + disables include-lag-date.
    page.include_lag_date_checkbox.setChecked(True)
    page.post_lag_average_checkbox.setChecked(True)
    assert page.include_lag_date_checkbox.isChecked() is False
    assert page.include_lag_date_checkbox.isEnabled() is False

    # Turning averaging back off re-enables include-lag-date.
    page.post_lag_average_checkbox.setChecked(False)
    assert page.include_lag_date_checkbox.isEnabled() is True

    # Symmetric: enabling include-lag-date disables averaging.
    page.include_lag_date_checkbox.setChecked(True)
    assert page.post_lag_average_checkbox.isEnabled() is False
    page.include_lag_date_checkbox.setChecked(False)
    assert page.post_lag_average_checkbox.isEnabled() is True


def test_pipeline_config_highlights_missing_fields(qtbot):
    """Clicking Add Job with missing save dir / output name flags those fields."""
    from stitch.gui.pages.pipeline_config_page import PipelineConfigPage

    page = PipelineConfigPage()
    qtbot.addWidget(page)

    page.output_name_edit.setText("")  # clear the default filename

    assert page.validatePage() is False
    assert page.save_dir_picker.path_edit.styleSheet() == PipelineConfigPage.ERROR_STYLE
    assert page.output_name_edit.styleSheet() == PipelineConfigPage.ERROR_STYLE
    assert page.validation_label.text() != ""

    # Editing a flagged field clears its highlight immediately.
    page.output_name_edit.setText("out.dta")
    assert page.output_name_edit.styleSheet() == ""


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

    window.jobs.append(Job(name="A", args=argparse.Namespace(save_dir=str(dir_a))))
    window.jobs.append(Job(name="B", args=argparse.Namespace(save_dir=str(dir_b))))
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
# Run All warnings + Quit button
# ---------------------------------------------------------------------------


def test_run_all_warns_when_no_jobs(qtbot, monkeypatch):
    """Clicking Run All with an empty queue pops a 'no jobs' warning."""
    from PyQt6.QtWidgets import QMessageBox

    window = StitchMainWindow()
    qtbot.addWidget(window)

    warnings = []
    monkeypatch.setattr(
        QMessageBox,
        "warning",
        staticmethod(lambda *a, **k: warnings.append((a[1], a[2]))),
    )

    window._on_run_all()

    assert len(warnings) == 1
    assert warnings[0][0] == "No Jobs in Queue"


def test_run_all_warns_when_all_jobs_ran(
    qtbot,
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """Clicking Run All when nothing is pending pops an 'all jobs ran' warning."""
    from PyQt6.QtWidgets import QMessageBox

    save_dir = tmp_path / "save"
    save_dir.mkdir()

    window = StitchMainWindow()
    qtbot.addWidget(window)

    args = _make_job_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
    )
    _add_job_via_ui(window, args, monkeypatch)

    # Pretend it already ran.
    window.jobs[0].status = STATUS_DONE
    window._refresh_job_item(0)

    warnings = []
    monkeypatch.setattr(
        QMessageBox,
        "warning",
        staticmethod(lambda *a, **k: warnings.append((a[1], a[2]))),
    )

    window._on_run_all()

    assert len(warnings) == 1
    assert warnings[0][0] == "All Jobs Ran"


def test_quit_button_closes_window(qtbot, monkeypatch):
    """The Quit button triggers the window's close path (-> QApplication.quit)."""
    from PyQt6.QtWidgets import QApplication

    window = StitchMainWindow()
    qtbot.addWidget(window)

    assert window.quit_button.text() == "Quit"

    quit_calls = []
    monkeypatch.setattr(
        QApplication, "quit", staticmethod(lambda *a, **k: quit_calls.append(True))
    )
    window.quit_button.click()

    assert quit_calls == [True]


def test_job_buttons_have_short_labels(qtbot):
    """Edit/Remove buttons use the short labels."""
    window = StitchMainWindow()
    qtbot.addWidget(window)

    assert window.edit_button.text() == "Edit"
    assert window.remove_button.text() == "Remove"
    assert window.open_output_button.text() == "Open Output Directory"


# ---------------------------------------------------------------------------
# Sequential run integration (modal ExecutionDialog + single worker thread)
# ---------------------------------------------------------------------------


def _run_pending_via_dialog(window, qtbot, timeout=240000):
    """Run the window's pending jobs through an ExecutionDialog without blocking.

    Mirrors ``StitchMainWindow._on_run_all`` but drives the dialog with
    ``start()`` + ``qtbot.waitUntil`` instead of the blocking ``exec()``.
    """
    run_items = [
        (i, job) for i, job in enumerate(window.jobs) if job.status == STATUS_PENDING
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

    # No pending jobs remain, but Run All stays enabled (it warns on click).
    assert window.run_all_button.isEnabled() is True


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


# ---------------------------------------------------------------------------
# Stopping a running job (cooperative cancellation)
# ---------------------------------------------------------------------------


def test_run_pipeline_honors_cancellation(
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
):
    """run_pipeline raises PipelineCancelled and writes no output when cancelled."""
    from stitch.process import run_pipeline, PipelineCancelled

    save_dir = tmp_path / "save"
    save_dir.mkdir()

    args = _make_job_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="cancelled.dta",
        n_lags=2,
        parallel=False,
    )

    with pytest.raises(PipelineCancelled):
        run_pipeline(args, should_cancel=lambda: True)

    assert not (save_dir / "cancelled.dta").exists()


def test_stop_cancels_current_job_and_leaves_rest_pending(qtbot, tmp_path, monkeypatch):
    """Stop returns the running job to Pending and never starts later jobs."""
    import time
    from stitch.gui.pages import execution_page as ep

    started = []

    def fake_run_pipeline(job_args, should_cancel=None):
        started.append(job_args.output_name)
        # Block until cancellation is signalled via the cooperative flag.
        for _ in range(2000):
            if should_cancel is not None and should_cancel():
                raise ep.PipelineCancelled("cancelled")
            time.sleep(0.005)

    monkeypatch.setattr(ep, "run_pipeline", fake_run_pipeline)

    window = StitchMainWindow()
    qtbot.addWidget(window)

    for i in range(2):
        args = argparse.Namespace(
            survey_data="s",
            context_dir="c",
            save_dir=str(tmp_path),
            output_name=f"job_{i}.dta",
            n_lags=1,
        )
        window.jobs.append(Job(name=f"Job {i}", args=args, status=STATUS_PENDING))
        window._add_job_item(window.jobs[-1])
    window._refresh_buttons()

    run_items = [(i, job) for i, job in enumerate(window.jobs)]
    dialog = ExecutionDialog(run_items, window)
    dialog.job_status_changed.connect(window._on_job_status_changed)
    qtbot.addWidget(dialog)
    dialog.start()

    # Wait until the first job is actually running, then request stop.
    qtbot.waitUntil(lambda: window.jobs[0].status == STATUS_RUNNING, timeout=5000)
    dialog._on_stop_clicked()
    qtbot.waitUntil(dialog.is_finished, timeout=5000)

    # First job was stopped -> back to Pending; second job never started.
    assert started == ["job_0.dta"]
    assert dialog._cancelled == 1
    assert window.jobs[0].status == STATUS_PENDING
    assert window.jobs[1].status == STATUS_PENDING
    assert _item_bg(window, 0) == STATUS_COLORS[STATUS_PENDING][0].lower()
    assert _item_bg(window, 1) == STATUS_COLORS[STATUS_PENDING][0].lower()

    # Dialog reflects the stopped state and can be closed.
    assert dialog.close_button.isEnabled() is True
    assert dialog.stop_button.isEnabled() is False


def test_stop_midrun_then_rerun_resumes_in_gui(
    qtbot,
    fake_residential_history_file,
    survey_data_2016_2020,
    heat_index_dir,
    tmp_path,
    monkeypatch,
):
    """End-to-end GUI resume: Stop a real run mid-processing, then Run All again
    resumes into the same temp dir and only reprocesses the unfinished lags.
    """
    import tempfile
    import time
    from pathlib import Path

    import stitch.process

    # Isolate the OS temp location so we can watch this job's lag files and so
    # discovery/cleanup can't be perturbed by unrelated stitch_* dirs.
    temp_root = tmp_path / "ostemp"
    temp_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))

    save_dir = tmp_path / "save"
    save_dir.mkdir()

    # Slow down each lag write while `slow["on"]`, so the Stop click reliably
    # lands after at least one lag file exists but before the job finishes.
    slow = {"on": True}
    real_write_data = stitch.process.write_data

    def slow_write_data(*args, **kwargs):
        result = real_write_data(*args, **kwargs)
        if slow["on"]:
            time.sleep(0.2)
        return result

    monkeypatch.setattr(stitch.process, "write_data", slow_write_data)

    # Count job temp dirs created so we can prove the rerun reused (didn't recreate).
    create_calls = []
    real_create = stitch.process._create_job_temp_dir

    def spy_create(job_id=None):
        path = real_create(job_id)
        create_calls.append(path)
        return path

    monkeypatch.setattr(stitch.process, "_create_job_temp_dir", spy_create)

    window = StitchMainWindow()
    qtbot.addWidget(window)

    args = _make_job_args(
        survey_data=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        residential_hist=fake_residential_history_file,
        output_name="resumed.dta",
        n_lags=3,
        parallel=False,
    )
    _add_job_via_ui(window, args, monkeypatch)

    def partial_lag_files():
        return sorted(Path(tempfile.gettempdir()).glob("stitch_*/heat_lag_*.parquet"))

    # --- First run: start, wait for a lag file to land, then Stop mid-run. ---
    run_items = [(i, job) for i, job in enumerate(window.jobs)]
    dialog = ExecutionDialog(run_items, window)
    dialog.job_status_changed.connect(window._on_job_status_changed)
    qtbot.addWidget(dialog)
    dialog.start()

    qtbot.waitUntil(lambda: len(partial_lag_files()) >= 1, timeout=30000)
    dialog._on_stop_clicked()
    qtbot.waitUntil(dialog.is_finished, timeout=30000)
    window._refresh_buttons()

    # The job was stopped mid-run -> returned to Pending, output not yet written.
    assert dialog._cancelled == 1
    assert window.jobs[0].status == STATUS_PENDING
    assert not (save_dir / "resumed.dta").exists()

    # Exactly one temp dir was created and it survives with a partial lag set.
    assert len(create_calls) == 1
    temp_dir = create_calls[0]
    assert temp_dir.exists()
    done_before = sorted(temp_dir.glob("heat_lag_*.parquet"))
    assert 1 <= len(done_before) < args.n_lags

    # --- Second run: no artificial slowdown; Run All resumes and completes. ---
    slow["on"] = False
    creates_after_first = len(create_calls)

    dialog2 = _run_pending_via_dialog(window, qtbot)

    # The rerun succeeded, reused the same temp dir (no new one created), and the
    # completed job cleaned up its temp dir.
    assert dialog2._succeeded == 1
    assert dialog2._failed == 0
    assert window.jobs[0].status == STATUS_DONE
    assert _item_bg(window, 0) == STATUS_COLORS[STATUS_DONE][0].lower()
    assert (save_dir / "resumed.dta").exists()
    assert len(create_calls) == creates_after_first  # reused, not recreated
    assert not temp_dir.exists()
