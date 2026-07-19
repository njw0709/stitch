"""
Sequential pipeline execution.

Execution is driven by a single :class:`QueueRunner` worker that runs every
queued job one after another inside one long-lived ``QThread``. The
:class:`ExecutionDialog` is a modal window that owns that thread, shows the live
log/progress, and reports each job's status back to the dashboard via
:attr:`ExecutionDialog.job_status_changed`.

Running the whole queue in a single thread (rather than one thread per job)
keeps the thread lifecycle trivial: it is created when the dialog opens and torn
down once, when the queue finishes.
"""

import contextlib
import io
import re

from PyQt6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QTextEdit,
    QProgressBar,
    QLabel,
    QMessageBox,
    QFileDialog,
)
from PyQt6.QtCore import QObject, QThread, pyqtSignal

from stitch.process import run_pipeline
from stitch.gui.job import (
    STATUS_RUNNING,
    STATUS_DONE,
    STATUS_FAILED,
)


# Regex to remove most emoji code points while preserving non-emoji Unicode
EMOJI_PATTERN = re.compile(
    "[\U0001f600-\U0001f64f"  # emoticons
    "\U0001f300-\U0001f5ff"  # symbols & pictographs
    "\U0001f680-\U0001f6ff"  # transport & map
    "\U0001f1e0-\U0001f1ff"  # flags
    "\U00002702-\U000027b0"  # dingbats
    "\U000024c2-\U0001f251"  # enclosed characters
    "\U0001f900-\U0001f9ff"  # supplemental symbols & pictographs
    "\U0001fa70-\U0001faff"  # extended-A
    "\U00002600-\U000026ff"  # misc symbols
    "\U00002300-\U000023ff"  # misc technical
    "]+",
    flags=re.UNICODE,
)


def remove_emojis(text: str) -> str:
    """Remove emoji-related code points from text.

    Keeps regular Unicode letters/numbers; strips common emoji ranges,
    zero-width joiner and variation selector.
    """
    text = text.replace("\u200d", "").replace("\ufe0f", "")
    return EMOJI_PATTERN.sub("", text)


class EmittingStream(io.StringIO):
    """Custom stream that emits a Qt signal on each non-empty write."""

    def __init__(self, signal):
        super().__init__()
        self.signal = signal

    def write(self, text):
        if text.strip():
            self.signal.emit(text.rstrip())

    def flush(self):
        pass


class QueueRunner(QObject):
    """Runs a list of jobs sequentially inside a single worker thread.

    ``run_items`` is a list of ``(row, Job)`` tuples, where ``row`` is the job's
    index in the dashboard's list (used to report status back).
    """

    job_started = pyqtSignal(int)  # row
    output = pyqtSignal(str)
    job_finished = pyqtSignal(int, bool, str)  # row, success, message
    finished = pyqtSignal()

    def __init__(self, run_items):
        super().__init__()
        self._run_items = list(run_items)

    def run(self):
        """Execute each job in order, redirecting its stdout/stderr to the log."""
        stream = EmittingStream(self.output)

        for row, job in self._run_items:
            self.job_started.emit(row)
            try:
                with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(
                    stream
                ):
                    self.output.emit("")
                    self.output.emit(f"=== {job.name} ===")
                    self.output.emit(f"HRS data: {job.args.survey_data}")
                    self.output.emit(f"Context directory: {job.args.context_dir}")
                    self.output.emit(
                        f"Output: {job.args.save_dir}/{job.args.output_name}"
                    )
                    self.output.emit(f"Number of lags: {job.args.n_lags}")
                    self.output.emit("")

                    run_pipeline(job.args)

                self.job_finished.emit(row, True, "Pipeline completed successfully!")

            except Exception as e:  # noqa: BLE001 - surface any pipeline error
                self.job_finished.emit(row, False, f"Error running pipeline: {str(e)}")

        self.finished.emit()


class ExecutionDialog(QDialog):
    """
    Modal dialog that runs all queued jobs sequentially in one worker thread.

    While open it blocks the dashboard, and reports each job's status transition
    (running/done/failed) through :attr:`job_status_changed` so the dashboard can
    recolor its list rows live.
    """

    #: Emitted with (row, status) as each job starts/finishes.
    job_status_changed = pyqtSignal(int, str)

    def __init__(self, run_items, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Running Jobs")
        self.setModal(True)
        self.resize(750, 550)

        self._run_items = list(run_items)
        self._total = len(self._run_items)
        self._completed = 0
        self._succeeded = 0
        self._failed = 0
        self._finished = False

        self.thread = None
        self.worker = None

        layout = QVBoxLayout(self)

        self.status_label = QLabel("Preparing to run...")
        layout.addWidget(self.status_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, self._total)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        layout.addWidget(QLabel("Pipeline Output:"))
        self.output_text = QTextEdit()
        self.output_text.setReadOnly(True)
        self.output_text.setFontFamily("Monospace")
        layout.addWidget(self.output_text)

        button_layout = QHBoxLayout()
        self.save_log_button = QPushButton("Save Log")
        self.save_log_button.clicked.connect(self._save_log)
        button_layout.addWidget(self.save_log_button)

        button_layout.addStretch()

        self.close_button = QPushButton("Close")
        self.close_button.clicked.connect(self.accept)
        self.close_button.setEnabled(False)
        button_layout.addWidget(self.close_button)

        layout.addLayout(button_layout)

    # ------------------------------------------------------------------
    # Execution lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Create the worker thread and begin running the queue."""
        self.status_label.setText(f"Running {self._total} job(s)...")

        self.thread = QThread(self)
        self.worker = QueueRunner(self._run_items)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.job_started.connect(self._on_job_started)
        self.worker.output.connect(self._on_output)
        self.worker.job_finished.connect(self._on_job_finished)
        self.worker.finished.connect(self._on_all_finished)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    def is_finished(self) -> bool:
        """True once the whole queue has finished running."""
        return self._finished

    def _on_job_started(self, row: int):
        self.job_status_changed.emit(row, STATUS_RUNNING)

    def _on_output(self, line: str):
        self.output_text.append(line)
        scrollbar = self.output_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _on_job_finished(self, row: int, success: bool, message: str):
        self._completed += 1
        if success:
            self._succeeded += 1
        else:
            self._failed += 1
            self.output_text.append(f"[FAILED] {message}")
        self.progress_bar.setValue(self._completed)
        self.job_status_changed.emit(row, STATUS_DONE if success else STATUS_FAILED)

    def _on_all_finished(self):
        self._finished = True
        summary = (
            f"Queue finished: {self._succeeded} succeeded, {self._failed} failed."
        )
        self.status_label.setText(summary)
        self.close_button.setEnabled(True)

    # ------------------------------------------------------------------
    # Log saving / closing
    # ------------------------------------------------------------------

    def _save_log(self):
        """Save the output log to a file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Log File",
            "pipeline_log.txt",
            "Text Files (*.txt);;All Files (*)",
        )

        if file_path:
            try:
                text = remove_emojis(self.output_text.toPlainText())
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(text)
                QMessageBox.information(self, "Saved", f"Log saved to {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to save log: {str(e)}")

    def closeEvent(self, event):
        """Block closing until the queue has finished."""
        if not self._finished:
            event.ignore()
            return
        self._shutdown_thread()
        super().closeEvent(event)

    def reject(self):
        """Ignore Escape/close attempts while the queue is still running."""
        if not self._finished:
            return
        self._shutdown_thread()
        super().reject()

    def _shutdown_thread(self, wait_ms: int = 5000):
        """Ensure the worker thread is fully stopped before the dialog closes."""
        thread = self.thread
        if thread is not None and thread.isRunning():
            thread.quit()
            thread.wait(wait_ms)
