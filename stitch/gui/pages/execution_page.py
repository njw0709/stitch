"""
Pipeline execution page.
"""

import argparse
import threading
from pathlib import Path
import re

from PyQt6.QtWidgets import (
    QWizardPage,
    QWizard,
    QVBoxLayout,
    QPushButton,
    QTextEdit,
    QProgressBar,
    QLabel,
    QHBoxLayout,
    QMessageBox,
)
from PyQt6.QtCore import QThread, pyqtSignal, QUrl
from PyQt6.QtGui import QDesktopServices

from stitch.process import run_pipeline, PipelineCancelledError


EMOJI_PATTERN = re.compile(
    "[\U0001f600-\U0001f64f"
    "\U0001f300-\U0001f5ff"
    "\U0001f680-\U0001f6ff"
    "\U0001f1e0-\U0001f1ff"
    "\U00002702-\U000027b0"
    "\U000024c2-\U0001f251"
    "\U0001f900-\U0001f9ff"
    "\U0001fa70-\U0001faff"
    "\U00002600-\U000026ff"
    "\U00002300-\U000023ff"
    "]+",
    flags=re.UNICODE,
)


def remove_emojis(text: str) -> str:
    """Remove emoji-related code points from text."""
    text = text.replace("\u200d", "").replace("\ufe0f", "")
    return EMOJI_PATTERN.sub("", text)


class PipelineRunner(QThread):
    """Thread for running the pipeline function."""

    output = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, str)  # success, message

    def __init__(self, args: argparse.Namespace):
        super().__init__()
        self.args = args
        self._cancel_event = threading.Event()

    def cancel(self):
        """Request cancellation of the running pipeline."""
        self._cancel_event.set()

    def run(self):
        """Run the pipeline, routing log output through the Qt signal."""
        try:
            self.output.emit("Starting pipeline execution...")
            self.output.emit(f"Survey data: {self.args.hrs_data}")
            self.output.emit(f"Context directory: {self.args.context_dir}")
            self.output.emit(f"Output: {self.args.save_dir}/{self.args.output_name}")
            self.output.emit(f"Number of lags: {self.args.n_lags}")
            self.output.emit(
                f"Processing mode: {'Parallel' if self.args.parallel else 'Batch'}"
            )
            self.output.emit("")

            run_pipeline(
                self.args,
                log_func=self.output.emit,
                cancel_check=self._cancel_event.is_set,
            )

            self.finished_signal.emit(True, "Pipeline completed successfully!")

        except PipelineCancelledError:
            self.output.emit("")
            self.output.emit("Pipeline was cancelled by user.")
            self.finished_signal.emit(False, "Pipeline cancelled.")

        except Exception as e:
            self.output.emit("")
            self.output.emit("=" * 50)
            self.output.emit(f"ERROR: {e}")
            self.output.emit("=" * 50)
            self.finished_signal.emit(False, f"Error running pipeline: {e}")


class ExecutionPage(QWizardPage):
    """
    Wizard page for executing the pipeline.
    """

    _RUN_STYLE = "background-color: #4CAF50; color: white; font-weight: bold;"
    _STOP_STYLE = "background-color: #d9534f; color: white; font-weight: bold;"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Run Pipeline")
        self.setSubTitle("Execute the linkage pipeline with your configured settings.")

        self.runner_thread = None
        self.pipeline_running = False
        self.pipeline_completed = False

        layout = QVBoxLayout()

        instructions = QLabel(
            "Click 'Run Pipeline' to start the linkage process. "
            "This may take a while depending on the number of lags and data size."
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        # Control buttons
        button_layout = QHBoxLayout()

        self.run_button = QPushButton("Run Pipeline")
        self.run_button.setStyleSheet(self._RUN_STYLE)
        self.run_button.clicked.connect(self._run_pipeline)
        button_layout.addWidget(self.run_button)

        self.save_log_button = QPushButton("Save Log")
        self.save_log_button.clicked.connect(self._save_log)
        self.save_log_button.setEnabled(False)
        button_layout.addWidget(self.save_log_button)

        self.open_output_button = QPushButton("Open Output Directory")
        self.open_output_button.clicked.connect(self._open_output_directory)
        self.open_output_button.setEnabled(False)
        button_layout.addWidget(self.open_output_button)

        button_layout.addStretch()
        layout.addLayout(button_layout)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        # Status label
        self.status_label = QLabel("Ready to run.")
        layout.addWidget(self.status_label)

        # Output log
        output_label = QLabel("Pipeline Output:")
        layout.addWidget(output_label)

        self.output_text = QTextEdit()
        self.output_text.setReadOnly(True)
        self.output_text.setFontFamily("Monospace")
        layout.addWidget(self.output_text)

        self.setLayout(layout)

    def _build_args(self) -> argparse.Namespace:
        """Build pipeline arguments from wizard fields."""
        wizard = self.wizard()
        if not wizard:
            return argparse.Namespace()

        args = argparse.Namespace(
            hrs_data=wizard.field("hrs_data_path"),
            context_dir=wizard.field("context_dir"),
            output_name=wizard.field("output_name"),
            id_col=wizard.field("id_col"),
            date_col=wizard.field("date_col"),
            measure_type=wizard.field("measure_type"),
            save_dir=wizard.field("save_dir"),
            data_col=wizard.field("data_col"),
            geoid_col=wizard.field("geoid_col"),
            contextual_geoid_col=wizard.field("contextual_geoid_col"),
            context_date_col=wizard.field("context_date_col"),
            n_lags=wizard.field("n_lags"),
            parallel=wizard.field("parallel"),
            include_lag_date=wizard.field("include_lag_date"),
        )

        file_ext = wizard.field("file_extension")
        args.file_extension = file_ext if file_ext != "Auto-detect" else None

        if wizard.field("use_residential_hist"):
            args.residential_hist = wizard.field("residential_hist_path")
            args.res_hist_hhidpn = wizard.field("res_hist_hhidpn")
            args.res_hist_movecol = wizard.field("res_hist_movecol")
            args.res_hist_mvyear = wizard.field("res_hist_mvyear")
            args.res_hist_mvmonth = wizard.field("res_hist_mvmonth")
            args.res_hist_moved_mark = wizard.field("res_hist_moved_mark")
            args.res_hist_geoid = wizard.field("res_hist_geoid")
            args.res_hist_survey_yr_col = wizard.field("res_hist_survey_yr_col")
            _first_mark = wizard.field("res_hist_first_tract_mark")
            try:
                args.res_hist_first_tract_mark = float(_first_mark)
            except (TypeError, ValueError):
                args.res_hist_first_tract_mark = _first_mark
        else:
            args.residential_hist = None

        return args

    # ------------------------------------------------------------------
    # Pipeline lifecycle
    # ------------------------------------------------------------------

    def _set_wizard_nav_enabled(self, enabled: bool):
        """Enable or disable the wizard Back / Cancel buttons."""
        wizard = self.wizard()
        if not wizard:
            return
        for btn_role in (
            QWizard.WizardButton.BackButton,
            QWizard.WizardButton.CancelButton,
        ):
            btn = wizard.button(btn_role)
            if btn:
                btn.setEnabled(enabled)

    def _run_pipeline(self):
        """Start the pipeline execution."""
        if self.pipeline_running:
            return

        args = self._build_args()

        self.output_text.clear()
        self.output_text.append("=== Pipeline Configuration ===")

        # Update UI state
        self.pipeline_running = True
        self.pipeline_completed = False
        self.progress_bar.setVisible(True)
        self.status_label.setText("Running pipeline...")

        # Swap button to "Stop Pipeline"
        self.run_button.setText("Stop Pipeline")
        self.run_button.setStyleSheet(self._STOP_STYLE)
        self.run_button.clicked.disconnect()
        self.run_button.clicked.connect(self._stop_pipeline)

        self._set_wizard_nav_enabled(False)

        # Start runner thread
        self.runner_thread = PipelineRunner(args)
        self.runner_thread.output.connect(self._on_output)
        self.runner_thread.finished_signal.connect(self._on_finished)
        self.runner_thread.start()

    def _stop_pipeline(self):
        """Request cancellation of the running pipeline."""
        if self.runner_thread is not None:
            self.runner_thread.cancel()
        self.run_button.setEnabled(False)
        self.status_label.setText("Cancelling pipeline...")

    def _on_output(self, line: str):
        """Handle output from pipeline."""
        self.output_text.append(line)
        scrollbar = self.output_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _on_finished(self, success: bool, message: str):
        """Handle pipeline completion."""
        self.pipeline_running = False
        self.pipeline_completed = success

        # Restore "Run Pipeline" button
        self.run_button.setText("Run Pipeline")
        self.run_button.setStyleSheet(self._RUN_STYLE)
        self.run_button.clicked.disconnect()
        self.run_button.clicked.connect(self._run_pipeline)
        self.run_button.setEnabled(True)

        self.progress_bar.setVisible(False)
        self.save_log_button.setEnabled(True)
        self._set_wizard_nav_enabled(True)

        if success:
            self.status_label.setText(f"Done - {message}")
            self.open_output_button.setEnabled(True)
            QMessageBox.information(self, "Success", message)
        else:
            self.status_label.setText(f"Stopped - {message}")
            QMessageBox.critical(self, "Error", message)

        self.completeChanged.emit()

    # ------------------------------------------------------------------
    # Utility actions
    # ------------------------------------------------------------------

    def _save_log(self):
        """Save the output log to a file."""
        from PyQt6.QtWidgets import QFileDialog

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Log File",
            "stitch_log.txt",
            "Text Files (*.txt);;All Files (*)",
        )

        if file_path:
            try:
                text = self.output_text.toPlainText()
                text = remove_emojis(text)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(text)
                QMessageBox.information(self, "Saved", f"Log saved to {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to save log: {str(e)}")

    def _open_output_directory(self):
        """Open the output directory in file explorer."""
        wizard = self.wizard()
        if not wizard:
            return

        save_dir = wizard.field("save_dir")
        if save_dir and Path(save_dir).exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(save_dir))

    def isComplete(self):
        """Page is complete when pipeline has finished successfully."""
        return self.pipeline_completed
