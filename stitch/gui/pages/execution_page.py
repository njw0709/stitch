"""
Pipeline execution page.
"""

import argparse
import contextlib
import io
from pathlib import Path
import re

from PyQt6.QtWidgets import (
    QComboBox,
    QWizardPage,
    QVBoxLayout,
    QPushButton,
    QTextEdit,
    QProgressBar,
    QLabel,
    QHBoxLayout,
    QMessageBox,
    QWizard,
)
from PyQt6.QtCore import QObject, QThread, pyqtSignal, QUrl
from PyQt6.QtGui import QDesktopServices

from stitch.process import run_pipeline


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
    """Custom stream that emits signals on write."""

    def __init__(self, signal):
        super().__init__()
        self.signal = signal

    def write(self, text):
        if text.strip():
            self.signal.emit(text.rstrip())

    def flush(self):
        pass


class PipelineWorker(QObject):
    """Worker for running the pipeline in a separate thread."""

    output = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, str)

    def __init__(self, args: argparse.Namespace):
        super().__init__()
        self.args = args

    def run(self):
        """Execute the pipeline with localized stdout/stderr redirection."""
        stream = EmittingStream(self.output)

        try:
            with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
                self.output.emit("Starting pipeline execution...")
                self.output.emit(f"HRS data: {self.args.survey_data}")
                self.output.emit(f"Context directory: {self.args.context_dir}")
                self.output.emit(
                    f"Output: {self.args.save_dir}/{self.args.output_name}"
                )
                self.output.emit(f"Number of lags: {self.args.n_lags}")
                self.output.emit(
                    f"Processing mode: {'Parallel' if self.args.parallel else 'Batch'}"
                )
                self.output.emit("")

                run_pipeline(self.args)

            self.finished_signal.emit(True, "Pipeline completed successfully!")

        except Exception as e:
            self.finished_signal.emit(False, f"Error running pipeline: {str(e)}")


class ExecutionPage(QWizardPage):
    """
    Wizard page for executing the pipeline.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Run Pipeline")
        self.setSubTitle("Execute the linkage pipeline with your configured settings.")

        self.thread = None
        self.worker = None
        self.pipeline_running = False
        self.pipeline_completed = False

        # Create layout
        layout = QVBoxLayout()

        # Instructions
        instructions = QLabel(
            "Click 'Run Pipeline' to start the linkage process. "
            "This may take a while depending on the number of lags and data size."
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        # Control buttons
        button_layout = QHBoxLayout()

        self.run_button = QPushButton("Run Pipeline")
        self.run_button.setStyleSheet(
            "background-color: #4CAF50; color: white; font-weight: bold;"
        )
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
        self.progress_bar.setRange(0, 0)  # Indeterminate
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

    @staticmethod
    def _get_residential_history_page(wizard: QWizard):
        """Return the residential history wizard page, or None."""
        for i in wizard.pageIds():
            page = wizard.page(i)
            if hasattr(page, "moved_mark_combo") and hasattr(page, "first_tract_combo"):
                return page
        return None

    @staticmethod
    def _combo_actual_value(combo: QComboBox) -> object:
        """Return combo's current itemData (actual value) or currentText() if no data."""
        data = combo.currentData()
        if data is not None:
            return data
        return combo.currentText()

    def _build_args(self) -> argparse.Namespace:
        """Build pipeline arguments from wizard fields."""
        wizard = self.wizard()
        if not wizard:
            return argparse.Namespace()

        # Build arguments namespace
        args = argparse.Namespace(
            survey_data=wizard.field("hrs_data_path"),
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

        # Optional: file extension
        file_ext = wizard.field("file_extension")
        args.file_extension = file_ext if file_ext != "Auto-detect" else None

        # GEOID normalization config
        args.geoid_treatment = wizard.field("geoid_treatment") or "code"
        zero_pad = wizard.field("geoid_zero_pad")
        if zero_pad:
            args.geoid_n_digits = int(wizard.field("geoid_n_digits") or 11)
        else:
            args.geoid_n_digits = 0
        args.geoid_numeric_type = wizard.field("geoid_numeric_type") or "int"

        # Optional: residential history
        if wizard.field("use_residential_hist"):
            args.residential_hist = wizard.field("residential_hist_path")
            args.res_hist_hhidpn = wizard.field("res_hist_hhidpn")
            args.res_hist_movecol = wizard.field("res_hist_movecol")
            args.res_hist_mvyear = wizard.field("res_hist_mvyear")
            args.res_hist_mvmonth = wizard.field("res_hist_mvmonth")
            args.res_hist_geoid = wizard.field("res_hist_geoid")
            args.res_hist_survey_yr_col = wizard.field("res_hist_survey_yr_col")
            # Pass actual values (from itemData) for move/first-tract marks, not string-only
            res_hist_page = self._get_residential_history_page(wizard)
            if res_hist_page is not None:
                args.res_hist_moved_mark = self._combo_actual_value(
                    res_hist_page.moved_mark_combo
                )
                args.res_hist_first_tract_mark = self._combo_actual_value(
                    res_hist_page.first_tract_combo
                )
            else:
                args.res_hist_moved_mark = wizard.field("res_hist_moved_mark")
                args.res_hist_first_tract_mark = wizard.field(
                    "res_hist_first_tract_mark"
                )
        else:
            args.residential_hist = None

        return args

    def _run_pipeline(self):
        """Start the pipeline execution."""
        if self.pipeline_running:
            return

        # Build arguments
        args = self._build_args()

        # Show configuration in output
        self.output_text.clear()
        self.output_text.append("=== Pipeline Configuration ===")

        # Disable Run and Back while running
        self.pipeline_running = True
        self.pipeline_completed = False
        self.run_button.setEnabled(False)
        self._set_back_button_enabled(False)
        self.progress_bar.setVisible(True)
        self.status_label.setText("Running pipeline...")

        # Create thread and worker using moveToThread pattern
        self.thread = QThread()
        self.worker = PipelineWorker(args)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.output.connect(self._on_output)
        self.worker.finished_signal.connect(self._on_finished)
        self.worker.finished_signal.connect(self.thread.quit)
        self.worker.finished_signal.connect(self.worker.deleteLater)
        self.thread.finished.connect(self._on_thread_finished)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    def _set_back_button_enabled(self, enabled: bool):
        """Enable or disable the wizard Back button."""
        wizard = self.wizard()
        if wizard:
            wizard.button(QWizard.WizardButton.BackButton).setEnabled(enabled)

    def _on_output(self, line: str):
        """Handle output from pipeline."""
        self.output_text.append(line)

        # Auto-scroll to bottom
        scrollbar = self.output_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _on_finished(self, success: bool, message: str):
        """Handle pipeline completion."""
        self.pipeline_running = False
        self.pipeline_completed = success

        self.run_button.setEnabled(True)
        self._set_back_button_enabled(True)
        self.progress_bar.setVisible(False)
        self.save_log_button.setEnabled(True)

        if success:
            self.status_label.setText(f"✓ {message}")
            self.open_output_button.setEnabled(True)
            QMessageBox.information(self, "Success", message)
        else:
            self.status_label.setText(f"✗ {message}")
            QMessageBox.critical(self, "Error", message)

        self.completeChanged.emit()

    def _on_thread_finished(self):
        """Release thread/worker references only after Qt thread has stopped."""
        self.worker = None
        self.thread = None

    def stop_pipeline_thread(self, wait_ms: int = 5000) -> bool:
        """Request worker thread shutdown and wait for completion."""
        thread = self.thread
        if thread is None:
            return True

        if thread.isRunning():
            thread.quit()
            return thread.wait(wait_ms)
        return True

    def _save_log(self):
        """Save the output log to a file."""
        from PyQt6.QtWidgets import QFileDialog

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Log File",
            "pipeline_log.txt",
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
