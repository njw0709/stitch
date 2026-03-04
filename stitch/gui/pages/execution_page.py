"""
Pipeline execution page.
"""

import argparse
import sys
from pathlib import Path
import re

from PyQt6.QtWidgets import (
    QWizardPage,
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


class OutputRedirector:
    """Redirects stdout/stderr to Qt signal."""

    def __init__(self, emit_func):
        self.emit_func = emit_func

    def write(self, text):
        if text.strip():
            self.emit_func(text.rstrip())

    def flush(self):
        pass


class PipelineRunner(QThread):
    """Thread for running the pipeline function."""

    output = pyqtSignal(str)  # Emits output lines
    finished_signal = pyqtSignal(bool, str)  # success, message

    def __init__(self, args: argparse.Namespace):
        super().__init__()
        self.args = args

    def run(self):
        """Run the pipeline function."""
        # Save original stdout/stderr
        original_stdout = sys.stdout
        original_stderr = sys.stderr

        try:
            # Redirect stdout and stderr to capture print statements
            sys.stdout = OutputRedirector(self.output.emit)
            sys.stderr = OutputRedirector(self.output.emit)

            self.output.emit("Starting pipeline execution...")
            self.output.emit(f"HRS data: {self.args.survey_data}")
            self.output.emit(f"Context directory: {self.args.context_dir}")
            self.output.emit(f"Output: {self.args.save_dir}/{self.args.output_name}")
            self.output.emit(f"Number of lags: {self.args.n_lags}")
            self.output.emit(
                f"Processing mode: {'Parallel' if self.args.parallel else 'Batch'}"
            )
            self.output.emit("")

            # Call the pipeline function directly
            run_pipeline(self.args)

            self.finished_signal.emit(True, "Pipeline completed successfully!")

        except Exception as e:
            self.finished_signal.emit(False, f"Error running pipeline: {str(e)}")

        finally:
            # Restore original stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr


class ExecutionPage(QWizardPage):
    """
    Wizard page for executing the pipeline.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Run Pipeline")
        self.setSubTitle("Execute the linkage pipeline with your configured settings.")

        self.runner_thread = None
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

        # Optional: residential history
        if wizard.field("use_residential_hist"):
            args.residential_hist = wizard.field("residential_hist_path")
            args.res_hist_hhidpn = wizard.field("res_hist_hhidpn")
            args.res_hist_movecol = wizard.field("res_hist_movecol")
            args.res_hist_mvyear = wizard.field("res_hist_mvyear")
            args.res_hist_mvmonth = wizard.field("res_hist_mvmonth")
            args.res_hist_moved_mark = wizard.field("res_hist_moved_mark")
            args.res_hist_geoid = wizard.field("res_hist_geoid")
            args.res_hist_survey_yr_col = wizard.field("res_hist_survey_yr_col")
            # Convert first tract mark to float to match CLI behavior
            _first_mark = wizard.field("res_hist_first_tract_mark")
            try:
                args.res_hist_first_tract_mark = float(_first_mark)
            except (TypeError, ValueError):
                args.res_hist_first_tract_mark = _first_mark
        else:
            args.residential_hist = None

        return args

    def _run_pipeline(self):
        """Start the pipeline execution."""
        if self.pipeline_running:
            QMessageBox.warning(
                self,
                "Pipeline Running",
                "Pipeline is already running. Please wait for it to complete.",
            )
            return

        # Build arguments
        args = self._build_args()

        # Show configuration in output
        self.output_text.clear()
        self.output_text.append("=== Pipeline Configuration ===")

        # Update UI
        self.pipeline_running = True
        self.pipeline_completed = False
        self.run_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.status_label.setText("Running pipeline...")

        # Start runner thread
        self.runner_thread = PipelineRunner(args)
        self.runner_thread.output.connect(self._on_output)
        self.runner_thread.finished_signal.connect(self._on_finished)
        self.runner_thread.start()

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
