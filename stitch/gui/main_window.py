"""
Main dashboard window for the STITCH Linkage Tool.

The dashboard lets users configure multiple linkage jobs (each via the
multi-step config wizard), queue them, and run them sequentially. Execution
happens in a modal :class:`ExecutionDialog` that runs the whole queue in a
single worker thread and reports each job's status back so the list rows are
recolored live.
"""

from pathlib import Path

from PyQt6.QtWidgets import (
    QWizard,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGroupBox,
    QLabel,
    QPushButton,
    QListWidget,
    QListWidgetItem,
    QApplication,
    QMessageBox,
    QInputDialog,
)
from PyQt6.QtCore import QUrl
from PyQt6.QtGui import QDesktopServices, QColor, QBrush

from .pages.hrs_data_page import HRSDataPage
from .pages.residential_history_page import ResidentialHistoryPage
from .pages.contextual_data_page import ContextualDataPage
from .pages.pipeline_config_page import PipelineConfigPage
from .pages.execution_page import ExecutionDialog
from .job import (
    Job,
    build_args_from_wizard,
    default_job_name,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_DONE,
    STATUS_FAILED,
)


# Background/foreground tints applied to each job row based on its status.
STATUS_COLORS = {
    STATUS_PENDING: ("#f0f0f0", "#333333"),  # neutral gray
    STATUS_RUNNING: ("#fff3cd", "#856404"),  # amber - currently working
    STATUS_DONE: ("#d4edda", "#155724"),  # green - succeeded
    STATUS_FAILED: ("#f8d7da", "#721c24"),  # red - failed
}


_STYLE = """
QGroupBox {
    font-weight: bold;
    border: 1px solid #cccccc;
    border-radius: 5px;
    margin-top: 10px;
    padding-top: 10px;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 5px 0 5px;
}
QPushButton {
    padding: 5px 15px;
    border-radius: 3px;
}
QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox {
    padding: 3px;
    border: 1px solid #cccccc;
    border-radius: 3px;
}
QTableWidget {
    border: 1px solid #cccccc;
    gridline-color: #e0e0e0;
}
QTextEdit {
    border: 1px solid #cccccc;
    border-radius: 3px;
    font-family: Monospace;
}
"""


class JobConfigWizard(QWizard):
    """
    Modal wizard for configuring a single linkage job.

    Contains the four configuration pages; on Finish, the caller reads the
    configured values via :func:`build_args_from_wizard`.
    """

    PAGE_HRS_DATA = 0
    PAGE_RESIDENTIAL_HISTORY = 1
    PAGE_CONTEXTUAL_DATA = 2
    PAGE_PIPELINE_CONFIG = 3

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("New Job - STITCH Linkage Configuration")
        self.setWizardStyle(QWizard.WizardStyle.ModernStyle)
        self.setOption(QWizard.WizardOption.HaveHelpButton, False)
        self.resize(900, 700)

        self.setPage(self.PAGE_HRS_DATA, HRSDataPage(self))
        self.setPage(self.PAGE_RESIDENTIAL_HISTORY, ResidentialHistoryPage(self))
        self.setPage(self.PAGE_CONTEXTUAL_DATA, ContextualDataPage(self))
        self.setPage(self.PAGE_PIPELINE_CONFIG, PipelineConfigPage(self))

        self.setButtonText(QWizard.WizardButton.FinishButton, "Add Job")
        self.setButtonText(QWizard.WizardButton.CancelButton, "Cancel")

        self.setStyleSheet(_STYLE)

    def load_args(self, args):
        """Prefill all pages from an existing job's args (for editing)."""
        self.page(self.PAGE_HRS_DATA).load_from_args(args)
        self.page(self.PAGE_RESIDENTIAL_HISTORY).load_from_args(args)
        self.page(self.PAGE_CONTEXTUAL_DATA).load_from_args(args)
        self.page(self.PAGE_PIPELINE_CONFIG).load_from_args(args)


class StitchMainWindow(QMainWindow):
    """
    Dashboard window: add jobs, queue them, and run them sequentially.
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle(
            "STITCH: Spatio-Temporal Integration Tool for Contextual and Historical data"
        )
        self.resize(900, 600)

        self.jobs: list[Job] = []
        self._job_counter = 0

        central = QWidget()
        root_layout = QVBoxLayout(central)

        # --- New Job panel ---
        new_job_group = QGroupBox("New Job")
        new_job_layout = QHBoxLayout()

        new_job_hint = QLabel(
            "Configure a linkage job and add it to the queue. Jobs run "
            "sequentially in the order listed."
        )
        new_job_hint.setWordWrap(True)
        new_job_layout.addWidget(new_job_hint, 1)

        self.new_job_button = QPushButton("New Job")
        self.new_job_button.setStyleSheet(
            "background-color: #4CAF50; color: white; font-weight: bold;"
        )
        self.new_job_button.clicked.connect(self._on_new_job)
        new_job_layout.addWidget(self.new_job_button, 0)

        new_job_group.setLayout(new_job_layout)
        root_layout.addWidget(new_job_group)

        # --- Jobs list window ---
        jobs_group = QGroupBox("Jobs")
        jobs_layout = QVBoxLayout()

        self.jobs_list = QListWidget()
        self.jobs_list.itemDoubleClicked.connect(self._on_rename_job)
        jobs_layout.addWidget(self.jobs_list)

        jobs_buttons = QHBoxLayout()
        self.run_all_button = QPushButton("Run All")
        self.run_all_button.setStyleSheet(
            "background-color: #4CAF50; color: white; font-weight: bold;"
        )
        self.run_all_button.clicked.connect(self._on_run_all)
        jobs_buttons.addWidget(self.run_all_button)

        self.edit_button = QPushButton("Edit")
        self.edit_button.clicked.connect(self._on_edit_selected)
        jobs_buttons.addWidget(self.edit_button)

        self.remove_button = QPushButton("Remove")
        self.remove_button.clicked.connect(self._on_remove_selected)
        jobs_buttons.addWidget(self.remove_button)

        self.open_output_button = QPushButton("Open Output Directory")
        self.open_output_button.clicked.connect(self._on_open_output)
        jobs_buttons.addWidget(self.open_output_button)

        # Quit is right-aligned, separated from the job-management buttons.
        jobs_buttons.addStretch()
        self.quit_button = QPushButton("Quit")
        self.quit_button.setStyleSheet(
            "background-color: #d9534f; color: white; font-weight: bold;"
        )
        self.quit_button.clicked.connect(self.close)
        jobs_buttons.addWidget(self.quit_button)

        jobs_layout.addLayout(jobs_buttons)

        jobs_group.setLayout(jobs_layout)
        root_layout.addWidget(jobs_group, 1)

        self.setCentralWidget(central)
        self.setStyleSheet(_STYLE)

        self._refresh_buttons()

    # ------------------------------------------------------------------
    # Job management
    # ------------------------------------------------------------------

    def _on_new_job(self):
        """Open the config wizard and, on accept, add a job to the queue."""
        wizard = JobConfigWizard(self)
        if wizard.exec() != QWizard.DialogCode.Accepted:
            return

        args = build_args_from_wizard(wizard)
        self._job_counter += 1
        name = default_job_name(args, self._job_counter)
        job = Job(name=name, args=args, status=STATUS_PENDING)
        self.jobs.append(job)
        self._add_job_item(job)
        self._refresh_buttons()

    def _add_job_item(self, job: Job):
        """Append a list item representing *job*."""
        item = QListWidgetItem(self._item_text(job))
        self.jobs_list.addItem(item)
        self._style_job_item(item, job)

    def _item_text(self, job: Job) -> str:
        return f"[{job.status}] {job.name}"

    def _style_job_item(self, item: QListWidgetItem, job: Job):
        """Color a job row's background/foreground based on its status."""
        bg, fg = STATUS_COLORS.get(job.status, STATUS_COLORS[STATUS_PENDING])
        item.setBackground(QBrush(QColor(bg)))
        item.setForeground(QBrush(QColor(fg)))

    def _refresh_job_item(self, index: int):
        """Update the list item text and color for the job at *index*."""
        item = self.jobs_list.item(index)
        if item is not None:
            item.setText(self._item_text(self.jobs[index]))
            self._style_job_item(item, self.jobs[index])

    def _on_rename_job(self, item: QListWidgetItem):
        """Rename a job on double-click."""
        index = self.jobs_list.row(item)
        if index < 0 or index >= len(self.jobs):
            return
        new_name, ok = QInputDialog.getText(
            self, "Rename Job", "Job name:", text=self.jobs[index].name
        )
        if ok and new_name.strip():
            self.jobs[index].name = new_name.strip()
            self._refresh_job_item(index)

    def _on_edit_selected(self):
        """Reopen the config wizard prefilled with the selected job's settings."""
        items = self.jobs_list.selectedItems()
        if not items:
            QMessageBox.information(self, "No Job Selected", "Select a job to edit.")
            return
        index = self.jobs_list.row(items[0])
        if index < 0 or index >= len(self.jobs):
            return

        job = self.jobs[index]
        wizard = JobConfigWizard(self)
        wizard.setWindowTitle(f"Edit Job - {job.name}")
        wizard.setButtonText(QWizard.WizardButton.FinishButton, "Save Job")
        wizard.load_args(job.args)
        if wizard.exec() != QWizard.DialogCode.Accepted:
            return

        job.args = build_args_from_wizard(wizard)
        # An edited job should run again, so reset it to pending.
        job.status = STATUS_PENDING
        self._refresh_job_item(index)
        self._refresh_buttons()

    def _on_open_output(self):
        """Open the selected job's output (save) directory in the file explorer."""
        items = self.jobs_list.selectedItems()
        if not items:
            QMessageBox.information(
                self,
                "No Job Selected",
                "Select a job to open its output directory.",
            )
            return
        index = self.jobs_list.row(items[0])
        if index < 0 or index >= len(self.jobs):
            return

        save_dir = getattr(self.jobs[index].args, "save_dir", None)
        if save_dir and Path(save_dir).exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(save_dir))
        else:
            QMessageBox.warning(
                self,
                "Directory Not Found",
                f"Output directory does not exist:\n{save_dir}",
            )

    def _on_remove_selected(self):
        """Remove the selected jobs from the queue."""
        for item in self.jobs_list.selectedItems():
            index = self.jobs_list.row(item)
            self.jobs_list.takeItem(index)
            del self.jobs[index]
        self._refresh_buttons()

    # ------------------------------------------------------------------
    # Sequential execution
    # ------------------------------------------------------------------

    def _on_run_all(self):
        """Run all pending jobs sequentially in a modal execution dialog."""
        run_items = [
            (i, job)
            for i, job in enumerate(self.jobs)
            if job.status == STATUS_PENDING
        ]
        if not run_items:
            if self.jobs:
                QMessageBox.warning(
                    self,
                    "All Jobs Ran",
                    "All jobs have already been run. Edit or add a job to run again.",
                )
            else:
                QMessageBox.warning(
                    self, "No Jobs in Queue", "There are no jobs in the queue. Add a job first."
                )
            return

        dialog = ExecutionDialog(run_items, self)
        dialog.job_status_changed.connect(self._on_job_status_changed)
        dialog.start()
        dialog.exec()

        self._refresh_buttons()

    def _on_job_status_changed(self, row: int, status: str):
        """Apply a status transition reported by the execution dialog."""
        if 0 <= row < len(self.jobs):
            self.jobs[row].status = status
            self._refresh_job_item(row)

    def _refresh_buttons(self):
        """Enable/disable buttons based on current queue state."""
        has_jobs = len(self.jobs) > 0
        self.open_output_button.setEnabled(has_jobs)
        # Always clickable so "Run All" can surface a warning when there are no
        # pending jobs (all ran) or no jobs at all.
        self.run_all_button.setEnabled(True)
        self.edit_button.setEnabled(has_jobs)
        self.remove_button.setEnabled(has_jobs)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def closeEvent(self, event):
        """Close the window and quit the application."""
        super().closeEvent(event)
        QApplication.quit()
