"""
Main wizard window for the HRS Linkage Tool.
"""

from PyQt6.QtWidgets import QWizard, QApplication
from PyQt6.QtCore import Qt

from .pages.hrs_data_page import HRSDataPage
from .pages.residential_history_page import ResidentialHistoryPage
from .pages.contextual_data_page import ContextualDataPage
from .pages.pipeline_config_page import PipelineConfigPage
from .pages.execution_page import ExecutionPage


class LinkageWizard(QWizard):
    """
    Main wizard for the HRS Linkage Tool.

    Guides users through configuring and running the lagged contextual
    data linkage pipeline.
    """

    # Page IDs
    PAGE_HRS_DATA = 0
    PAGE_RESIDENTIAL_HISTORY = 1
    PAGE_CONTEXTUAL_DATA = 2
    PAGE_PIPELINE_CONFIG = 3
    PAGE_EXECUTION = 4

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle(
            "STITCH: Spatio-Temporal Integration Tool for Contextual and Historical data"
        )
        self.setWizardStyle(QWizard.WizardStyle.ModernStyle)
        self.setOption(QWizard.WizardOption.HaveHelpButton, False)

        # Set window size
        self.resize(900, 700)

        # Add pages
        self.setPage(self.PAGE_HRS_DATA, HRSDataPage(self))
        self.setPage(self.PAGE_RESIDENTIAL_HISTORY, ResidentialHistoryPage(self))
        self.setPage(self.PAGE_CONTEXTUAL_DATA, ContextualDataPage(self))
        self.setPage(self.PAGE_PIPELINE_CONFIG, PipelineConfigPage(self))
        self.setPage(self.PAGE_EXECUTION, ExecutionPage(self))

        # Set button text
        self.setButtonText(QWizard.WizardButton.FinishButton, "Close")
        self.setButtonText(QWizard.WizardButton.CancelButton, "Quit")

        # Quit closes the entire application
        self.rejected.connect(QApplication.quit)

        # Apply styling
        self._apply_styles()

    def _apply_styles(self):
        """Apply custom styling to the wizard."""
        # You can customize the appearance here
        style = """
        QWizard {
            background-color: #f5f5f5;
        }
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
        self.setStyleSheet(style)
