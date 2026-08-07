"""
HRS Survey Data selection page.
"""

from pathlib import Path

from PyQt6.QtWidgets import (
    QWizardPage,
    QVBoxLayout,
    QLabel,
    QComboBox,
    QMessageBox,
    QGroupBox,
    QFormLayout,
)
from PyQt6.QtCore import Qt

from ..widgets.file_picker import FilePicker
from ..widgets.data_preview_table import DataPreviewTable
from ..validators import (
    load_preview_data,
    validate_data_file,
    validate_date_column,
    validate_survey_column_roles,
)
from ...validation import duplicate_column_values
from .field_error import FieldErrorMixin


class HRSDataPage(FieldErrorMixin, QWizardPage):
    """
    Wizard page for selecting survey data file and date column.
    """

    #: Shown in a column dropdown until the user picks a column.
    COLUMN_PLACEHOLDER = "Select a column..."

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Base Dataset")
        self.setSubTitle("Select the base dataset file and specify the date column.")

        self.preview_df = None

        # Create layout
        layout = QVBoxLayout()

        # File selection group
        file_group = QGroupBox("Survey Data File")
        file_layout = QFormLayout()

        self.file_picker = FilePicker(
            file_filter="All Supported Files (*.dta *.csv *.parquet *.pq *.feather *.xlsx *.xls);;"
            "Stata Files (*.dta);;"
            "CSV Files (*.csv);;"
            "Parquet Files (*.parquet *.pq);;"
            "Feather Files (*.feather);;"
            "Excel Files (*.xlsx *.xls);;"
            "All Files (*)"
        )
        self.file_picker.fileSelected.connect(self._on_file_selected)
        file_layout.addRow("Survey Data File:", self.file_picker)

        file_group.setLayout(file_layout)
        layout.addWidget(file_group)

        # Preview group
        preview_group = QGroupBox("Data Preview (first 5 rows)")
        preview_layout = QVBoxLayout()

        self.preview_table = DataPreviewTable()
        self.preview_table.setMinimumHeight(150)
        preview_layout.addWidget(self.preview_table)

        preview_group.setLayout(preview_layout)
        layout.addWidget(preview_group)

        # Data configuration group
        config_group = QGroupBox("Data Configuration")
        config_layout = QFormLayout()

        # Nothing is pre-selected in these combos: every column is an explicit
        # choice, so a wrong guess can never be carried into a run unnoticed.
        self.date_column_combo = QComboBox()
        self.date_column_combo.setPlaceholderText(self.COLUMN_PLACEHOLDER)
        self.date_column_combo.currentTextChanged.connect(self._on_date_column_changed)
        config_layout.addRow("Date Column:", self.date_column_combo)

        self.id_col_combo = QComboBox()
        self.id_col_combo.setPlaceholderText(self.COLUMN_PLACEHOLDER)
        config_layout.addRow("ID Column:", self.id_col_combo)

        self.geoid_col_combo = QComboBox()
        self.geoid_col_combo.setPlaceholderText(self.COLUMN_PLACEHOLDER)
        config_layout.addRow("GEOID Column:", self.geoid_col_combo)

        for combo in self._column_combos():
            combo.currentTextChanged.connect(
                lambda _text, c=combo: self._set_field_error(c, False)
            )

        config_note = QLabel(
            "Note: GEOID column will not be used if residential history is provided"
        )
        config_note.setWordWrap(True)
        config_note.setStyleSheet("color: gray; font-style: italic;")
        config_layout.addRow("", config_note)

        config_group.setLayout(config_layout)
        layout.addWidget(config_group)

        # Status label
        self.status_label = QLabel("")
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        # Validation label (problems that block Next), kept separate from the
        # file/date status above.
        self.validation_label = QLabel("")
        self.validation_label.setWordWrap(True)
        self.validation_label.setStyleSheet("color: #dc3545;")
        layout.addWidget(self.validation_label)

        layout.addStretch()
        self.setLayout(layout)

        # Register fields for wizard
        self.registerField("hrs_data_path*", self.file_picker.path_edit)
        self.registerField("date_col*", self.date_column_combo, "currentText")
        self.registerField("id_col*", self.id_col_combo, "currentText")
        self.registerField("geoid_col*", self.geoid_col_combo, "currentText")

    def _column_combos(self):
        """The three role dropdowns, in the order they appear on the page."""
        return (self.date_column_combo, self.id_col_combo, self.geoid_col_combo)

    def _on_file_selected(self, file_path: str):
        """Handle file selection."""
        # Validate file
        is_valid, error_msg = validate_data_file(file_path)

        if not is_valid:
            QMessageBox.warning(self, "Invalid File", error_msg)
            self.preview_table.set_dataframe(None)
            self.date_column_combo.clear()
            self.id_col_combo.clear()
            self.geoid_col_combo.clear()
            self.status_label.setText(f"Error: {error_msg}")
            self.preview_df = None
            return

        # Load preview
        preview_df, error_msg = load_preview_data(file_path, n_rows=5)

        if preview_df is None:
            QMessageBox.warning(self, "Error Loading File", error_msg)
            self.preview_table.set_dataframe(None)
            self.date_column_combo.clear()
            self.id_col_combo.clear()
            self.geoid_col_combo.clear()
            self.status_label.setText(f"Error: {error_msg}")
            self.preview_df = None
            return

        # Store preview and update UI
        self.preview_df = preview_df
        self.preview_table.set_dataframe(preview_df)

        columns = preview_df.columns.tolist()

        # Offer every column in every role and select none of them: guessing a
        # role from a column name is how a single column ends up serving two
        # roles without the user noticing.
        for combo in self._column_combos():
            combo.clear()
            combo.addItems(columns)
            combo.setCurrentIndex(-1)
            self._set_field_error(combo, False)
        self.validation_label.setText("")

        self.status_label.setText(
            f"Loaded successfully: {len(preview_df.columns)} columns, "
            f"{Path(file_path).name}"
        )

        # Emit completeChanged to update wizard buttons
        self.completeChanged.emit()

    def _on_date_column_changed(self, col_name: str):
        """Handle date column selection change."""
        if not col_name or self.preview_df is None:
            return

        # Validate date column
        is_valid, error_msg = validate_date_column(self.preview_df, col_name)

        if not is_valid:
            self.status_label.setText(f"Warning: {error_msg}")
        else:
            self.status_label.setText(f"Date column '{col_name}' selected.")

        self.completeChanged.emit()

    def load_from_args(self, args):
        """Restore this page's state from a previously built args namespace."""
        path = getattr(args, "survey_data", "") or ""
        if path:
            # Setting the path emits fileSelected, which loads the preview and
            # populates the column combos synchronously.
            self.file_picker.set_path(path)

        for combo, value in (
            (self.date_column_combo, getattr(args, "date_col", "")),
            (self.id_col_combo, getattr(args, "id_col", "")),
            (self.geoid_col_combo, getattr(args, "geoid_col", "")),
        ):
            index = combo.findText(value or "")
            if index >= 0:
                combo.setCurrentIndex(index)

        self.completeChanged.emit()

    def validatePage(self):
        """Reject a configuration where one column is asked to play two roles.

        The loader normalizes the date column and then the ID column, so a
        column serving as both comes back out as epoch nanoseconds and the run
        fails deep inside the lag machinery; the GEOID collisions corrupt more
        quietly still.
        """
        date_col = self.date_column_combo.currentText()
        id_col = self.id_col_combo.currentText()
        geoid_col = self.geoid_col_combo.currentText()

        is_valid, error_msg = validate_survey_column_roles(date_col, id_col, geoid_col)

        duplicates = duplicate_column_values([date_col, id_col, geoid_col])
        for combo in self._column_combos():
            self._set_field_error(combo, combo.currentText() in duplicates)

        if not is_valid:
            self.validation_label.setText(f"✗ {error_msg}")
            return False

        self.validation_label.setText("")
        return True

    def isComplete(self):
        """Check if the page is complete."""
        # Must have valid file and date column selected
        if not self.file_picker.get_path():
            return False
        if not self.file_picker.is_valid():
            return False
        if not self.date_column_combo.currentText():
            return False
        if not self.id_col_combo.currentText():
            return False
        if not self.geoid_col_combo.currentText():
            return False
        return True
