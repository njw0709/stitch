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
from ..validators import validate_data_file, validate_date_column, load_preview_data


class HRSDataPage(QWizardPage):
    """
    Wizard page for selecting survey data file and date column.
    """

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

        # Date column selection group
        date_group = QGroupBox("Date Column")
        date_layout = QFormLayout()

        self.date_column_combo = QComboBox()
        self.date_column_combo.currentTextChanged.connect(self._on_date_column_changed)
        date_layout.addRow("Date Column:", self.date_column_combo)

        date_group.setLayout(date_layout)
        layout.addWidget(date_group)

        # Configuration group
        config_group = QGroupBox("Data Configuration")
        config_layout = QFormLayout()

        self.id_col_combo = QComboBox()
        config_layout.addRow("ID Column:", self.id_col_combo)

        self.geoid_col_combo = QComboBox()
        config_layout.addRow("GEOID Column:", self.geoid_col_combo)

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

        layout.addStretch()
        self.setLayout(layout)

        # Register fields for wizard
        self.registerField("hrs_data_path*", self.file_picker.path_edit)
        self.registerField("date_col*", self.date_column_combo, "currentText")
        self.registerField("id_col*", self.id_col_combo, "currentText")
        self.registerField("geoid_col*", self.geoid_col_combo, "currentText")

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

        # Populate date column dropdown
        self.date_column_combo.clear()
        self.date_column_combo.addItems(columns)

        # Populate id column dropdown
        self.id_col_combo.clear()
        self.id_col_combo.addItems(columns)
        # Try to set default to "hhidpn"
        hhidpn_index = self.id_col_combo.findText("hhidpn")
        if hhidpn_index >= 0:
            self.id_col_combo.setCurrentIndex(hhidpn_index)

        # Populate geoid column dropdown
        self.geoid_col_combo.clear()
        self.geoid_col_combo.addItems(columns)
        # Try to find and set default to a column containing "GEOID" or "GEOID"
        for possible_geoid in ["GEOID2010", "GEOID", "geoid"]:
            geoid_index = self.geoid_col_combo.findText(possible_geoid)
            if geoid_index >= 0:
                self.geoid_col_combo.setCurrentIndex(geoid_index)
                break

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
