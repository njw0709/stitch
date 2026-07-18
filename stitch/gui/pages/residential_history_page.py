"""
Residential History configuration page.
"""

from pathlib import Path

import pandas as pd

from PyQt6.QtWidgets import (
    QWizardPage,
    QVBoxLayout,
    QLabel,
    QComboBox,
    QCheckBox,
    QGroupBox,
    QFormLayout,
    QMessageBox,
)

from ..widgets.file_picker import FilePicker
from ..widgets.data_preview_table import DataPreviewTable
from ...io_utils import infer_datetime_series, read_data
from ..validators import validate_data_file, load_preview_data


class ResidentialHistoryPage(QWizardPage):
    """
    Wizard page for optional residential history configuration.

    The residential history is a simple long-format table with one row per
    residence: a participant ID column, a move date column (format inferred;
    the earliest entry per person is their residence at survey entry), and a
    GEOID column.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Residential History (Optional)")
        self.setSubTitle(
            "Configure residential history if participants moved during the study period."
        )

        self.preview_df = None

        # Create layout
        layout = QVBoxLayout()

        # Use residential history checkbox
        self.use_res_hist_checkbox = QCheckBox("Use residential history data")
        self.use_res_hist_checkbox.stateChanged.connect(self._on_checkbox_changed)
        layout.addWidget(self.use_res_hist_checkbox)

        # Container for residential history options
        self.res_hist_widget = QGroupBox("Residential History Configuration")
        res_hist_layout = QVBoxLayout()

        # Description of the expected format
        format_label = QLabel(
            "Provide a table with one row per residence and three columns: a "
            "participant ID, a move date, and a GEOID. The earliest entry per "
            "person is used as their residence at survey entry. Move date "
            "formats are inferred automatically (e.g. 2013, 2013-06, "
            "March 2013, 2013-06-15)."
        )
        format_label.setWordWrap(True)
        res_hist_layout.addWidget(format_label)

        # File selection
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
        file_layout.addRow("Residential History File:", self.file_picker)

        # Add form layout to main layout
        res_hist_layout.addLayout(file_layout)

        # Preview table
        preview_label = QLabel("Data Preview (first 5 rows):")
        res_hist_layout.addWidget(preview_label)

        self.preview_table = DataPreviewTable()
        self.preview_table.setMinimumHeight(150)
        res_hist_layout.addWidget(self.preview_table)

        # Column selections
        columns_layout = QFormLayout()

        self.id_combo = QComboBox()
        columns_layout.addRow("ID Column:", self.id_combo)

        self.date_combo = QComboBox()
        self.date_combo.currentTextChanged.connect(self._on_date_col_changed)
        columns_layout.addRow("Move Date Column:", self.date_combo)

        self.geoid_combo = QComboBox()
        columns_layout.addRow("GEOID Column:", self.geoid_combo)

        res_hist_layout.addLayout(columns_layout)

        # Feedback about whether the selected date column parses
        self.date_check_label = QLabel("")
        self.date_check_label.setWordWrap(True)
        res_hist_layout.addWidget(self.date_check_label)

        self.res_hist_widget.setLayout(res_hist_layout)
        self.res_hist_widget.setEnabled(False)
        layout.addWidget(self.res_hist_widget)

        layout.addStretch()
        self.setLayout(layout)

        # Register fields
        self.registerField("use_residential_hist", self.use_res_hist_checkbox)
        self.registerField("residential_hist_path", self.file_picker.path_edit)
        self.registerField("res_hist_id_col", self.id_combo, "currentText")
        self.registerField("res_hist_date_col", self.date_combo, "currentText")
        self.registerField("res_hist_geoid_col", self.geoid_combo, "currentText")

    def _on_checkbox_changed(self, state):
        """Handle checkbox state change."""
        enabled = bool(state)
        self.res_hist_widget.setEnabled(enabled)
        self.completeChanged.emit()

    def _on_file_selected(self, file_path: str):
        """Handle file selection."""
        # Validate file
        is_valid, error_msg = validate_data_file(file_path)

        if not is_valid:
            QMessageBox.warning(self, "Invalid File", error_msg)
            self.preview_table.set_dataframe(None)
            self._clear_column_combos()
            return

        # Load preview
        preview_df, error_msg = load_preview_data(file_path, n_rows=5)

        if preview_df is None:
            QMessageBox.warning(self, "Error Loading File", error_msg)
            self.preview_table.set_dataframe(None)
            self._clear_column_combos()
            return

        # Store preview and update UI
        self.preview_df = preview_df
        self.preview_table.set_dataframe(preview_df)

        # Populate column dropdowns
        columns = preview_df.columns.tolist()

        self.id_combo.clear()
        self.id_combo.addItems(columns)
        self._set_default_if_exists(self.id_combo, "hhidpn")

        self.date_combo.clear()
        self.date_combo.addItems(columns)
        self._set_default_if_exists(self.date_combo, "move_date")

        self.geoid_combo.clear()
        self.geoid_combo.addItems(columns)
        self._set_default_if_exists(self.geoid_combo, "GEOID")

        self._on_date_col_changed(self.date_combo.currentText())
        self.completeChanged.emit()

    def _on_date_col_changed(self, col_name: str):
        """Check that the selected move-date column can be parsed as dates."""
        if not col_name or not self.file_picker.get_path():
            self.date_check_label.setText("")
            return

        try:
            file_path = self.file_picker.get_path()
            df = read_data(Path(file_path), usecols=[col_name]).head(1000)
            parsed = infer_datetime_series(df[col_name])
            total = df[col_name].notna().sum()
            parsed_ok = parsed.notna().sum()

            if total == 0:
                self.date_check_label.setText(
                    "⚠️ The selected date column has no values in the preview."
                )
            elif parsed_ok == 0:
                self.date_check_label.setText(
                    "❌ None of the values in this column could be parsed as "
                    "dates. Choose a different column or check the format."
                )
            elif parsed_ok < total:
                example = parsed[parsed.notna()].iloc[0]
                self.date_check_label.setText(
                    f"⚠️ {parsed_ok}/{total} values parsed as dates "
                    f"(e.g. {example:%Y-%m-%d}). Unparseable rows will be skipped."
                )
            else:
                example = parsed.iloc[0]
                self.date_check_label.setText(
                    f"✓ All {total} sampled values parsed as dates "
                    f"(e.g. {example:%Y-%m-%d})."
                )
        except Exception as e:
            self.date_check_label.setText(f"⚠️ Could not check date column: {e}")

    def _clear_column_combos(self):
        """Clear all column combo boxes."""
        self.id_combo.clear()
        self.date_combo.clear()
        self.geoid_combo.clear()
        self.date_check_label.setText("")

    def _set_default_if_exists(self, combo: QComboBox, default_value: str):
        """Set combo box to default value if it exists in the list."""
        index = combo.findText(default_value)
        if index >= 0:
            combo.setCurrentIndex(index)

    def isComplete(self):
        """Check if the page is complete."""
        # If not using residential history, page is complete
        if not self.use_res_hist_checkbox.isChecked():
            return True

        # If using residential history, must have valid file and all columns selected
        if not self.file_picker.get_path():
            return False
        if not self.file_picker.is_valid():
            return False

        # Check all combos have selections
        if not all(
            [
                self.id_combo.currentText(),
                self.date_combo.currentText(),
                self.geoid_combo.currentText(),
            ]
        ):
            return False

        return True
