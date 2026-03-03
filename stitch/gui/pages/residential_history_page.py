"""
Residential History configuration page.
"""

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
from ..validators import validate_stata_file, load_preview_data


class ResidentialHistoryPage(QWizardPage):
    """
    Wizard page for optional residential history configuration.
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

        # File selection
        file_layout = QFormLayout()
        self.file_picker = FilePicker(file_filter="Stata Files (*.dta);;All Files (*)")
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

        self.hhidpn_combo = QComboBox()
        columns_layout.addRow("ID Column (hhidpn):", self.hhidpn_combo)

        self.movecol_combo = QComboBox()
        self.movecol_combo.currentTextChanged.connect(self._on_movecol_changed)
        columns_layout.addRow("Move Indicator Column:", self.movecol_combo)

        self.mvyear_combo = QComboBox()
        columns_layout.addRow("Move Year Column:", self.mvyear_combo)

        self.mvmonth_combo = QComboBox()
        columns_layout.addRow("Move Month Column:", self.mvmonth_combo)

        self.survey_yr_combo = QComboBox()
        columns_layout.addRow(
            "Survey Year Column (Will be used if Move Year is empty):",
            self.survey_yr_combo,
        )

        self.geoid_combo = QComboBox()
        columns_layout.addRow("GEOID Column:", self.geoid_combo)

        # Dropdowns for marks (populated from move indicator column)
        self.moved_mark_combo = QComboBox()
        self.moved_mark_combo.setEditable(True)
        columns_layout.addRow("Moved Mark Value:", self.moved_mark_combo)

        self.first_tract_combo = QComboBox()
        self.first_tract_combo.setEditable(True)
        columns_layout.addRow("First Tract Mark:", self.first_tract_combo)

        res_hist_layout.addLayout(columns_layout)

        self.res_hist_widget.setLayout(res_hist_layout)
        self.res_hist_widget.setEnabled(False)
        layout.addWidget(self.res_hist_widget)

        layout.addStretch()
        self.setLayout(layout)

        # Register fields
        self.registerField("use_residential_hist", self.use_res_hist_checkbox)
        self.registerField("residential_hist_path", self.file_picker.path_edit)
        self.registerField("res_hist_hhidpn", self.hhidpn_combo, "currentText")
        self.registerField("res_hist_movecol", self.movecol_combo, "currentText")
        self.registerField("res_hist_mvyear", self.mvyear_combo, "currentText")
        self.registerField("res_hist_mvmonth", self.mvmonth_combo, "currentText")
        self.registerField(
            "res_hist_survey_yr_col", self.survey_yr_combo, "currentText"
        )
        self.registerField("res_hist_geoid", self.geoid_combo, "currentText")
        self.registerField("res_hist_moved_mark", self.moved_mark_combo, "currentText")
        self.registerField(
            "res_hist_first_tract_mark", self.first_tract_combo, "currentText"
        )

    def _on_checkbox_changed(self, state):
        """Handle checkbox state change."""
        enabled = bool(state)
        self.res_hist_widget.setEnabled(enabled)
        self.completeChanged.emit()

    def _on_file_selected(self, file_path: str):
        """Handle file selection."""
        # Validate file
        is_valid, error_msg = validate_stata_file(file_path)

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

        self.hhidpn_combo.clear()
        self.hhidpn_combo.addItems(columns)
        self._set_default_if_exists(self.hhidpn_combo, "hhidpn")

        self.movecol_combo.clear()
        self.movecol_combo.addItems(columns)
        self._set_default_if_exists(self.movecol_combo, "trmove_tr")

        self.mvyear_combo.clear()
        self.mvyear_combo.addItems(columns)
        self._set_default_if_exists(self.mvyear_combo, "mvyear")

        self.mvmonth_combo.clear()
        self.mvmonth_combo.addItems(columns)
        self._set_default_if_exists(self.mvmonth_combo, "mvmonth")

        self.survey_yr_combo.clear()
        self.survey_yr_combo.addItems(columns)
        self._set_default_if_exists(self.survey_yr_combo, "year")

        self.geoid_combo.clear()
        self.geoid_combo.addItems(columns)
        self._set_default_if_exists(self.geoid_combo, "GEOID2010")

        self.completeChanged.emit()

    def _on_movecol_changed(self, col_name: str):
        """Handle move column selection change - populate mark dropdowns."""
        if not col_name or not self.file_picker.get_path():
            return

        try:
            # Read first 1000 rows to get unique values
            file_path = self.file_picker.get_path()
            df = pd.read_stata(
                file_path, chunksize=None, iterator=False, columns=[col_name]
            )

            # Limit to first 1000 rows
            df = df.head(1000)

            # Get unique values and convert to strings
            unique_values = df[col_name].dropna().unique()
            unique_values = sorted([str(v) for v in unique_values])

            # Populate both dropdowns
            self.moved_mark_combo.clear()
            self.moved_mark_combo.addItems(unique_values)

            self.first_tract_combo.clear()
            self.first_tract_combo.addItems(unique_values)

            # Set defaults if they exist
            self._set_default_if_exists(self.moved_mark_combo, "1. move")
            self._set_default_if_exists(self.first_tract_combo, "999.0")

        except Exception as e:
            QMessageBox.warning(
                self,
                "Error Loading Values",
                f"Could not load unique values from move column: {str(e)}",
            )

    def _clear_column_combos(self):
        """Clear all column combo boxes."""
        self.hhidpn_combo.clear()
        self.movecol_combo.clear()
        self.mvyear_combo.clear()
        self.mvmonth_combo.clear()
        self.survey_yr_combo.clear()
        self.geoid_combo.clear()
        self.moved_mark_combo.clear()
        self.first_tract_combo.clear()

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
                self.hhidpn_combo.currentText(),
                self.movecol_combo.currentText(),
                self.mvyear_combo.currentText(),
                self.mvmonth_combo.currentText(),
                self.survey_yr_combo.currentText(),
                self.geoid_combo.currentText(),
            ]
        ):
            return False

        return True
