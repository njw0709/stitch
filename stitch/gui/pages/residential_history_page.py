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
from ..validators import (
    load_preview_data,
    validate_data_file,
    validate_residential_history_column_roles,
)
from ...validation import duplicate_column_values
from .field_error import FieldErrorMixin


class ResidentialHistoryPage(FieldErrorMixin, QWizardPage):
    """
    Wizard page for optional residential history configuration.

    The residential history is a simple long-format table with one row per
    residence: a participant ID column, a move date column (format inferred;
    the earliest entry per person is their residence at survey entry), and a
    GEOID column.
    """

    #: Shown in a column dropdown until the user picks a column.
    COLUMN_PLACEHOLDER = "Select a column..."

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

        # Description of the expected format (details on hover)
        format_label = QLabel(
            "One row per residence, with three columns: participant ID, move "
            "date, and GEOID.  ⓘ"
        )
        format_label.setWordWrap(True)
        format_label.setToolTip(
            "The earliest date per person is their residence at survey entry.\n\n"
            "Date formats are inferred automatically (e.g. 2013, 2013-06, "
            "March 2013, 2013-06-15). When a date is coarser than the linkage "
            "resolution, it is anchored to the midpoint of the period it "
            "spans. If requested linkage is daily: 2013 (year) → 2013-07-02, "
            "2013-06 (month) → 2013-06-15, 2013-06-15 (day) → 2013-06-15.\n\n"
            "To mark an exit (stop linking after a date), add a row with that "
            "date and no GEOID."
        )
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

        # Nothing is pre-selected: every column is an explicit choice, so a
        # wrong guess can never be carried into a run unnoticed.
        self.id_combo = QComboBox()
        self.id_combo.setPlaceholderText(self.COLUMN_PLACEHOLDER)
        columns_layout.addRow("ID Column:", self.id_combo)

        self.date_combo = QComboBox()
        self.date_combo.setPlaceholderText(self.COLUMN_PLACEHOLDER)
        self.date_combo.currentTextChanged.connect(self._on_date_col_changed)
        columns_layout.addRow("Move Date Column:", self.date_combo)

        self.geoid_combo = QComboBox()
        self.geoid_combo.setPlaceholderText(self.COLUMN_PLACEHOLDER)
        columns_layout.addRow("GEOID Column:", self.geoid_combo)

        for combo in self._column_combos():
            combo.currentTextChanged.connect(
                lambda _text, c=combo: self._on_column_selection_changed(c)
            )

        res_hist_layout.addLayout(columns_layout)

        # Feedback about whether the selected date column parses
        self.date_check_label = QLabel("")
        self.date_check_label.setWordWrap(True)
        res_hist_layout.addWidget(self.date_check_label)

        # Validation problems that block Next, kept separate from the
        # date-parsing feedback above.
        self.validation_label = QLabel("")
        self.validation_label.setWordWrap(True)
        self.validation_label.setStyleSheet("color: #dc3545;")
        res_hist_layout.addWidget(self.validation_label)

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

        for combo in self._column_combos():
            combo.clear()
            combo.addItems(columns)
            combo.setCurrentIndex(-1)
            self._set_field_error(combo, False)
        self.validation_label.setText("")

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

    def _column_combos(self):
        """The three role dropdowns, in the order they appear on the page."""
        return (self.id_combo, self.date_combo, self.geoid_combo)

    def _on_column_selection_changed(self, combo):
        """Clear the combo's error highlight once the user changes it."""
        self._set_field_error(combo, False)

    def _clear_column_combos(self):
        """Clear all column combo boxes."""
        for combo in self._column_combos():
            combo.clear()
            self._set_field_error(combo, False)
        self.date_check_label.setText("")
        self.validation_label.setText("")

    def _set_default_if_exists(self, combo: QComboBox, default_value: str):
        """Set combo box to default value if it exists in the list."""
        index = combo.findText(default_value)
        if index >= 0:
            combo.setCurrentIndex(index)

    def load_from_args(self, args):
        """Restore this page's state from a previously built args namespace."""
        res_path = getattr(args, "residential_hist", None)
        if res_path:
            self.use_res_hist_checkbox.setChecked(True)
            # Setting the path emits fileSelected, which loads the preview and
            # populates the column combos synchronously.
            self.file_picker.set_path(res_path)
            self._set_default_if_exists(
                self.id_combo, getattr(args, "res_hist_id_col", "") or ""
            )
            self._set_default_if_exists(
                self.date_combo, getattr(args, "res_hist_date_col", "") or ""
            )
            self._set_default_if_exists(
                self.geoid_combo, getattr(args, "res_hist_geoid_col", "") or ""
            )
        else:
            self.use_res_hist_checkbox.setChecked(False)

        self.completeChanged.emit()

    def validatePage(self):
        """Validate the page when the user leaves it.

        An unused residential history is always valid. Otherwise the required
        inputs are reported first, then the role collisions: all three columns
        are genuinely read — the ID is coerced to an integer key, the move
        dates are parsed, the GEOIDs are normalized — so any overlap either
        fails cryptically or produces silently empty linkage.
        """
        if not self.use_res_hist_checkbox.isChecked():
            self._set_field_error(self.file_picker.path_edit, False)
            for combo in self._column_combos():
                self._set_field_error(combo, False)
            self.validation_label.setText("")
            return True

        problems = []

        file_ok = bool(self.file_picker.get_path()) and self.file_picker.is_valid()
        self._set_field_error(self.file_picker.path_edit, not file_ok)
        if not file_ok:
            problems.append("a valid residential history file")

        for combo, label in (
            (self.id_combo, "an ID column"),
            (self.date_combo, "a move date column"),
            (self.geoid_combo, "a GEOID column"),
        ):
            selected = bool(combo.currentText())
            self._set_field_error(combo, not selected)
            if not selected:
                problems.append(label)

        if problems:
            self.validation_label.setText(
                "✗ Please provide: " + ", ".join(problems) + "."
            )
            return False

        id_col = self.id_combo.currentText()
        date_col = self.date_combo.currentText()
        geoid_col = self.geoid_combo.currentText()

        is_valid, error_msg = validate_residential_history_column_roles(
            id_col, date_col, geoid_col
        )

        duplicates = duplicate_column_values([id_col, date_col, geoid_col])
        for combo in self._column_combos():
            self._set_field_error(combo, combo.currentText() in duplicates)

        if not is_valid:
            self.validation_label.setText(f"✗ {error_msg}")
            return False

        self.validation_label.setText("")
        return True

    def isComplete(self):
        """Keep the Next button interactive; validation runs in validatePage."""
        return True
