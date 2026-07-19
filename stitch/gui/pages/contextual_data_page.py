"""
Contextual Data Directory configuration page.
"""

from pathlib import Path
from typing import Optional

from PyQt6.QtWidgets import (
    QApplication,
    QWizardPage,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QComboBox,
    QGroupBox,
    QFormLayout,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QListWidget,
)

from ..widgets.file_picker import DirectoryPicker
from ..widgets.data_preview_table import DataPreviewTable
from ..validators import (
    validate_contextual_directory,
    check_column_consistency,
    load_preview_data,
)


class ContextualDataPage(QWizardPage):
    """
    Wizard page for configuring contextual data directory.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Contextual Data Directory")
        self.setSubTitle(
            "Select the directory containing daily contextual data files. (Must be long format)"
        )

        self.preview_df = None
        self.file_paths = []

        # Create layout
        layout = QVBoxLayout()

        # Directory selection group
        dir_group = QGroupBox("Data Directory")
        dir_layout = QFormLayout()

        self.dir_picker = DirectoryPicker()
        self.dir_picker.directorySelected.connect(self._on_directory_selected)
        self.dir_picker.path_edit.textChanged.connect(
            lambda: self._set_field_error(self.dir_picker.path_edit, False)
        )
        dir_layout.addRow("Contextual Data Directory:", self.dir_picker)

        # File extension selector
        self.file_ext_combo = QComboBox()
        self.file_ext_combo.addItems(
            ["Auto-detect", ".csv", ".parquet", ".dta", ".feather", ".xlsx"]
        )
        dir_layout.addRow("File Extension:", self.file_ext_combo)

        # File name filter input and Load preview button (same row)
        self.measure_type_edit = QLineEdit()
        self.measure_type_edit.setPlaceholderText(
            "Shared substring in the file names to select (e.g. 'pm25_')"
        )
        self.measure_type_edit.textChanged.connect(
            lambda: self._set_field_error(self.measure_type_edit, False)
        )
        self.load_preview_btn = QPushButton("Load preview")
        self.load_preview_btn.clicked.connect(self._on_load_preview_clicked)
        self.load_preview_btn.setStyleSheet(
            """
            QPushButton {
                background-color: #28a745;
                color: white;
                border: none;
                padding: 5px 15px;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #218838;
            }
            QPushButton:pressed {
                background-color: #1e7e34;
            }
            """
        )
        filter_row = QHBoxLayout()
        filter_row.addWidget(self.measure_type_edit, 1)
        filter_row.addWidget(self.load_preview_btn, 0)
        dir_layout.addRow("Common file name head:", filter_row)

        dir_group.setLayout(dir_layout)
        layout.addWidget(dir_group)

        # Validation status
        self.validation_label = QLabel("")
        self.validation_label.setWordWrap(True)
        layout.addWidget(self.validation_label)

        # Preview group
        preview_group = QGroupBox("Data Preview (first file, first 5 rows)")
        preview_layout = QVBoxLayout()

        self.preview_table = DataPreviewTable()
        self.preview_table.setMinimumHeight(150)
        preview_layout.addWidget(self.preview_table)

        preview_group.setLayout(preview_layout)
        layout.addWidget(preview_group)

        # Column selection group
        columns_group = QGroupBox("Column Selection")
        columns_layout = QFormLayout()

        # Data columns: multi-select with Add/Remove buttons
        data_col_label = QLabel("Contextual Data Columns:")
        data_col_widget = QVBoxLayout()

        # Source selector and Add button
        data_col_select_layout = QHBoxLayout()
        self.data_col_source_combo = QComboBox()
        self.data_col_add_btn = QPushButton("Add")
        self.data_col_add_btn.setStyleSheet(
            """
            QPushButton {
                background-color: #28a745;
                color: white;
                border: none;
                padding: 5px 15px;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #218838;
            }
            QPushButton:pressed {
                background-color: #1e7e34;
            }
        """
        )
        self.data_col_add_btn.clicked.connect(self._on_add_data_column)
        data_col_select_layout.addWidget(self.data_col_source_combo, 1)
        data_col_select_layout.addWidget(self.data_col_add_btn, 0)
        data_col_widget.addLayout(data_col_select_layout)

        # Selected columns list and Remove button
        data_col_list_layout = QHBoxLayout()
        self.data_col_list = QListWidget()
        self.data_col_list.setMaximumHeight(100)
        self.data_col_remove_btn = QPushButton("Remove")
        self.data_col_remove_btn.setStyleSheet(
            """
            QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                padding: 5px 15px;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
            QPushButton:pressed {
                background-color: #bd2130;
            }
        """
        )
        self.data_col_remove_btn.clicked.connect(self._on_remove_data_column)
        data_col_list_layout.addWidget(self.data_col_list, 1)
        data_col_list_layout.addWidget(self.data_col_remove_btn, 0)
        data_col_widget.addLayout(data_col_list_layout)

        columns_layout.addRow(data_col_label, data_col_widget)

        # Hidden field for storing comma-separated list
        self.data_col_hidden = QLineEdit()
        self.data_col_hidden.setVisible(False)

        self.geoid_col_combo = QComboBox()
        self.geoid_col_combo.currentTextChanged.connect(
            lambda: self._set_field_error(self.geoid_col_combo, False)
        )
        columns_layout.addRow("GEOID Column:", self.geoid_col_combo)

        self.date_col_combo = QComboBox()
        self.date_col_combo.currentTextChanged.connect(
            lambda: self._set_field_error(self.date_col_combo, False)
        )
        columns_layout.addRow("Date Column:", self.date_col_combo)

        columns_group.setLayout(columns_layout)
        layout.addWidget(columns_group)

        layout.addStretch()
        self.setLayout(layout)

        # Register fields (validation handled in validatePage so the Next
        # button stays interactive and missing fields can be highlighted)
        self.registerField("context_dir", self.dir_picker.path_edit)
        self.registerField("measure_type", self.measure_type_edit)
        self.registerField("data_col", self.data_col_hidden)
        self.registerField("contextual_geoid_col", self.geoid_col_combo, "currentText")
        self.registerField("context_date_col", self.date_col_combo, "currentText")
        self.registerField("file_extension", self.file_ext_combo, "currentText")

    def _on_directory_selected(self, dir_path: str):
        """Handle directory selection."""
        self.validation_label.setText("Directory set; click Load preview to validate.")

    def _on_load_preview_clicked(self):
        """Validate directory and load preview synchronously."""
        dir_path = self.dir_picker.get_path()
        if not dir_path:
            self.validation_label.setText("Select a directory first.")
            return

        measure_type = self.measure_type_edit.text().strip() or None
        file_ext_text = self.file_ext_combo.currentText()
        file_extension = None if file_ext_text == "Auto-detect" else file_ext_text

        self.validation_label.setText("Loading...")
        QApplication.processEvents()

        try:
            is_valid, years, error_msg = validate_contextual_directory(
                dir_path, measure_type, file_extension
            )
            if not is_valid:
                self._clear_preview_state()
                self.validation_label.setText(f"✗ {error_msg}")
                self.completeChanged.emit()
                return

            dirpath = Path(dir_path)
            if file_extension is None:
                supported_extensions = [
                    ".csv",
                    ".dta",
                    ".parquet",
                    ".pq",
                    ".feather",
                    ".xlsx",
                    ".xls",
                ]
            else:
                supported_extensions = [file_extension]

            all_files = []
            for ext in supported_extensions:
                all_files.extend(dirpath.glob(f"*{ext}"))

            if measure_type is not None:
                file_paths = [f for f in all_files if measure_type in f.name]
            else:
                file_paths = all_files

            if not file_paths:
                self._clear_preview_state()
                self.validation_label.setText(
                    "✗ No matching files found for the given directory and filter."
                )
                self.completeChanged.emit()
                return

            is_valid, error_msg = check_column_consistency(file_paths)
            if not is_valid:
                self._clear_preview_state()
                self.validation_label.setText(f"✗ {error_msg}")
                self.completeChanged.emit()
                return

            self.validation_label.setText(
                f"✓ Found {len(file_paths)} valid files for years: {', '.join(years)}"
            )
            self.file_paths = file_paths
            self._load_preview(file_paths[0])

        except Exception as e:
            self._clear_preview_state()
            self.validation_label.setText(f"✗ Validation error: {str(e)}")

        self.completeChanged.emit()

    def _clear_preview_state(self):
        """Clear preview table and column selection state."""
        self.preview_table.set_dataframe(None)
        self.data_col_source_combo.clear()
        self.data_col_list.clear()
        self.data_col_hidden.clear()
        self.geoid_col_combo.clear()
        self.date_col_combo.clear()
        self.file_paths = []

    def _on_add_data_column(self):
        """Add selected column to the data columns list."""
        selected_col = self.data_col_source_combo.currentText()
        if not selected_col:
            return

        # Check if already in list
        for i in range(self.data_col_list.count()):
            if self.data_col_list.item(i).text() == selected_col:
                return  # Already added

        # Add to list
        self.data_col_list.addItem(selected_col)
        self._set_field_error(self.data_col_list, False)
        self._update_data_col_field()
        self.completeChanged.emit()

    def _on_remove_data_column(self):
        """Remove selected column(s) from the data columns list."""
        selected_items = self.data_col_list.selectedItems()
        if not selected_items:
            return

        for item in selected_items:
            self.data_col_list.takeItem(self.data_col_list.row(item))

        self._update_data_col_field()
        self.completeChanged.emit()

    def _update_data_col_field(self):
        """Update the hidden field with comma-separated list of data columns."""
        columns = []
        for i in range(self.data_col_list.count()):
            columns.append(self.data_col_list.item(i).text())
        self.data_col_hidden.setText(",".join(columns))

    def _load_preview(self, file_path: Path):
        """Load preview of a data file."""
        preview_df, error_msg = load_preview_data(str(file_path), n_rows=5)

        if preview_df is None:
            QMessageBox.warning(self, "Error Loading Preview", error_msg)
            return

        self.preview_df = preview_df
        self.preview_table.set_dataframe(preview_df)

        # Populate column dropdowns
        columns = preview_df.columns.tolist()

        self.data_col_source_combo.clear()
        self.data_col_source_combo.addItems(columns)

        self.geoid_col_combo.clear()
        self.geoid_col_combo.addItems(columns)

        self.date_col_combo.clear()
        self.date_col_combo.addItems(columns)

        # Try to set defaults
        self._set_default_if_exists(self.geoid_col_combo, "GEOID10")
        self._set_default_if_exists(self.date_col_combo, "Date")

        self.completeChanged.emit()

    def _set_default_if_exists(self, combo: QComboBox, default_value: str):
        """Set combo box to default value if it exists in the list."""
        index = combo.findText(default_value)
        if index >= 0:
            combo.setCurrentIndex(index)

    ERROR_STYLE = "border: 2px solid #dc3545; border-radius: 3px;"

    def _set_field_error(self, widget, has_error: bool):
        """Toggle a red error border on a widget."""
        widget.setStyleSheet(self.ERROR_STYLE if has_error else "")

    def load_from_args(self, args):
        """Restore this page's state from a previously built args namespace."""
        measure = getattr(args, "measure_type", "") or ""
        self.measure_type_edit.setText(measure)

        # File extension: args stores None for "Auto-detect"
        file_ext = getattr(args, "file_extension", None)
        ext_text = file_ext if file_ext else "Auto-detect"
        ext_index = self.file_ext_combo.findText(ext_text)
        if ext_index >= 0:
            self.file_ext_combo.setCurrentIndex(ext_index)

        ctx_dir = getattr(args, "context_dir", "") or ""
        if ctx_dir:
            self.dir_picker.set_path(ctx_dir)
            # Validate and populate the column combos synchronously.
            self._on_load_preview_clicked()

        # Restore multi-select data columns from the comma-separated value.
        data_col = getattr(args, "data_col", "") or ""
        self.data_col_list.clear()
        for col in [c.strip() for c in data_col.split(",") if c.strip()]:
            self.data_col_list.addItem(col)
        self._update_data_col_field()

        self._set_default_if_exists(
            self.geoid_col_combo, getattr(args, "contextual_geoid_col", "") or ""
        )
        self._set_default_if_exists(
            self.date_col_combo, getattr(args, "context_date_col", "") or ""
        )

        self.completeChanged.emit()

    def isComplete(self):
        """Keep the Next button interactive; validation runs in validatePage."""
        return True

    def validatePage(self):
        """Validate required fields, highlighting any that are missing."""
        # If the preview was never loaded but we have enough to try, auto-load
        # it so the user doesn't have to press 'Load preview' separately.
        if (
            not self.file_paths
            and self.dir_picker.get_path()
            and self.dir_picker.is_valid()
            and self.measure_type_edit.text().strip()
        ):
            self._on_load_preview_clicked()

        problems = []

        # Directory
        dir_ok = bool(self.dir_picker.get_path()) and self.dir_picker.is_valid()
        self._set_field_error(self.dir_picker.path_edit, not dir_ok)
        if not dir_ok:
            problems.append("a valid data directory")

        # Common file name head
        measure_ok = bool(self.measure_type_edit.text().strip())
        self._set_field_error(self.measure_type_edit, not measure_ok)
        if not measure_ok:
            problems.append("a common file name head")

        # Column selections (populated only after a successful preview load)
        data_col_ok = self.data_col_list.count() > 0
        self._set_field_error(self.data_col_list, not data_col_ok)
        if not data_col_ok:
            problems.append("at least one contextual data column")

        geoid_ok = bool(self.geoid_col_combo.currentText())
        self._set_field_error(self.geoid_col_combo, not geoid_ok)
        if not geoid_ok:
            problems.append("a GEOID column")

        date_ok = bool(self.date_col_combo.currentText())
        self._set_field_error(self.date_col_combo, not date_ok)
        if not date_ok:
            problems.append("a date column")

        # Files must have been validated via Load preview
        files_ok = bool(self.file_paths)

        if problems or not files_ok:
            if not files_ok and not problems:
                message = (
                    "Click 'Load preview' to validate the files before continuing."
                )
            else:
                message = "Please provide: " + ", ".join(problems) + "."
                if not files_ok:
                    message += " Then click 'Load preview' to validate the files."
            self.validation_label.setText(f"✗ {message}")
            return False

        return True
