"""
Pipeline configuration page.
"""

import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

from PyQt6.QtWidgets import (
    QWizardPage,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QCheckBox,
    QComboBox,
    QGroupBox,
    QFormLayout,
    QLineEdit,
    QSpinBox,
    QTextEdit,
    QPushButton,
    QApplication,
)

from ..widgets.file_picker import DirectoryPicker
from ..validators import load_preview_data


SUPPORTED_EXTENSIONS = [".csv", ".dta", ".parquet", ".pq", ".feather", ".xlsx", ".xls"]

GREEN_BUTTON_STYLE = """
    QPushButton {
        background-color: #28a745;
        color: white;
        border: none;
        padding: 5px 15px;
        border-radius: 3px;
        font-weight: bold;
    }
    QPushButton:hover { background-color: #218838; }
    QPushButton:pressed { background-color: #1e7e34; }
"""


class PipelineConfigPage(QWizardPage):
    """
    Wizard page for pipeline execution settings.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Pipeline Configuration")
        self.setSubTitle("Configure pipeline execution settings and output options.")

        self._raw_samples: dict[str, list] = {}

        layout = QVBoxLayout()

        # --- Execution options group ---
        exec_group = QGroupBox("Execution Options")
        exec_layout = QFormLayout()

        self.n_lags_spin = QSpinBox()
        self.n_lags_spin.setMinimum(1)
        self.n_lags_spin.setMaximum(10000)
        self.n_lags_spin.setValue(365)
        self.n_lags_spin.setSingleStep(1)
        exec_layout.addRow("Number of Lags:", self.n_lags_spin)

        self.parallel_checkbox = QCheckBox(
            "Use parallel processing (faster for large datasets)"
        )
        self.parallel_checkbox.setChecked(True)
        exec_layout.addRow("", self.parallel_checkbox)

        self.include_lag_date_checkbox = QCheckBox("Include lag date columns in output")
        exec_layout.addRow("", self.include_lag_date_checkbox)

        exec_group.setLayout(exec_layout)
        layout.addWidget(exec_group)

        # --- GEOID Normalization group ---
        geoid_group = QGroupBox("GEOID Normalization")
        geoid_layout = QVBoxLayout()

        # Preview window: shows raw read-in formats by default; shows normalization on Preview click
        self.preview_result_text = QTextEdit()
        self.preview_result_text.setReadOnly(True)
        self.preview_result_text.setFontFamily("Monospace")
        self.preview_result_text.setMaximumHeight(120)
        self.preview_result_text.setPlaceholderText(
            "Raw GEOID samples from data sources. Click Preview to see normalization result."
        )
        geoid_layout.addWidget(self.preview_result_text)

        # Treatment selection
        treatment_form = QFormLayout()

        self.treatment_combo = QComboBox()
        self.treatment_combo.addItems(["Treat as Code (string)", "Treat as Numeric"])
        self.treatment_combo.currentIndexChanged.connect(self._on_treatment_changed)
        treatment_form.addRow("GEOID Treatment:", self.treatment_combo)

        geoid_layout.addLayout(treatment_form)

        # Shared preview button (placed next to the active sub-option)
        self.preview_btn = QPushButton("Preview")
        self.preview_btn.setStyleSheet(GREEN_BUTTON_STYLE)
        self.preview_btn.clicked.connect(self._on_preview_clicked)

        # Code sub-options
        self.code_options_widget = QGroupBox()
        self.code_options_widget.setFlat(True)
        code_opts_layout = QFormLayout()

        self.zero_pad_checkbox = QCheckBox("Zero-pad to N digits")
        self.zero_pad_checkbox.setChecked(True)
        self.zero_pad_checkbox.stateChanged.connect(self._on_zero_pad_toggled)

        self.n_digits_spin = QSpinBox()
        self.n_digits_spin.setMinimum(1)
        self.n_digits_spin.setMaximum(20)
        self.n_digits_spin.setValue(11)

        code_row = QHBoxLayout()
        code_row.addWidget(self.zero_pad_checkbox)
        code_row.addWidget(self.n_digits_spin)
        code_row.addWidget(self.preview_btn)
        code_row.addStretch()
        code_opts_layout.addRow("", code_row)

        code_hint = QLabel(
            "If checked, values are stripped to digits and left-padded with "
            "zeros to N digits. If unchecked, digits are kept as-is (no padding)."
        )
        code_hint.setWordWrap(True)
        code_hint.setStyleSheet("color: gray; font-style: italic;")
        code_opts_layout.addRow("", code_hint)

        self.code_options_widget.setLayout(code_opts_layout)
        geoid_layout.addWidget(self.code_options_widget)

        # Numeric sub-options (hidden by default)
        self.numeric_options_widget = QGroupBox()
        self.numeric_options_widget.setFlat(True)
        num_opts_layout = QFormLayout()

        self.numeric_type_combo = QComboBox()
        self.numeric_type_combo.addItems(["Integer", "Float"])

        # Second reference to preview button — reparented when treatment changes
        self.preview_btn_numeric = QPushButton("Preview")
        self.preview_btn_numeric.setStyleSheet(GREEN_BUTTON_STYLE)
        self.preview_btn_numeric.clicked.connect(self._on_preview_clicked)

        num_row = QHBoxLayout()
        num_row.addWidget(QLabel("Cast to:"))
        num_row.addWidget(self.numeric_type_combo)
        num_row.addWidget(self.preview_btn_numeric)
        num_row.addStretch()
        num_opts_layout.addRow("", num_row)

        self.numeric_options_widget.setLayout(num_opts_layout)
        self.numeric_options_widget.setVisible(False)
        geoid_layout.addWidget(self.numeric_options_widget)

        geoid_group.setLayout(geoid_layout)
        layout.addWidget(geoid_group)

        # Hidden fields for wizard field registration
        self._geoid_treatment_edit = QLineEdit("code")
        self._geoid_treatment_edit.setVisible(False)
        self._geoid_n_digits_spin = self.n_digits_spin
        self._geoid_numeric_type_edit = QLineEdit("int")
        self._geoid_numeric_type_edit.setVisible(False)

        # Keep hidden fields in sync
        self.treatment_combo.currentIndexChanged.connect(self._sync_treatment_field)
        self.numeric_type_combo.currentIndexChanged.connect(
            self._sync_numeric_type_field
        )

        # --- Output settings group ---
        output_group = QGroupBox("Output Settings")
        output_layout = QFormLayout()

        self.save_dir_picker = DirectoryPicker()
        self.save_dir_picker.browse_btn.setStyleSheet(GREEN_BUTTON_STYLE)

        self.output_name_edit = QLineEdit("linked_data.dta")

        output_row = QHBoxLayout()
        output_row.addWidget(QLabel("Save Directory:"))
        output_row.addWidget(self.save_dir_picker.path_edit, 1)
        output_row.addWidget(self.save_dir_picker.browse_btn)
        output_row.addWidget(QLabel("Output Filename:"))
        output_row.addWidget(self.output_name_edit, 1)
        output_layout.addRow("", output_row)

        output_group.setLayout(output_layout)
        layout.addWidget(output_group)

        # --- Configuration summary ---
        summary_group = QGroupBox("Configuration Summary")
        summary_layout = QVBoxLayout()

        summary_label = QLabel("Review your configuration before running the pipeline:")
        summary_layout.addWidget(summary_label)

        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        self.summary_text.setMaximumHeight(200)
        summary_layout.addWidget(self.summary_text)

        summary_group.setLayout(summary_layout)
        layout.addWidget(summary_group)

        layout.addStretch()
        self.setLayout(layout)

        # Register fields
        self.registerField("n_lags", self.n_lags_spin)
        self.registerField("parallel", self.parallel_checkbox)
        self.registerField("include_lag_date", self.include_lag_date_checkbox)
        self.registerField("save_dir*", self.save_dir_picker.path_edit)
        self.registerField("output_name*", self.output_name_edit)
        self.registerField("geoid_treatment", self._geoid_treatment_edit)
        self.registerField("geoid_n_digits", self._geoid_n_digits_spin)
        self.registerField("geoid_numeric_type", self._geoid_numeric_type_edit)
        self.registerField("geoid_zero_pad", self.zero_pad_checkbox)

    # ------------------------------------------------------------------
    # Wizard lifecycle
    # ------------------------------------------------------------------

    def initializePage(self):
        """Called when the page is shown. Load raw geoid samples and summary."""
        self._load_raw_geoid_samples()
        self._update_summary()

    # ------------------------------------------------------------------
    # GEOID sampling
    # ------------------------------------------------------------------

    def _sample_unique_geoids(
        self, file_path: str, col_name: str, n: int = 3
    ) -> List[str]:
        """Read a small preview from *file_path* and return up to *n* unique raw values."""
        df, _ = load_preview_data(file_path, n_rows=100)
        if df is None or col_name not in df.columns:
            return []
        raw = df[col_name].dropna().unique()
        return [str(v) for v in raw[:n]]

    def _find_first_contextual_file(self) -> Optional[str]:
        """Return the path of the first matching contextual data file."""
        wizard = self.wizard()
        if not wizard:
            return None
        context_dir = wizard.field("context_dir")
        if not context_dir:
            return None

        dirpath = Path(context_dir)
        if not dirpath.is_dir():
            return None

        file_ext_text = wizard.field("file_extension") or ""
        if file_ext_text and file_ext_text != "Auto-detect":
            extensions = [file_ext_text]
        else:
            extensions = SUPPORTED_EXTENSIONS

        measure_type = wizard.field("measure_type") or ""

        for ext in extensions:
            for fp in sorted(dirpath.glob(f"*{ext}")):
                if not measure_type or measure_type in fp.name:
                    return str(fp)
        return None

    def _load_raw_geoid_samples(self):
        """Synchronously load 3 unique raw geoid values per data source."""
        wizard = self.wizard()
        if not wizard:
            return

        self._raw_samples.clear()
        lines: list[str] = []

        # HRS
        hrs_path = wizard.field("hrs_data_path")
        geoid_col = wizard.field("geoid_col")
        if hrs_path and geoid_col:
            samples = self._sample_unique_geoids(hrs_path, geoid_col)
            self._raw_samples["HRS"] = samples
            lines.append(f"Survey ({geoid_col}): {samples or '(none)'}")

        # Residential History
        if wizard.field("use_residential_hist"):
            res_path = wizard.field("residential_hist_path")
            res_geoid = wizard.field("res_hist_geoid")
            if res_path and res_geoid:
                samples = self._sample_unique_geoids(res_path, res_geoid)
                self._raw_samples["ResHist"] = samples
                lines.append(
                    f"Residential History ({res_geoid}): {samples or '(none)'}"
                )

        # Contextual
        ctx_geoid = wizard.field("contextual_geoid_col")
        ctx_file = self._find_first_contextual_file()
        if ctx_file and ctx_geoid:
            samples = self._sample_unique_geoids(ctx_file, ctx_geoid)
            self._raw_samples["Contextual"] = samples
            lines.append(f"Contextual ({ctx_geoid}): {samples or '(none)'}")

        if lines:
            self.preview_result_text.setPlainText("\n".join(lines))
        else:
            self.preview_result_text.setPlainText("No GEOID samples available.")

        # Auto-detect max digit length for the spinbox default
        self._auto_detect_n_digits()

    def _auto_detect_n_digits(self):
        """Set the N-digits spinbox to the max digit length found in raw samples."""
        max_len = 0
        for samples in self._raw_samples.values():
            for val in samples:
                digits = re.sub(r"\D", "", val)
                max_len = max(max_len, len(digits))
        if max_len > 0:
            self.n_digits_spin.setValue(max_len)

    # ------------------------------------------------------------------
    # Treatment toggle
    # ------------------------------------------------------------------

    def _on_treatment_changed(self, index: int):
        is_code = index == 0
        self.code_options_widget.setVisible(is_code)
        self.numeric_options_widget.setVisible(not is_code)

    def _on_zero_pad_toggled(self, _state: int):
        self.n_digits_spin.setEnabled(self.zero_pad_checkbox.isChecked())

    def _sync_treatment_field(self, index: int):
        self._geoid_treatment_edit.setText("code" if index == 0 else "numeric")

    def _sync_numeric_type_field(self, index: int):
        self._geoid_numeric_type_edit.setText("int" if index == 0 else "float")

    # ------------------------------------------------------------------
    # Preview normalization
    # ------------------------------------------------------------------

    def _on_preview_clicked(self):
        """Apply the chosen normalization to the raw samples and display before -> after."""
        if not self._raw_samples:
            self.preview_result_text.setPlainText("No samples loaded.")
            return

        from ...io_utils import (
            apply_geoid_normalization,
            normalize_geoid_for_processing,
        )

        treatment = "code" if self.treatment_combo.currentIndex() == 0 else "numeric"
        zero_pad = self.zero_pad_checkbox.isChecked()
        n_digits = self.n_digits_spin.value() if zero_pad else 0
        numeric_type = "int" if self.numeric_type_combo.currentIndex() == 0 else "float"

        lines: list[str] = []
        for source, raw_vals in self._raw_samples.items():
            if not raw_vals:
                continue
            series = pd.Series(raw_vals)
            processing = normalize_geoid_for_processing(
                series,
                treatment=treatment,
                n_digits=n_digits,
                numeric_type=numeric_type,
            )
            final = apply_geoid_normalization(
                series,
                treatment=treatment,
                n_digits=n_digits,
                numeric_type=numeric_type,
            )
            lines.append(f"{source}:")
            for raw, proc, fin in zip(raw_vals, processing, final):
                lines.append(f"  {raw!s:>20s}  ->  {proc!s:>15s}  (output: {fin!s})")

        self.preview_result_text.setPlainText("\n".join(lines))

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _update_summary(self):
        """Update the configuration summary text."""
        wizard = self.wizard()
        if not wizard:
            return

        summary_lines = []

        # HRS Data
        summary_lines.append("=== Survey Data ===")
        hrs_path = wizard.field("hrs_data_path")
        date_col = wizard.field("date_col")
        id_col = wizard.field("id_col")
        geoid_col = wizard.field("geoid_col")
        summary_lines.append(f"File: {hrs_path}")
        summary_lines.append(f"Date Column: {date_col}")
        summary_lines.append(f"ID Column: {id_col}")
        summary_lines.append(f"GEOID Column: {geoid_col}")
        summary_lines.append("")

        # Residential History
        use_res_hist = wizard.field("use_residential_hist")
        summary_lines.append("=== Residential History ===")
        if use_res_hist:
            res_hist_path = wizard.field("residential_hist_path")
            summary_lines.append("Enabled: Yes")
            summary_lines.append(f"File: {res_hist_path}")
            summary_lines.append(f"ID Column: {wizard.field('res_hist_hhidpn')}")
            summary_lines.append(f"Move Column: {wizard.field('res_hist_movecol')}")
        else:
            summary_lines.append("Enabled: No")
        summary_lines.append("")

        # Contextual Data
        summary_lines.append("=== Contextual Data ===")
        context_dir = wizard.field("context_dir")
        measure_type = wizard.field("measure_type")
        data_col = wizard.field("data_col")
        contextual_geoid_col = wizard.field("contextual_geoid_col")
        file_ext = wizard.field("file_extension")
        summary_lines.append(f"Directory: {context_dir}")
        summary_lines.append(f"Measure Type: {measure_type}")

        if data_col and "," in data_col:
            data_cols = [col.strip() for col in data_col.split(",")]
            summary_lines.append(f"Data Columns ({len(data_cols)}):")
            for col in data_cols:
                summary_lines.append(f"  - {col}")
        else:
            summary_lines.append(f"Data Column: {data_col}")

        summary_lines.append(f"GEOID Column: {contextual_geoid_col}")
        summary_lines.append(f"File Extension: {file_ext}")
        summary_lines.append("")

        # GEOID Normalization
        summary_lines.append("=== GEOID Normalization ===")
        if self.treatment_combo.currentIndex() == 0:
            summary_lines.append("Treatment: Code (string)")
            if self.zero_pad_checkbox.isChecked():
                summary_lines.append(
                    f"Zero-pad to {self.n_digits_spin.value()} digits"
                )
            else:
                summary_lines.append("No zero-padding (digits only)")
        else:
            summary_lines.append("Treatment: Numeric")
            summary_lines.append(f"Cast to: {self.numeric_type_combo.currentText()}")
        summary_lines.append("")

        # Pipeline Settings
        summary_lines.append("=== Pipeline Settings ===")
        summary_lines.append(f"Number of Lags: {self.n_lags_spin.value()}")
        summary_lines.append(
            f"Parallel Processing: {'Yes' if self.parallel_checkbox.isChecked() else 'No'}"
        )
        summary_lines.append(
            f"Include Lag Dates: {'Yes' if self.include_lag_date_checkbox.isChecked() else 'No'}"
        )
        summary_lines.append("")

        # Output
        summary_lines.append("=== Output ===")
        summary_lines.append(f"Save Directory: {self.save_dir_picker.get_path()}")
        summary_lines.append(f"Output Filename: {self.output_name_edit.text()}")

        self.summary_text.setText("\n".join(summary_lines))

    def isComplete(self):
        """Check if the page is complete."""
        if not self.save_dir_picker.get_path():
            return False
        if not self.save_dir_picker.is_valid():
            return False
        if not self.output_name_edit.text().strip():
            return False
        return True
