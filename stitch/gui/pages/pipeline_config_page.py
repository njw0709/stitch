"""
Pipeline configuration page.
"""

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
    QStyle,
)

from ..widgets.file_picker import DirectoryPicker
from ..validators import load_preview_data
from ...temporal import LinkageResolution


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
        # Inferred contextual resolution (set on initializePage); None until known.
        self._contextual_resolution: Optional[LinkageResolution] = None
        # Whether the resolution selector has been seeded once (fresh vs. edit).
        self._resolution_initialized = False

        layout = QVBoxLayout()

        # --- Temporal lag options group ---
        exec_group = QGroupBox("Temporal lag options")
        exec_layout = QFormLayout()

        # 1. Linkage resolution (drives lag unit and aggregation). Entries finer
        #    than the contextual data's inferred resolution are disabled in
        #    initializePage(); the default is the contextual resolution itself.
        self.resolution_combo = QComboBox()
        for res in (
            LinkageResolution.HOURLY,
            LinkageResolution.DAILY,
            LinkageResolution.MONTHLY,
        ):
            self.resolution_combo.addItem(res.label, res.value)
        # Default to Daily until a contextual resolution is known.
        self.resolution_combo.setCurrentText(LinkageResolution.DAILY.label)
        self.resolution_combo.currentTextChanged.connect(self._on_resolution_changed)
        exec_layout.addRow("Linkage resolution:", self.resolution_combo)

        # 2. Lags (interpreted in the chosen resolution unit).
        self.start_lag_spin = QSpinBox()
        self.start_lag_spin.setMinimum(0)
        self.start_lag_spin.setMaximum(10000)
        self.start_lag_spin.setValue(0)
        self.start_lag_spin.setSingleStep(1)

        self.end_lag_spin = QSpinBox()
        self.end_lag_spin.setMinimum(0)
        self.end_lag_spin.setMaximum(10000)
        self.end_lag_spin.setValue(365)
        self.end_lag_spin.setSingleStep(1)

        self.lag_count_label = QLabel()
        self.lag_count_label.setStyleSheet("color: gray; font-style: italic;")

        self.lag_unit_label = QLabel("day prior")

        lags_row = QHBoxLayout()
        lags_row.addWidget(self.start_lag_spin)
        lags_row.addWidget(QLabel("~"))
        lags_row.addWidget(self.end_lag_spin)
        lags_row.addWidget(self.lag_unit_label)
        lags_row.addWidget(self.lag_count_label)
        lags_row.addStretch()
        exec_layout.addRow("Lags:", lags_row)

        # 3. Aggregation method — shown only when the chosen resolution is
        #    coarser than the contextual data (so the data must be aggregated).
        self.agg_method_combo = QComboBox()
        self.agg_method_combo.addItem("Average", "average")
        self.agg_method_combo.addItem("Midpoint", "midpoint")
        self.agg_method_label = QLabel("Aggregation method:")
        exec_layout.addRow(self.agg_method_label, self.agg_method_combo)

        self.start_lag_spin.valueChanged.connect(self._update_lag_count_label)
        self.start_lag_spin.valueChanged.connect(self.completeChanged)
        self.end_lag_spin.valueChanged.connect(self._update_lag_count_label)
        self.end_lag_spin.valueChanged.connect(self.completeChanged)
        self._update_lag_count_label()
        self._update_agg_method_visibility()

        # Checkboxes moved to the Output Settings group below.
        self.parallel_checkbox = QCheckBox(
            "Use parallel processing (faster for large datasets)"
        )
        self.parallel_checkbox.setChecked(True)

        self.include_lag_date_checkbox = QCheckBox(
            "Include lag date and GEOID columns in output"
        )

        self.post_lag_average_checkbox = QCheckBox(
            "Post-linkage averaging: average measure across all lags"
        )

        # Small information icon whose tooltip explains the strict-NaN behavior.
        self.post_lag_average_info = QLabel()
        info_icon = self.style().standardIcon(
            QStyle.StandardPixmap.SP_MessageBoxInformation
        )
        self.post_lag_average_info.setPixmap(info_icon.pixmap(16, 16))
        self.post_lag_average_info.setToolTip(
            "Averages each measure across all lags into a single column.\n"
            "Strict handling: any participant missing a value for any lag in the "
            "range will have a missing (NaN) average.\n"
            "Incompatible with 'Include lag date and GEOID columns'."
        )

        # Post-lag averaging and include-lag-date are mutually exclusive.
        self.post_lag_average_checkbox.toggled.connect(
            self._on_post_lag_average_toggled
        )
        self.include_lag_date_checkbox.toggled.connect(
            self._on_include_lag_date_toggled
        )

        self.save_temp_checkbox = QCheckBox(
            "Save intermediate lag files to output directory (as CSV)"
        )

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

        code_hint = QLabel(
            "If checked, values are stripped to digits and left-padded with "
            "zeros to N digits."
        )
        code_hint.setWordWrap(True)
        code_hint.setStyleSheet("color: gray; font-style: italic;")
        code_opts_layout.addRow(code_hint)

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

        output_layout.addRow("", self.parallel_checkbox)
        output_layout.addRow("", self.include_lag_date_checkbox)

        post_lag_row = QHBoxLayout()
        post_lag_row.addWidget(self.post_lag_average_checkbox)
        post_lag_row.addWidget(self.post_lag_average_info)
        post_lag_row.addStretch()
        output_layout.addRow("", post_lag_row)

        output_layout.addRow("", self.save_temp_checkbox)

        output_group.setLayout(output_layout)
        layout.addWidget(output_group)

        # Validation message shown when "Add Job" is clicked with missing fields.
        self.validation_label = QLabel()
        self.validation_label.setStyleSheet("color: #dc3545;")
        self.validation_label.setWordWrap(True)
        layout.addWidget(self.validation_label)

        # Clear the error highlight as soon as the user edits a flagged field.
        self.save_dir_picker.path_edit.textChanged.connect(
            lambda: self._set_field_error(self.save_dir_picker.path_edit, False)
        )
        self.output_name_edit.textChanged.connect(
            lambda: self._set_field_error(self.output_name_edit, False)
        )
        self.start_lag_spin.valueChanged.connect(self._clear_lag_range_error)
        self.end_lag_spin.valueChanged.connect(self._clear_lag_range_error)

        layout.addStretch()
        self.setLayout(layout)

        # Register fields
        self.registerField("start_lag", self.start_lag_spin)
        self.registerField("end_lag", self.end_lag_spin)
        self.registerField("linkage_resolution", self.resolution_combo, "currentText")
        self.registerField("agg_method", self.agg_method_combo, "currentText")
        self.registerField("parallel", self.parallel_checkbox)
        self.registerField("include_lag_date", self.include_lag_date_checkbox)
        self.registerField("post_lag_average", self.post_lag_average_checkbox)
        self.registerField("save_temp_to_output", self.save_temp_checkbox)
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
        """Called when the page is shown. Load raw geoid samples."""
        self._load_raw_geoid_samples()
        self._apply_contextual_resolution_constraints()

    # ------------------------------------------------------------------
    # Linkage resolution helpers
    # ------------------------------------------------------------------

    def _current_resolution(self) -> LinkageResolution:
        data = self.resolution_combo.currentData()
        try:
            return LinkageResolution.from_str(
                data or self.resolution_combo.currentText()
            )
        except ValueError:
            return LinkageResolution.DAILY

    def _set_resolution(self, res: LinkageResolution):
        idx = self.resolution_combo.findData(res.value)
        if idx >= 0:
            self.resolution_combo.setCurrentIndex(idx)

    def _apply_contextual_resolution_constraints(self):
        """Disable resolutions finer than the contextual data and pick a default.

        The contextual data resolution (inferred on the contextual page) is the
        finest linkage resolution allowed. On first show the selector defaults
        to that resolution (exact match, no aggregation); a resolution restored
        from an edited job is preserved.
        """
        wizard = self.wizard()
        ctx_raw = wizard.field("contextual_resolution") if wizard else None
        try:
            ctx_res = LinkageResolution.from_str(ctx_raw) if ctx_raw else None
        except ValueError:
            ctx_res = None
        self._contextual_resolution = ctx_res

        model = self.resolution_combo.model()
        for i in range(self.resolution_combo.count()):
            res = LinkageResolution.from_str(self.resolution_combo.itemData(i))
            disabled = ctx_res is not None and res.is_finer_than(ctx_res)
            item = model.item(i)
            if item is not None:
                item.setEnabled(not disabled)

        if not self._resolution_initialized:
            if ctx_res is not None:
                self._set_resolution(ctx_res)
            self._resolution_initialized = True
        elif ctx_res is not None and self._current_resolution().is_finer_than(ctx_res):
            # A previously-selected resolution is now too fine; snap to coarsest-safe.
            self._set_resolution(ctx_res)

        self._update_lag_count_label()
        self._update_agg_method_visibility()

    def _on_resolution_changed(self, *args):
        self._update_lag_count_label()
        self._update_agg_method_visibility()

    def _update_agg_method_visibility(self):
        """Show the aggregation method only when coarsening the contextual data."""
        ctx_res = self._contextual_resolution
        coarser = ctx_res is not None and self._current_resolution().is_coarser_than(
            ctx_res
        )
        self.agg_method_label.setVisible(coarser)
        self.agg_method_combo.setVisible(coarser)

    # ------------------------------------------------------------------
    # GEOID sampling
    # ------------------------------------------------------------------

    def _sample_unique_geoids(self, file_path: str, col_name: str, n: int = 3) -> list:
        """Read a small preview from *file_path* and return up to *n* unique raw values.

        Returns values in their original types (float, int, str) so the preview
        normalization matches the pipeline, which receives the same types from read_data.
        """
        df, _ = load_preview_data(file_path, n_rows=100)
        if df is None or col_name not in df.columns:
            return []
        raw = df[col_name].dropna().unique()
        return list(raw[:n])

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
            res_geoid = wizard.field("res_hist_geoid_col")
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
        from ...io_utils import normalize_geoid_value_for_processing

        max_len = 0
        for samples in self._raw_samples.values():
            for val in samples:
                digits = normalize_geoid_value_for_processing(
                    val, treatment="code", n_digits=0
                )
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

    def _on_post_lag_average_toggled(self, checked: bool):
        """Averaging is incompatible with per-lag date columns; disable that option."""
        if checked:
            self.include_lag_date_checkbox.setChecked(False)
        self.include_lag_date_checkbox.setEnabled(not checked)

    def _on_include_lag_date_toggled(self, checked: bool):
        """Per-lag date columns are incompatible with averaging; disable that option."""
        self.post_lag_average_checkbox.setEnabled(not checked)

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

    def load_from_args(self, args):
        """Restore this page's state from a previously built args namespace."""
        self.start_lag_spin.setValue(int(getattr(args, "start_lag", 0) or 0))
        self.end_lag_spin.setValue(int(getattr(args, "n_lags", 366) or 366) - 1)

        # Restore linkage resolution / aggregation method and mark the selector
        # as user-seeded so initializePage does not override the restored choice.
        res_raw = getattr(args, "linkage_resolution", "daily") or "daily"
        try:
            self._set_resolution(LinkageResolution.from_str(res_raw))
        except ValueError:
            self._set_resolution(LinkageResolution.DAILY)
        self._resolution_initialized = True

        agg_raw = (getattr(args, "agg_method", "average") or "average").lower()
        agg_idx = self.agg_method_combo.findData(agg_raw)
        if agg_idx >= 0:
            self.agg_method_combo.setCurrentIndex(agg_idx)

        self.parallel_checkbox.setChecked(bool(getattr(args, "parallel", True)))
        self.include_lag_date_checkbox.setChecked(
            bool(getattr(args, "include_lag_date", False))
        )
        self.post_lag_average_checkbox.setChecked(
            bool(getattr(args, "post_lag_average", False))
        )
        self.save_temp_checkbox.setChecked(
            bool(getattr(args, "save_temp_to_output", False))
        )

        save_dir = getattr(args, "save_dir", "") or ""
        if save_dir:
            self.save_dir_picker.set_path(save_dir)
        self.output_name_edit.setText(getattr(args, "output_name", "") or "")

        # GEOID treatment
        treatment = getattr(args, "geoid_treatment", "code") or "code"
        self.treatment_combo.setCurrentIndex(0 if treatment == "code" else 1)

        n_digits = int(getattr(args, "geoid_n_digits", 11) or 0)
        if n_digits > 0:
            self.zero_pad_checkbox.setChecked(True)
            self.n_digits_spin.setValue(n_digits)
        else:
            self.zero_pad_checkbox.setChecked(False)

        numeric_type = getattr(args, "geoid_numeric_type", "int") or "int"
        self.numeric_type_combo.setCurrentIndex(0 if numeric_type == "int" else 1)

    ERROR_STYLE = "border: 2px solid #dc3545; border-radius: 3px;"

    def isComplete(self):
        """Keep the "Add Job" button interactive; validation runs in validatePage."""
        return True

    def validatePage(self):
        """Validate required fields, highlighting any that are missing/invalid."""
        problems = []

        # Lag range: start must not exceed end.
        range_ok = self.start_lag_spin.value() <= self.end_lag_spin.value()
        self._set_field_error(self.start_lag_spin, not range_ok)
        self._set_field_error(self.end_lag_spin, not range_ok)
        if not range_ok:
            problems.append("a valid lag range (start day must not exceed end day)")

        # Save directory: must be set and valid.
        save_dir_ok = bool(self.save_dir_picker.get_path()) and (
            self.save_dir_picker.is_valid()
        )
        self._set_field_error(self.save_dir_picker.path_edit, not save_dir_ok)
        if not save_dir_ok:
            problems.append("a valid save directory")

        # Output filename: must be non-empty.
        output_ok = bool(self.output_name_edit.text().strip())
        self._set_field_error(self.output_name_edit, not output_ok)
        if not output_ok:
            problems.append("an output filename")

        if problems:
            self.validation_label.setText(
                "✗ Please provide: " + ", ".join(problems) + "."
            )
            return False

        self.validation_label.setText("")
        return True

    def _set_field_error(self, widget, has_error: bool):
        """Toggle a red error border on a widget."""
        widget.setStyleSheet(self.ERROR_STYLE if has_error else "")

    def _clear_lag_range_error(self):
        """Clear the lag-range highlight once the range becomes valid again."""
        if self.start_lag_spin.value() <= self.end_lag_spin.value():
            self._set_field_error(self.start_lag_spin, False)
            self._set_field_error(self.end_lag_spin, False)

    def _update_lag_count_label(self):
        """Refresh the unit label and ``(N lags)`` helper from the spinboxes."""
        unit = self._current_resolution().lag_unit
        self.lag_unit_label.setText(f"{unit} prior")
        n = self.end_lag_spin.value() - self.start_lag_spin.value() + 1
        if n < 1:
            self.lag_count_label.setText("(invalid range)")
        else:
            self.lag_count_label.setText(f"({n} lags)")
