"""
Pipeline configuration page.
"""

from PyQt6.QtWidgets import (
    QWizardPage,
    QVBoxLayout,
    QLabel,
    QCheckBox,
    QGroupBox,
    QFormLayout,
    QLineEdit,
    QSpinBox,
    QTextEdit,
)

from ..widgets.file_picker import DirectoryPicker


class PipelineConfigPage(QWizardPage):
    """
    Wizard page for pipeline execution settings.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTitle("Pipeline Configuration")
        self.setSubTitle("Configure pipeline execution settings and output options.")

        # Create layout
        layout = QVBoxLayout()

        # General settings group
        general_group = QGroupBox("General Settings")
        general_layout = QFormLayout()

        self.n_lags_spin = QSpinBox()
        self.n_lags_spin.setMinimum(1)
        self.n_lags_spin.setMaximum(10000)
        self.n_lags_spin.setValue(365)
        self.n_lags_spin.setSingleStep(1)
        general_layout.addRow("Number of Lags:", self.n_lags_spin)

        general_group.setLayout(general_layout)
        layout.addWidget(general_group)

        # Execution options group
        exec_group = QGroupBox("Execution Options")
        exec_layout = QVBoxLayout()

        self.parallel_checkbox = QCheckBox(
            "Use parallel processing (faster for large datasets)"
        )
        self.parallel_checkbox.setChecked(True)
        exec_layout.addWidget(self.parallel_checkbox)

        self.include_lag_date_checkbox = QCheckBox("Include lag date columns in output")
        exec_layout.addWidget(self.include_lag_date_checkbox)

        exec_group.setLayout(exec_layout)
        layout.addWidget(exec_group)

        # Output settings group
        output_group = QGroupBox("Output Settings")
        output_layout = QFormLayout()

        self.save_dir_picker = DirectoryPicker()
        output_layout.addRow("Save Directory:", self.save_dir_picker)

        self.output_name_edit = QLineEdit("linked_data.dta")
        output_layout.addRow("Output Filename:", self.output_name_edit)

        output_group.setLayout(output_layout)
        layout.addWidget(output_group)

        # Configuration summary
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

    def initializePage(self):
        """Called when the page is shown. Update configuration summary."""
        self._update_summary()

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
            summary_lines.append(f"Enabled: Yes")
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

        # Handle multiple data columns (comma-separated)
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
        # Must have all required fields
        if not self.save_dir_picker.get_path():
            return False
        if not self.save_dir_picker.is_valid():
            return False
        if not self.output_name_edit.text().strip():
            return False

        return True
