"""
GUI tests for the configuration guards on the wizard pages.

Covers, per page: a column asked to play two roles is rejected with an inline
message and a highlight on exactly the offending widgets; fixing the choice
clears the highlight; and the output-path / previous-output guards on the
pipeline page. Also pins the two behaviors those guards rely on — column
dropdowns start with nothing selected, and a page built without a wizard still
validates.
"""

import pandas as pd
import pytest

pytest.importorskip("PyQt6")

from stitch.gui.main_window import JobConfigWizard
from stitch.gui.pages.contextual_data_page import ContextualDataPage
from stitch.gui.pages.hrs_data_page import HRSDataPage
from stitch.gui.pages.pipeline_config_page import PipelineConfigPage
from stitch.gui.pages.residential_history_page import ResidentialHistoryPage


SURVEY_COLUMNS = ["hhidpn", "iwdate", "GEOID2010", "age"]


def _populate(combo, columns, selected=None):
    combo.clear()
    combo.addItems(columns)
    combo.setCurrentIndex(-1 if selected is None else combo.findText(selected))


def _write_survey(tmp_path, name="survey.dta", extra_columns=()):
    df = pd.DataFrame(
        {
            "hhidpn": [1, 2],
            "iwdate": pd.to_datetime(["2018-01-01", "2019-06-15"]),
            "GEOID2010": ["01001020100", "01001020200"],
            "age": [61, 72],
        }
    )
    for column in extra_columns:
        df[column] = 1.0
    path = tmp_path / name
    df.to_stata(path, write_index=False)
    return path


# ---------------------------------------------------------------------------
# Survey page
# ---------------------------------------------------------------------------


def _survey_page(qtbot, tmp_path, date_col=None, id_col=None, geoid_col=None):
    """A survey page with a file loaded and the three roles chosen by hand."""
    page = HRSDataPage()
    qtbot.addWidget(page)
    page.file_picker.set_path(str(_write_survey(tmp_path)))
    for combo, column in (
        (page.date_column_combo, date_col),
        (page.id_col_combo, id_col),
        (page.geoid_col_combo, geoid_col),
    ):
        if column is not None:
            combo.setCurrentText(column)
    return page


def test_survey_page_accepts_distinct_columns(qtbot, tmp_path):
    page = _survey_page(qtbot, tmp_path, "iwdate", "hhidpn", "GEOID2010")
    assert page.validatePage() is True
    assert page.validation_label.text() == ""


def test_survey_page_rejects_id_equal_to_date(qtbot, tmp_path):
    """The reported bug, caught on the page that configures it."""
    page = _survey_page(qtbot, tmp_path, "iwdate", "iwdate", "GEOID2010")

    assert page.validatePage() is False
    assert "iwdate" in page.validation_label.text()
    assert page.date_column_combo.styleSheet() == HRSDataPage.ERROR_STYLE
    assert page.id_col_combo.styleSheet() == HRSDataPage.ERROR_STYLE
    # The column that is not part of the collision is left alone.
    assert page.geoid_col_combo.styleSheet() == ""


def test_survey_page_rejects_geoid_equal_to_id(qtbot, tmp_path):
    page = _survey_page(qtbot, tmp_path, "iwdate", "hhidpn", "hhidpn")
    assert page.validatePage() is False


def test_survey_page_highlight_clears_on_edit(qtbot, tmp_path):
    page = _survey_page(qtbot, tmp_path, "iwdate", "iwdate", "GEOID2010")
    assert page.validatePage() is False

    page.id_col_combo.setCurrentText("hhidpn")

    assert page.id_col_combo.styleSheet() == ""
    assert page.validatePage() is True


def test_survey_page_does_not_preselect_columns(qtbot, tmp_path):
    """Roles are never guessed from column names."""
    page = HRSDataPage()
    qtbot.addWidget(page)

    page._on_file_selected(str(_write_survey(tmp_path)))

    assert page.date_column_combo.count() == len(SURVEY_COLUMNS)
    for combo in page._column_combos():
        assert combo.currentIndex() == -1
        assert combo.currentText() == ""


def test_survey_page_reports_missing_selections(qtbot, tmp_path):
    """An unfinished page says what is missing rather than refusing silently."""
    page = HRSDataPage()
    qtbot.addWidget(page)
    page.file_picker.set_path(str(_write_survey(tmp_path)))

    assert page.isComplete() is True  # Next stays clickable
    assert page.validatePage() is False
    message = page.validation_label.text()
    assert message.startswith("✗ Please provide:")
    for expected in ("a date column", "an ID column", "a GEOID column"):
        assert expected in message
    for combo in page._column_combos():
        assert combo.styleSheet() == HRSDataPage.ERROR_STYLE


def test_survey_page_next_button_enabled_once_columns_are_chosen(qtbot, tmp_path):
    """Regression: Next must not stay greyed out after the columns are picked."""
    from PyQt6.QtWidgets import QWizard

    wizard = JobConfigWizard()
    qtbot.addWidget(wizard)
    wizard.show()
    page = wizard.page(JobConfigWizard.PAGE_HRS_DATA)
    page.file_picker.set_path(str(_write_survey(tmp_path)))

    next_button = wizard.button(QWizard.WizardButton.NextButton)
    assert next_button.isEnabled() is True

    page.date_column_combo.setCurrentText("iwdate")
    page.id_col_combo.setCurrentText("hhidpn")
    page.geoid_col_combo.setCurrentText("GEOID2010")

    assert next_button.isEnabled() is True
    assert page.validatePage() is True


# ---------------------------------------------------------------------------
# Residential history page
# ---------------------------------------------------------------------------


def _write_res_hist(tmp_path):
    path = tmp_path / "res_hist.dta"
    pd.DataFrame(
        {
            "hhidpn": [1, 2],
            "move_date": ["2015", "2016"],
            "GEOID": ["01001020100", "01001020200"],
        }
    ).to_stata(path, write_index=False)
    return path


def _res_hist_page(
    qtbot, tmp_path=None, *, checked, id_col=None, date_col=None, geoid_col=None
):
    page = ResidentialHistoryPage()
    qtbot.addWidget(page)
    page.use_res_hist_checkbox.setChecked(checked)
    if tmp_path is not None:
        page.file_picker.set_path(str(_write_res_hist(tmp_path)))
    for combo, column in (
        (page.id_combo, id_col),
        (page.date_combo, date_col),
        (page.geoid_combo, geoid_col),
    ):
        if column is not None:
            combo.setCurrentText(column)
    return page


def test_residential_history_page_accepts_distinct_columns(qtbot, tmp_path):
    page = _res_hist_page(
        qtbot,
        tmp_path,
        checked=True,
        id_col="hhidpn",
        date_col="move_date",
        geoid_col="GEOID",
    )
    assert page.validatePage() is True


def test_residential_history_page_reports_missing_selections(qtbot):
    """Enabled but unfinished: Next stays clickable and the page explains."""
    page = _res_hist_page(qtbot, checked=True)

    assert page.isComplete() is True
    assert page.validatePage() is False
    assert page.validation_label.text().startswith("✗ Please provide:")
    assert "a valid residential history file" in page.validation_label.text()


def test_residential_history_page_rejects_collision(qtbot, tmp_path):
    page = _res_hist_page(
        qtbot,
        tmp_path,
        checked=True,
        id_col="hhidpn",
        date_col="move_date",
        geoid_col="move_date",
    )

    assert page.validatePage() is False
    assert page.date_combo.styleSheet() == ResidentialHistoryPage.ERROR_STYLE
    assert page.geoid_combo.styleSheet() == ResidentialHistoryPage.ERROR_STYLE
    assert page.id_combo.styleSheet() == ""


def test_residential_history_page_skipped_when_unchecked(qtbot):
    """An unused residential history must never block the wizard."""
    page = _res_hist_page(
        qtbot,
        checked=False,
        id_col="move_date",
        date_col="move_date",
        geoid_col="move_date",
    )

    assert page.validatePage() is True
    assert page.validation_label.text() == ""


# ---------------------------------------------------------------------------
# Contextual page
# ---------------------------------------------------------------------------


def _contextual_page(qtbot, tmp_path, *, date_col, geoid_col, data_cols):
    ctx_file = tmp_path / "2020_heat_index.csv"
    pd.DataFrame(
        {"Date": ["2020-01-01"], "GEOID10": ["01001020100"], "index": [1.0]}
    ).to_csv(ctx_file, index=False)

    page = ContextualDataPage()
    qtbot.addWidget(page)
    page.dir_picker.set_path(str(tmp_path))
    page.measure_type_edit.setText("heat_index")
    page.file_paths = [ctx_file]
    _populate(page.date_col_combo, ["Date", "GEOID10", "index"], date_col)
    _populate(page.geoid_col_combo, ["Date", "GEOID10", "index"], geoid_col)
    page.data_col_list.clear()
    for column in data_cols:
        page.data_col_list.addItem(column)
    page._update_data_col_field()
    return page


def test_contextual_page_accepts_distinct_columns(qtbot, tmp_path):
    page = _contextual_page(
        qtbot, tmp_path, date_col="Date", geoid_col="GEOID10", data_cols=["index"]
    )
    assert page.validatePage() is True


def test_contextual_page_rejects_measure_equal_to_date(qtbot, tmp_path):
    page = _contextual_page(
        qtbot, tmp_path, date_col="Date", geoid_col="GEOID10", data_cols=["Date"]
    )

    assert page.validatePage() is False
    assert "Date" in page.validation_label.text()
    assert page.date_col_combo.styleSheet() == ContextualDataPage.ERROR_STYLE
    assert page.data_col_list.styleSheet() == ContextualDataPage.ERROR_STYLE
    assert page.geoid_col_combo.styleSheet() == ""


def test_contextual_page_missing_field_message_unchanged(qtbot, tmp_path):
    """Regression: the missing-field wording is not disturbed by the new pass."""
    page = _contextual_page(
        qtbot, tmp_path, date_col="Date", geoid_col=None, data_cols=["index"]
    )

    assert page.validatePage() is False
    assert page.validation_label.text().startswith("✗ Please provide:")
    assert "a GEOID column" in page.validation_label.text()


# ---------------------------------------------------------------------------
# Pipeline config page
# ---------------------------------------------------------------------------


def _wizard_with_survey(qtbot, survey_path, save_dir, output_name, **fields):
    wizard = JobConfigWizard()
    qtbot.addWidget(wizard)
    wizard.setField("hrs_data_path", str(survey_path))
    wizard.setField("date_col", "iwdate")
    wizard.setField("id_col", "hhidpn")
    wizard.setField("geoid_col", "GEOID2010")
    wizard.setField("data_col", "index")
    for name, value in fields.items():
        wizard.setField(name, value)

    page = wizard.page(JobConfigWizard.PAGE_PIPELINE_CONFIG)
    page.save_dir_picker.set_path(str(save_dir))
    page.output_name_edit.setText(output_name)
    page.start_lag_spin.setValue(0)
    page.end_lag_spin.setValue(1)
    # The wizard owns the page, so the caller must keep it alive.
    return wizard, page


def test_pipeline_page_rejects_output_over_survey_input(qtbot, tmp_path):
    survey = _write_survey(tmp_path)
    wizard, page = _wizard_with_survey(qtbot, survey, tmp_path, survey.name)
    page.initializePage()

    assert page.validatePage() is False
    assert "survey data file" in page.validation_label.text()
    assert page.output_name_edit.styleSheet() == PipelineConfigPage.ERROR_STYLE
    assert (
        page.save_dir_picker.path_edit.styleSheet() == PipelineConfigPage.ERROR_STYLE
    )


def test_pipeline_page_rejects_previous_output_as_survey(qtbot, tmp_path):
    survey = _write_survey(
        tmp_path, name="already_linked.dta", extra_columns=["index_iwdate_1day_prior"]
    )
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    wizard, page = _wizard_with_survey(qtbot, survey, save_dir, "linked.dta")
    page.initializePage()

    assert page.validatePage() is False
    assert "index_iwdate_1day_prior" in page.validation_label.text()


def test_pipeline_page_accepts_a_clean_configuration(qtbot, tmp_path):
    survey = _write_survey(tmp_path)
    save_dir = tmp_path / "save"
    save_dir.mkdir()
    wizard, page = _wizard_with_survey(qtbot, survey, save_dir, "linked.dta")
    page.initializePage()

    assert page.validatePage() is True
    assert page.validation_label.text() == ""


def test_pipeline_page_without_a_wizard_still_validates(qtbot, tmp_path):
    """Standalone construction (as other tests do) must not hit the wizard."""
    page = PipelineConfigPage()
    qtbot.addWidget(page)
    page.save_dir_picker.set_path(str(tmp_path))
    page.output_name_edit.setText("linked.dta")

    assert page.validatePage() is True
