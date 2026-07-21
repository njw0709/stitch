"""
GUI tests for the linkage-resolution controls.

Covers:
- Contextual page inferring and exposing the data resolution.
- Pipeline config page: default resolution, disabling finer-than-contextual
  options, unit-label switching, aggregation-method visibility, and mapping
  through ``build_args_from_wizard``.
"""

import pandas as pd
import pytest

pytest.importorskip("PyQt6")

from stitch.gui.main_window import JobConfigWizard
from stitch.gui.pages.contextual_data_page import ContextualDataPage
from stitch.gui.job import build_args_from_wizard
from stitch.temporal import AggMethod, LinkageResolution


GEOIDS = ["01001020100", "01001020200"]


def _pipeline_page(wizard):
    return wizard.page(JobConfigWizard.PAGE_PIPELINE_CONFIG)


def _combo_item_enabled(combo, value):
    idx = combo.findData(value)
    return combo.model().item(idx).isEnabled()


# ---------------------------------------------------------------------------
# Contextual page resolution inference
# ---------------------------------------------------------------------------


def _write_monthly_dir(tmp_path):
    for month in (1, 2, 3):
        ts = pd.date_range(f"2020-{month:02d}-01", periods=1, freq="MS")
        df = pd.DataFrame(
            {"Date": list(ts) * len(GEOIDS), "GEOID10": GEOIDS, "index": 1.0}
        )
        df.to_csv(tmp_path / f"2020_{month:02d}_heat_index.csv", index=False)
    return tmp_path


def test_contextual_page_infers_monthly(qtbot, tmp_path):
    ctx_dir = _write_monthly_dir(tmp_path)
    page = ContextualDataPage()
    qtbot.addWidget(page)

    page.file_paths = sorted(ctx_dir.glob("*.csv"))
    page.date_col_combo.addItem("Date")
    page.date_col_combo.setCurrentText("Date")
    page._update_inferred_resolution()

    assert page.resolution_field.text() == "monthly"
    assert "Monthly" in page.resolution_label.text()


def test_contextual_page_infers_daily(qtbot, tmp_path):
    ts = pd.date_range("2020-01-01", periods=20, freq="D")
    df = pd.DataFrame(
        {
            "Date": [d for d in ts for _ in GEOIDS],
            "GEOID10": GEOIDS * len(ts),
            "index": 1.0,
        }
    )
    df.to_csv(tmp_path / "2020_daily_heat_index.csv", index=False)

    page = ContextualDataPage()
    qtbot.addWidget(page)
    page.file_paths = sorted(tmp_path.glob("*.csv"))
    page.date_col_combo.addItem("Date")
    page.date_col_combo.setCurrentText("Date")
    page._update_inferred_resolution()

    assert page.resolution_field.text() == "daily"


# ---------------------------------------------------------------------------
# Pipeline config resolution selector
# ---------------------------------------------------------------------------


def test_default_resolution_matches_contextual(qtbot):
    wizard = JobConfigWizard()
    qtbot.addWidget(wizard)
    wizard.setField("contextual_resolution", "monthly")

    page = _pipeline_page(wizard)
    page.initializePage()

    assert page.resolution_combo.currentData() == "monthly"
    # Finer options disabled.
    assert not _combo_item_enabled(page.resolution_combo, "daily")
    assert not _combo_item_enabled(page.resolution_combo, "hourly")
    assert _combo_item_enabled(page.resolution_combo, "monthly")
    # Equal resolution -> no aggregation needed (widget explicitly hidden).
    assert page.agg_method_combo.isHidden()
    assert page.lag_unit_label.text() == "month prior"


def test_daily_contextual_allows_coarser_and_shows_agg(qtbot):
    wizard = JobConfigWizard()
    qtbot.addWidget(wizard)
    wizard.setField("contextual_resolution", "daily")

    page = _pipeline_page(wizard)
    page.initializePage()

    # Default is daily; hourly disabled, daily/monthly enabled.
    assert page.resolution_combo.currentData() == "daily"
    assert not _combo_item_enabled(page.resolution_combo, "hourly")
    assert _combo_item_enabled(page.resolution_combo, "monthly")
    assert page.agg_method_combo.isHidden()

    # Choosing a coarser resolution reveals the aggregation method.
    page._set_resolution(LinkageResolution.MONTHLY)
    assert not page.agg_method_combo.isHidden()
    assert page.lag_unit_label.text() == "month prior"


def test_hourly_contextual_enables_all(qtbot):
    wizard = JobConfigWizard()
    qtbot.addWidget(wizard)
    wizard.setField("contextual_resolution", "hourly")

    page = _pipeline_page(wizard)
    page.initializePage()

    for value in ("hourly", "daily", "monthly"):
        assert _combo_item_enabled(page.resolution_combo, value)
    assert page.resolution_combo.currentData() == "hourly"


def test_build_args_maps_resolution_and_agg(qtbot):
    wizard = JobConfigWizard()
    qtbot.addWidget(wizard)
    wizard.setField("contextual_resolution", "daily")

    page = _pipeline_page(wizard)
    page.initializePage()
    page._set_resolution(LinkageResolution.MONTHLY)
    median_idx = page.agg_method_combo.findData("median")
    page.agg_method_combo.setCurrentIndex(median_idx)

    args = build_args_from_wizard(wizard)
    assert LinkageResolution.from_str(args.linkage_resolution) is LinkageResolution.MONTHLY
    assert AggMethod.from_str(args.agg_method) is AggMethod.MEDIAN
