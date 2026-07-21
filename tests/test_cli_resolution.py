"""Tests for the linkage-resolution CLI flags and end-to-end CLI runs."""

import pandas as pd
import pytest

from stitch_cli import _create_parser
from stitch.process import run_pipeline


_REQUIRED = [
    "--survey-data", "survey.dta",
    "--context-dir", "context",
    "--id-col", "hhidpn",
    "--date-col", "iwdate",
    "--measure-type", "heat",
    "--save-dir", "out",
]


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def test_linkage_resolution_defaults_daily():
    args = _create_parser().parse_args(_REQUIRED)
    assert args.linkage_resolution == "daily"
    assert args.agg_method == "average"


@pytest.mark.parametrize("res", ["hourly", "daily", "monthly"])
def test_linkage_resolution_override(res):
    args = _create_parser().parse_args(_REQUIRED + ["--linkage-resolution", res])
    assert args.linkage_resolution == res


@pytest.mark.parametrize("method", ["average", "median"])
def test_agg_method_override(method):
    args = _create_parser().parse_args(_REQUIRED + ["--agg-method", method])
    assert args.agg_method == method


def test_invalid_resolution_rejected():
    with pytest.raises(SystemExit):
        _create_parser().parse_args(_REQUIRED + ["--linkage-resolution", "weekly"])


def test_invalid_agg_method_rejected():
    with pytest.raises(SystemExit):
        _create_parser().parse_args(_REQUIRED + ["--agg-method", "sum"])


# ---------------------------------------------------------------------------
# End-to-end CLI runs per resolution
# ---------------------------------------------------------------------------


def _run_args(parser, *, survey, context_dir, save_dir, resolution, n_lags, extra=None):
    argv = [
        "--survey-data", str(survey),
        "--context-dir", str(context_dir),
        "--id-col", "hhidpn",
        "--date-col", "iwdate",
        "--measure-type", "heat_index",
        "--data-col", "index",
        "--geoid-col", "GEOID2010_2010",
        "--contextual-geoid-col", "GEOID10",
        "--save-dir", str(save_dir),
        "--output_name", "linked.dta",
        "--linkage-resolution", resolution,
        "--n-lags", str(n_lags),
    ]
    if extra:
        argv += extra
    return parser.parse_args(argv)


def test_cli_run_daily(survey_data_2016_2020, heat_index_dir, tmp_path):
    parser = _create_parser()
    save_dir = tmp_path / "daily_out"
    args = _run_args(
        parser,
        survey=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        resolution="daily",
        n_lags=3,
    )
    run_pipeline(args)

    out = pd.read_stata(save_dir / "linked.dta")
    for n in (0, 1, 2):
        assert f"index_iwdate_{n}day_prior" in out.columns


def test_cli_run_monthly(survey_data_2016_2020, heat_index_dir, tmp_path):
    parser = _create_parser()
    save_dir = tmp_path / "monthly_out"
    args = _run_args(
        parser,
        survey=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        resolution="monthly",
        n_lags=2,
        extra=["--agg-method", "average"],
    )
    run_pipeline(args)

    out = pd.read_stata(save_dir / "linked.dta")
    for n in (0, 1):
        assert f"index_iwdate_{n}month_prior" in out.columns
    assert out["index_iwdate_0month_prior"].notna().any()


def test_cli_run_monthly_post_lag_average(
    survey_data_2016_2020, heat_index_dir, tmp_path
):
    parser = _create_parser()
    save_dir = tmp_path / "monthly_avg_out"
    args = _run_args(
        parser,
        survey=survey_data_2016_2020,
        context_dir=heat_index_dir,
        save_dir=save_dir,
        resolution="monthly",
        n_lags=3,
        extra=["--post-lag-average"],
    )
    run_pipeline(args)

    out = pd.read_stata(save_dir / "linked.dta")
    # Averaged column name uses the resolution unit.
    assert "index_avg_0_2month_prior" in out.columns
