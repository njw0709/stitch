"""Tests for the command-line argument parser in ``stitch_cli``."""

from stitch_cli import _create_parser


_REQUIRED = [
    "--survey-data", "survey.dta",
    "--context-dir", "context",
    "--id-col", "hhidpn",
    "--date-col", "iwdate",
    "--measure-type", "heat",
    "--save-dir", "out",
]


def test_start_lag_defaults_to_zero():
    parser = _create_parser()
    args = parser.parse_args(_REQUIRED)
    assert args.start_lag == 0


def test_start_lag_override():
    parser = _create_parser()
    args = parser.parse_args(_REQUIRED + ["--start-lag", "30"])
    assert args.start_lag == 30
