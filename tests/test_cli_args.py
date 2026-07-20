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


def test_post_lag_average_defaults_false():
    parser = _create_parser()
    args = parser.parse_args(_REQUIRED)
    assert args.post_lag_average is False


def test_post_lag_average_flag_sets_true():
    parser = _create_parser()
    args = parser.parse_args(_REQUIRED + ["--post-lag-average"])
    assert args.post_lag_average is True


def test_save_temp_to_output_defaults_false():
    parser = _create_parser()
    args = parser.parse_args(_REQUIRED)
    assert args.save_temp_to_output is False


def test_save_temp_to_output_flag_sets_true():
    parser = _create_parser()
    args = parser.parse_args(_REQUIRED + ["--save-temp-to-output"])
    assert args.save_temp_to_output is True


def test_post_lag_average_and_include_lag_date_parse_together():
    """Both flags parse successfully; the conflict is resolved at runtime."""
    parser = _create_parser()
    args = parser.parse_args(
        _REQUIRED + ["--post-lag-average", "--include-lag-date"]
    )
    assert args.post_lag_average is True
    assert args.include_lag_date is True
