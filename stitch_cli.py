#!/usr/bin/env python
"""
Lagged contextual data linkage for Survey datasets.

This script links daily contextual datasets (e.g., heat index, PM2.5)
to Survey interview/epigenetic data by computing n-day prior dates and GEOIDs,
then merging for each lag day. Supports both parallel and sequential processing.

Example:
--------
python stitch_cli.py \
    --survey-data "C:/path/to/Surveyprep2016full.dta" \
    --context-dir "C:/path/to/daily_heat_long" \
    --output "C:/path/to/output/SurveyHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --measure-type heat_index \
    --data-col HeatIndex \
    --n-lags 2191 \
    --geoid-col GEOID2010 \
    --contextual-geoid-col GEOID10 \
    --file-extension .parquet \
    --parallel

Averaging across lags:
----------------------
Add ``--post-lag-average`` to collapse the per-lag measure columns into a single
averaged column per measure (e.g. ``HeatIndex_avg_0_2190day_prior``). This is
strict: any participant missing a value for any lag in the range gets a missing
(NaN) average. It is incompatible with ``--include-lag-date`` (which is ignored
when both are given). Add ``--save-temp-to-output`` to keep the intermediate
per-lag files as CSV under ``<save-dir>/<output_stem>_lag_files/``.

With residential history:
-------------------------
The residential history file is a simple long-format table with one row per
residence: a participant ID column, a move date column (format is inferred;
the earliest entry per person is their residence at survey entry), and a
GEOID column.

python stitch_cli.py \
    --survey-data "C:/path/to/Surveyprep2016full.dta" \
    --residential-hist "C:/path/to/residential_history.dta" \
    --res-hist-id-col hhidpn \
    --res-hist-date-col move_date \
    --res-hist-geoid-col GEOID \
    --context-dir "C:/path/to/daily_heat_long" \
    --output "C:/path/to/output/SurveyHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --measure-type heat_index \
    --data-col HeatIndex \
    --n-lags 2191 \
    --geoid-col GEOID2010 \
    --contextual-geoid-col GEOID10 \
    --file-extension .parquet \
    --parallel
"""

import argparse

from stitch.process import run_pipeline


def _create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Link daily contextual data to Survey dataset with n-day lags."
    )
    parser.add_argument(
        "--survey-data", required=True, help="Path to Survey Stata file"
    )
    parser.add_argument(
        "--context-dir",
        required=True,
        help="Directory containing daily contextual data files (supports CSV, Stata, Parquet, Feather, Excel)",
    )
    parser.add_argument(
        "--output_name",
        default="linked_data.dta",
        type=str,
        help="Output .dta file name",
    )
    parser.add_argument(
        "--id-col",
        required=True,
        help="Unique identifier column name (survey participants)",
    )
    parser.add_argument(
        "--date-col", required=True, help="Interview date column name (survey)"
    )
    parser.add_argument(
        "--measure-type",
        required=True,
        help="Measurement type (e.g., heat_index, pm25, ozone, other contextual data). File name must include this as substrings",
    )
    parser.add_argument(
        "--save-dir",
        required=True,
        help="Directory where output and temporary lag files will be saved",
    )

    parser.add_argument(
        "--data-col",
        help="Explicit data column name to use.",
    )
    parser.add_argument(
        "--geoid-col",
        default="GEOID2010",
        help="GEOID column name in Survey data (default: GEOID2010)",
    )
    parser.add_argument(
        "--contextual-geoid-col",
        default="GEOID10",
        help="GEOID column name in contextual data files (default: GEOID10)",
    )
    parser.add_argument(
        "--file-extension",
        help="File extension to search for in context directory (e.g., .csv, .parquet). If not specified, searches all supported formats.",
    )
    parser.add_argument(
        "--residential-hist", help="Path to residential history file (optional)"
    )

    # Residential history configuration options
    parser.add_argument(
        "--res-hist-id-col",
        default="hhidpn",
        help="ID column name in residential history (default: hhidpn)",
    )
    parser.add_argument(
        "--res-hist-date-col",
        default="move_date",
        help="Move date column name in residential history (default: move_date). "
        "Format is inferred per value: dates, year-month (2010-03), month names "
        "(March 2010), or numeric YYYY / YYYYMM / YYYYMMDD. Values coarser than "
        "daily are anchored to the midpoint of the period they span. The "
        "earliest entry per person is their residence at survey entry.",
    )
    parser.add_argument(
        "--res-hist-geoid-col",
        default="GEOID",
        help="GEOID column name in residential history (default: GEOID)",
    )

    parser.add_argument(
        "--linkage-resolution",
        default="daily",
        choices=["hourly", "daily", "monthly"],
        help="Temporal resolution for linkage (default: daily). Lags are counted "
        "in this unit (hours / days / months). Must not be finer than the "
        "contextual data's own resolution.",
    )
    parser.add_argument(
        "--agg-method",
        default="average",
        choices=["average", "midpoint"],
        help="How to reconcile contextual data when the requested resolution is "
        "coarser than the data: 'average' (mean within each period) or "
        "'midpoint' (observation nearest the period midpoint). Ignored when the "
        "resolution matches the data (default: average).",
    )
    parser.add_argument(
        "--n-lags",
        type=int,
        default=365,
        help="Number of lags to process, in the linkage-resolution unit "
        "(default: 365).",
    )
    parser.add_argument(
        "--start-lag",
        type=int,
        default=0,
        help="Lag to start from, i.e. minimum periods prior in the "
        "linkage-resolution unit (default: 0).",
    )
    parser.add_argument(
        "--parallel", action="store_true", help="Use parallel processing"
    )
    parser.add_argument(
        "--include-lag-date",
        action="store_true",
        help="Include lag date and GEOID columns in the output (default: False). "
        "Ignored if --post-lag-average is also set (averaging wins).",
    )
    parser.add_argument(
        "--post-lag-average",
        action="store_true",
        help="Average each measure across all lags into a single column per "
        "measure (e.g. HeatIndex_avg_0_365day_prior) instead of one column per "
        "lag. Strict: a participant missing any lag gets a missing (NaN) average. "
        "Incompatible with --include-lag-date (which is ignored if both are set).",
    )
    parser.add_argument(
        "--save-temp-to-output",
        action="store_true",
        help="Write the intermediate per-lag files as CSV into "
        "<save-dir>/<output_stem>_lag_files/ and keep them after the run "
        "(default: hidden Parquet files in a private temp dir, deleted on success).",
    )

    # GEOID normalization options
    parser.add_argument(
        "--geoid-treatment",
        default="code",
        choices=["code", "numeric"],
        help='GEOID treatment: "code" for zero-padded string, "numeric" for int/float (default: code)',
    )
    parser.add_argument(
        "--geoid-n-digits",
        type=int,
        default=11,
        help="Number of digits for zero-padded GEOID strings (default: 11). "
        "Set to 0 to disable zero-padding (digits only). "
        "Only used with --geoid-treatment code.",
    )
    parser.add_argument(
        "--geoid-numeric-type",
        default="int",
        choices=["int", "float"],
        help='Numeric type for GEOID when --geoid-treatment is "numeric" (default: int)',
    )
    return parser


def main():
    """Entry point for script execution."""
    import multiprocessing

    multiprocessing.freeze_support()
    parser = _create_parser()
    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
