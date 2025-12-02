#!/usr/bin/env python
"""
Lagged contextual data linkage for HRS datasets.

This script links daily contextual datasets (e.g., heat index, PM2.5)
to HRS interview/epigenetic data by computing n-day prior dates and GEOIDs,
then merging for each lag day. Supports both parallel and sequential processing.

Example:
--------
python stitch_cli.py \
    --hrs-data "C:/path/to/HRSprep2016full.dta" \
    --context-dir "C:/path/to/daily_heat_long" \
    --output "C:/path/to/output/HRSHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --measure-type heat_index \
    --data-col HeatIndex \
    --n-lags 2191 \
    --geoid-col LINKCEN2010 \
    --contextual-geoid-col GEOID10 \
    --file-extension .parquet \
    --parallel

With residential history:
-------------------------
python stitch_cli.py \
    --hrs-data "C:/path/to/HRSprep2016full.dta" \
    --residential-hist "C:/path/to/residential_history.dta" \
    --res-hist-hhidpn hhidpn \
    --res-hist-movecol trmove_tr \
    --res-hist-mvyear mvyear \
    --res-hist-mvmonth mvmonth \
    --res-hist-moved-mark "1. move" \
    --res-hist-geoid LINKCEN2010 \
    --res-hist-survey-yr-col year \
    --res-hist-first-tract-mark 999.0 \
    --context-dir "C:/path/to/daily_heat_long" \
    --output "C:/path/to/output/HRSHeatLinked.dta" \
    --id-col hhidpn \
    --date-col iwdate \
    --measure-type heat_index \
    --data-col HeatIndex \
    --n-lags 2191 \
    --geoid-col LINKCEN2010 \
    --contextual-geoid-col GEOID10 \
    --file-extension .parquet \
    --parallel
"""

import argparse

from stitch.process import run_pipeline


def _create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Link daily contextual data to HRS dataset with n-day lags."
    )
    parser.add_argument("--hrs-data", required=True, help="Path to HRS Stata file")
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
        default="LINKCEN2010",
        help="GEOID column name in HRS data (default: LINKCEN2010)",
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
        "--res-hist-hhidpn",
        default="hhidpn",
        help="ID column name in residential history (default: hhidpn)",
    )
    parser.add_argument(
        "--res-hist-movecol",
        default="trmove_tr",
        help="Move indicator column name in residential history (default: trmove_tr)",
    )
    parser.add_argument(
        "--res-hist-mvyear",
        default="mvyear",
        help="Move year column name in residential history (default: mvyear)",
    )
    parser.add_argument(
        "--res-hist-mvmonth",
        default="mvmonth",
        help="Move month column name in residential history (default: mvmonth)",
    )
    parser.add_argument(
        "--res-hist-moved-mark",
        default="1. move",
        help="Value indicating a move occurred in residential history (default: '1. move')",
    )
    parser.add_argument(
        "--res-hist-geoid",
        default="LINKCEN2010",
        help="GEOID column name in residential history (default: LINKCEN2010)",
    )
    parser.add_argument(
        "--res-hist-survey-yr-col",
        default="year",
        help="Survey year column name in residential history (default: year)",
    )
    parser.add_argument(
        "--res-hist-first-tract-mark",
        type=float,
        default=999.0,
        help="Value indicating first tract in residential history (default: 999.0)",
    )

    parser.add_argument(
        "--n-lags",
        type=int,
        default=365,
        help="Number of lags to process (default: 365)",
    )
    parser.add_argument(
        "--parallel", action="store_true", help="Use parallel processing"
    )
    parser.add_argument(
        "--include-lag-date",
        action="store_true",
        help="Include lag date columns in the output (default: False)",
    )
    return parser


def main():
    """Entry point for script execution."""
    parser = _create_parser()
    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
