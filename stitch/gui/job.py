"""
Job model and helpers for the multi-job dashboard.

A "job" is a single pipeline run: a fully-configured ``argparse.Namespace``
plus a display name and a status used by the dashboard queue.
"""

import argparse
from dataclasses import dataclass
from pathlib import Path


# Job status values used by the dashboard queue
STATUS_PENDING = "Pending"
STATUS_RUNNING = "Running"
STATUS_DONE = "Done"
STATUS_FAILED = "Failed"


@dataclass
class Job:
    """A single pipeline run queued in the dashboard."""

    name: str
    args: argparse.Namespace
    status: str = STATUS_PENDING


def build_args_from_wizard(wizard) -> argparse.Namespace:
    """Build a pipeline arguments namespace from a configured wizard.

    Reads every configuration value registered via ``registerField`` across the
    config pages and assembles the ``argparse.Namespace`` expected by
    ``run_pipeline``.
    """
    args = argparse.Namespace(
        survey_data=wizard.field("hrs_data_path"),
        context_dir=wizard.field("context_dir"),
        output_name=wizard.field("output_name"),
        id_col=wizard.field("id_col"),
        date_col=wizard.field("date_col"),
        measure_type=wizard.field("measure_type"),
        save_dir=wizard.field("save_dir"),
        data_col=wizard.field("data_col"),
        geoid_col=wizard.field("geoid_col"),
        contextual_geoid_col=wizard.field("contextual_geoid_col"),
        context_date_col=wizard.field("context_date_col"),
        parallel=wizard.field("parallel"),
        include_lag_date=wizard.field("include_lag_date"),
        post_lag_average=wizard.field("post_lag_average"),
        save_temp_to_output=wizard.field("save_temp_to_output"),
    )

    # Temporal lag window: GUI presents an inclusive [start, end] range in the
    # chosen resolution unit; internally n_lags is the exclusive upper bound
    # (max lag = n_lags-1).
    start_lag = int(wizard.field("start_lag") or 0)
    end_lag = int(wizard.field("end_lag") or 0)
    args.start_lag = start_lag
    args.n_lags = end_lag + 1

    # Linkage temporal resolution and coarsening aggregation method.
    args.linkage_resolution = wizard.field("linkage_resolution") or "daily"
    args.agg_method = wizard.field("agg_method") or "average"

    # Optional: file extension
    file_ext = wizard.field("file_extension")
    args.file_extension = file_ext if file_ext != "Auto-detect" else None

    # GEOID normalization config
    args.geoid_treatment = wizard.field("geoid_treatment") or "code"
    zero_pad = wizard.field("geoid_zero_pad")
    if zero_pad:
        args.geoid_n_digits = int(wizard.field("geoid_n_digits") or 11)
    else:
        args.geoid_n_digits = 0
    args.geoid_numeric_type = wizard.field("geoid_numeric_type") or "int"

    # Optional: residential history
    if wizard.field("use_residential_hist"):
        args.residential_hist = wizard.field("residential_hist_path")
        args.res_hist_id_col = wizard.field("res_hist_id_col")
        args.res_hist_date_col = wizard.field("res_hist_date_col")
        args.res_hist_geoid_col = wizard.field("res_hist_geoid_col")
    else:
        args.residential_hist = None

    return args


def default_job_name(args: argparse.Namespace, index: int) -> str:
    """Derive a default display name for a job from its configuration.

    Uses the measure type and output filename when available, falling back to a
    generic indexed name.
    """
    measure = (getattr(args, "measure_type", "") or "").strip()
    output_name = (getattr(args, "output_name", "") or "").strip()
    output_stem = Path(output_name).stem if output_name else ""

    parts = [p for p in (measure, output_stem) if p]
    label = " - ".join(parts) if parts else "job"
    return f"Job {index}: {label}"
