#!/usr/bin/env python
"""
Compare batch (single-process) against parallel linkage across worker counts.

Load time is excluded exactly as in benchmark_linkage.py: the contextual cache is
warmed with the GEOID filter and year set the pipeline will request, so the timed
region contains only linkage work.

The last two lines matter most. ``process_multiple_lags_parallel`` builds the
per-lag date/GEOID columns in the *parent* process before the pool starts, so
that phase is serial in both modes. It bounds the achievable speedup no matter
how many workers are added.

Usage
-----
    uv run python benchmarks/benchmark_parallel.py
    uv run python benchmarks/benchmark_parallel.py --rows 50000 --lags 365
    uv run python benchmarks/benchmark_parallel.py --workers 4,8,16 --csv out.csv
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from benchmark_linkage import (  # noqa: E402
    DATA_COL,
    DATE_COL,
    GEOID_COL,
    ID_COL,
    make_geoid_pool,
    quiet,
    warm_contextual_cache,
    write_contextual_dir,
    write_reshist,
    write_survey,
)
from stitch.daily_measure import DailyMeasureDataDir  # noqa: E402
from stitch.hrs import HRSContextLinker, HRSInterviewData, ResidentialHistoryHRS  # noqa: E402
from stitch.process import (  # noqa: E402
    process_multiple_lags_batch,
    process_multiple_lags_parallel,
)


def load_inputs(survey: Path, reshist: Path, ctx_dir: Path, lags: List[int]):
    """Construct the pipeline inputs and warm the contextual cache (untimed)."""
    with quiet():
        hrs = HRSInterviewData(
            survey,
            datecol=DATE_COL,
            move=True,
            residential_hist=ResidentialHistoryHRS(
                reshist, id_col=ID_COL, date_col="move_date", geoid_col="GEOID"
            ),
            hhidpn=ID_COL,
            geoid_col=GEOID_COL,
        )
        ctx = DailyMeasureDataDir(
            ctx_dir, data_col=DATA_COL, measure_type=None, file_extension=".parquet"
        )
        warm_contextual_cache(hrs, ctx, lags)
    return hrs, ctx


def time_run(fn, work_root: Path, label: str, repeat: int, **kwargs) -> float:
    """Run *fn* into a fresh temp dir, returning the fastest wall time."""
    best = float("inf")
    for i in range(repeat):
        out_dir = work_root / f"{label}_{i}"
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True)
        with quiet():
            start = time.perf_counter()
            fn(temp_dir=out_dir, **kwargs)
            best = min(best, time.perf_counter() - start)
        shutil.rmtree(out_dir)
    return best


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--lags", type=int, default=90)
    parser.add_argument("--geoids", type=int, default=500)
    parser.add_argument(
        "--workers",
        type=str,
        default="2,4,8,16",
        help="comma-separated worker counts to test",
    )
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--csv", type=Path, default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    workers = [int(w) for w in args.workers.split(",") if w.strip()]
    lags = list(range(args.lags))
    rng = np.random.default_rng(args.seed)

    print("=" * 72)
    print("STITCH parallel scaling — load time excluded, residential history on")
    print("=" * 72)
    print(f"  platform : {platform.platform()}")
    print(f"  cpu cores: {os.cpu_count()}")
    print(f"  config   : {args.rows:,} rows, {args.lags} lags, {args.geoids:,} geoids")
    print()

    rows = []
    with tempfile.TemporaryDirectory(prefix="stitch_par_") as tmp:
        tmp = Path(tmp)

        print("  generating data ...", flush=True)
        pool = make_geoid_pool(args.geoids, rng)
        ctx_dir = tmp / "ctx"
        write_contextual_dir(ctx_dir, pool, rng)
        survey = tmp / "survey.parquet"
        write_survey(survey, args.rows, rng)
        reshist = tmp / "reshist.parquet"
        write_reshist(reshist, args.rows, pool, rng)

        hrs, ctx = load_inputs(survey, reshist, ctx_dir, lags)
        common = dict(
            hrs_data=hrs,
            contextual_dir=ctx,
            n_days=lags,
            id_col=ID_COL,
            prefix="bench",
        )

        print("  timing batch ...", flush=True)
        base = time_run(
            process_multiple_lags_batch, tmp, "batch", args.repeat, **common
        )
        rows.append({"mode": "batch", "workers": 1, "seconds": base, "speedup": 1.0})

        for w in workers:
            print(f"  timing parallel, {w} workers ...", flush=True)
            elapsed = time_run(
                process_multiple_lags_parallel,
                tmp,
                f"par{w}",
                args.repeat,
                max_workers=w,
                **common,
            )
            rows.append(
                {
                    "mode": "parallel",
                    "workers": w,
                    "seconds": elapsed,
                    "speedup": base / elapsed,
                }
            )

        # Cost of building every lag's date/GEOID columns in one process. The
        # batch path pays this serially; the parallel path distributes it across
        # workers, one lag per task.
        with quiet():
            start = time.perf_counter()
            HRSContextLinker.prepare_lag_columns_batch(hrs, lags, GEOID_COL)
            prep = time.perf_counter() - start

    print()
    print(f"  {'mode':<22}{'seconds':>10}{'speedup':>10}")
    print(f"  {'-' * 42}")
    for r in rows:
        label = "batch (1 process)" if r["mode"] == "batch" else f"parallel, {r['workers']} workers"
        print(f"  {label:<22}{r['seconds']:>10.2f}{r['speedup']:>9.2f}x")

    print()
    print(
        f"  lag-column build: {prep:.2f}s = {100 * prep / base:.0f}% of batch "
        f"(serial in batch, distributed in parallel)"
    )

    if args.csv:
        pd.DataFrame(rows).to_csv(args.csv, index=False)
        print(f"\n  wrote {args.csv}")


if __name__ == "__main__":
    main()
