#!/usr/bin/env python
"""
Benchmark STITCH linkage throughput on the current machine, excluding load time.

What is and is not measured
---------------------------
Reading the survey file, the residential history file, and the contextual
measure files is **excluded**. Before each timed run the contextual cache is
warmed with exactly the GEOID filter and year set the pipeline will ask for, so
``preload_years`` inside the timed call is a no-op and no file is re-read.

What remains inside the timer is the actual linkage work:

1. building the per-lag date + GEOID columns (``prepare_lag_columns_batch``),
2. reconciling contextual data to the linkage resolution and hashing it into a
   ``(date, GEOID)`` lookup,
3. one merge per lag, and writing each lag's parquet intermediate.

Step 1 is timed a second time on its own so the GEOID-construction cost can be
separated from the merge cost. That split is what answers "does residential
history change linkage speed?" -- see the note printed under the table.

Usage
-----
    uv run python benchmarks/benchmark_linkage.py            # default sweep
    uv run python benchmarks/benchmark_linkage.py --quick    # fast smoke sweep
    uv run python benchmarks/benchmark_linkage.py --csv out.csv
"""

from __future__ import annotations

import argparse
import contextlib
import io
import platform
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

# Allow running directly from a source checkout.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from stitch.daily_measure import DailyMeasureDataDir
from stitch.hrs import HRSContextLinker, HRSInterviewData, ResidentialHistoryHRS
from stitch.process import (
    compute_required_years,
    extract_unique_geoids,
    process_multiple_lags_batch,
)

# Contextual data spans these years; survey dates are drawn from SURVEY_YEARS so
# that even the longest lag stays inside the contextual coverage.
CONTEXT_YEARS = range(2016, 2021)
SURVEY_YEARS = (2018, 2021)  # half-open, as used by np.random.Generator.integers
RESHIST_START_YEAR = 2015  # before any lag date, so lookups always resolve

DATA_COL = "index"
GEOID_COL = "GEOID2010"
ID_COL = "hhidpn"
DATE_COL = "iwdate"


# ---------------------------------------------------------------------------
# Synthetic data generation (all of this happens outside the timed region)
# ---------------------------------------------------------------------------
def make_geoid_pool(n_geoids: int, rng: np.random.Generator) -> List[str]:
    """Generate ``n_geoids`` unique 11-digit GEOIDs with no leading zero."""
    pool: set = set()
    while len(pool) < n_geoids:
        pool.update(
            int(g)
            for g in rng.integers(10_000_000_000, 100_000_000_000, size=n_geoids - len(pool))
        )
    return [str(g) for g in sorted(pool)]


def write_contextual_dir(out_dir: Path, geoids: List[str], rng: np.random.Generator) -> int:
    """
    Write one parquet file per year in long format. Returns total row count.

    Parquet keeps generation cheap for the larger GEOID pools; since load time
    is excluded from the measurement, the format has no bearing on the reported
    numbers either way.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for year in CONTEXT_YEARS:
        dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
        n_rows = len(dates) * len(geoids)
        pd.DataFrame(
            {
                "Date": np.repeat(dates.values, len(geoids)),
                "GEOID10": np.tile(geoids, len(dates)),
                DATA_COL: rng.uniform(20.0, 100.0, size=n_rows).astype("float32"),
            }
        ).to_parquet(out_dir / f"{year}_ctx.parquet", index=False)
        total += n_rows
    return total


def write_survey(path: Path, n_rows: int, geoids: List[str], rng: np.random.Generator) -> None:
    """One row per person, with a static GEOID column and an interview date."""
    ids = np.arange(10_000_001, 10_000_001 + n_rows)
    years = rng.integers(SURVEY_YEARS[0], SURVEY_YEARS[1], size=n_rows)
    months = rng.integers(1, 13, size=n_rows)
    days = rng.integers(1, 29, size=n_rows)
    pd.DataFrame(
        {
            ID_COL: ids,
            DATE_COL: pd.to_datetime(
                {"year": years, "month": months, "day": days}
            ),
            GEOID_COL: rng.choice(geoids, size=n_rows),
            "age": rng.integers(50, 90, size=n_rows),
        }
    ).to_parquet(path, index=False)


def write_reshist(
    path: Path, n_rows: int, geoids: List[str], rng: np.random.Generator, max_moves: int = 3
) -> int:
    """
    One row per residence. Every person gets an entry at RESHIST_START_YEAR plus
    0-``max_moves`` later moves, so every lag date resolves to a real GEOID.
    Returns the number of residence rows written.
    """
    ids = np.arange(10_000_001, 10_000_001 + n_rows)
    n_moves = rng.integers(0, max_moves + 1, size=n_rows)

    # Entry residence for everyone.
    frames = [
        pd.DataFrame(
            {
                ID_COL: ids,
                "move_date": f"{RESHIST_START_YEAR}-01-15",
                "GEOID": rng.choice(geoids, size=n_rows),
            }
        )
    ]
    # Move k happens in year RESHIST_START_YEAR + k, keeping moves chronological.
    for k in range(1, max_moves + 1):
        movers = ids[n_moves >= k]
        if len(movers) == 0:
            continue
        frames.append(
            pd.DataFrame(
                {
                    ID_COL: movers,
                    "move_date": f"{RESHIST_START_YEAR + k}-06-10",
                    "GEOID": rng.choice(geoids, size=len(movers)),
                }
            )
        )

    out = pd.concat(frames, ignore_index=True)
    out.to_parquet(path, index=False)
    return len(out)


# ---------------------------------------------------------------------------
# Timing harness
# ---------------------------------------------------------------------------
@dataclass
class Result:
    survey_rows: int
    geoid_pool: int
    unique_geoids: int
    n_lags: int
    ctx_rows_loaded: int
    reshist: str
    reshist_rows: int
    t_total: float
    t_geoid: float
    t_rest: float
    cells_per_sec: float
    ms_per_lag: float


@contextlib.contextmanager
def quiet():
    """Silence the pipeline's progress output so terminal I/O doesn't skew timings."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        yield


def warm_contextual_cache(
    hrs: HRSInterviewData, ctx: DailyMeasureDataDir, lags: List[int]
) -> int:
    """
    Load exactly the years and GEOIDs the pipeline will ask for, so that the
    ``preload_years`` call inside the timed region finds everything cached.

    Returns the number of contextual rows resident in the cache.
    """
    lag_df = HRSContextLinker.prepare_lag_columns_batch(hrs, lags, GEOID_COL)
    unique_geoids = extract_unique_geoids(lag_df, GEOID_COL)

    required = compute_required_years(hrs, max(lags))
    available = set(ctx.list_years())
    years = [str(y) for y in required if str(y) in available]

    ctx.geoid_filter = unique_geoids
    ctx.preload_years(years)
    return sum(len(ctx[y].df) for y in years)


def run_config(
    *,
    survey_path: Path,
    reshist_path: Optional[Path],
    ctx_dir: Path,
    geoid_pool: int,
    lags: List[int],
    temp_root: Path,
    repeat: int,
) -> Result:
    """Load everything (untimed), warm the cache (untimed), then time linkage."""
    use_reshist = reshist_path is not None

    with quiet():
        reshist = (
            ResidentialHistoryHRS(reshist_path, id_col=ID_COL, date_col="move_date", geoid_col="GEOID")
            if use_reshist
            else None
        )
        hrs = HRSInterviewData(
            survey_path,
            datecol=DATE_COL,
            move=use_reshist,
            residential_hist=reshist,
            hhidpn=ID_COL,
            geoid_col=GEOID_COL,
        )
        ctx = DailyMeasureDataDir(
            ctx_dir, data_col=DATA_COL, measure_type=None, file_extension=".parquet"
        )
        ctx_rows = warm_contextual_cache(hrs, ctx, lags)
        unique_geoids = len(ctx.geoid_filter)

    # --- timed: full linkage, warm cache, no file reads ---
    best_total = float("inf")
    for i in range(repeat):
        run_dir = temp_root / f"run_{i}"
        if run_dir.exists():
            shutil.rmtree(run_dir)
        run_dir.mkdir(parents=True)

        with quiet():
            start = time.perf_counter()
            process_multiple_lags_batch(
                hrs_data=hrs,
                contextual_dir=ctx,
                n_days=lags,
                id_col=ID_COL,
                temp_dir=run_dir,
                prefix="bench",
            )
            best_total = min(best_total, time.perf_counter() - start)
        shutil.rmtree(run_dir)

    # --- timed: GEOID/date column construction alone ---
    best_geoid = float("inf")
    for _ in range(repeat):
        with quiet():
            start = time.perf_counter()
            HRSContextLinker.prepare_lag_columns_batch(hrs, lags, GEOID_COL)
            best_geoid = min(best_geoid, time.perf_counter() - start)

    survey_rows = len(hrs.df)
    cells = survey_rows * len(lags)
    return Result(
        survey_rows=survey_rows,
        geoid_pool=geoid_pool,
        unique_geoids=unique_geoids,
        n_lags=len(lags),
        ctx_rows_loaded=ctx_rows,
        reshist="yes" if use_reshist else "no",
        reshist_rows=len(reshist.df) if use_reshist else 0,
        t_total=best_total,
        t_geoid=best_geoid,
        t_rest=best_total - best_geoid,
        cells_per_sec=cells / best_total,
        ms_per_lag=1000.0 * best_total / len(lags),
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
COLUMNS = [
    ("survey_rows", "survey rows", "{:,}"),
    ("geoid_pool", "geoid pool", "{:,}"),
    ("unique_geoids", "geoids linked", "{:,}"),
    ("n_lags", "n lags", "{:,}"),
    ("ctx_rows_loaded", "ctx rows", "{:,}"),
    ("reshist", "res hist", "{}"),
    ("reshist_rows", "res rows", "{:,}"),
    ("t_total", "total s", "{:.2f}"),
    ("t_geoid", "geoid s", "{:.2f}"),
    ("t_rest", "merge s", "{:.2f}"),
    ("cells_per_sec", "cells/s", "{:,.0f}"),
    ("ms_per_lag", "ms/lag", "{:.1f}"),
]


def print_table(results: List[Result]) -> None:
    header = [label for _, label, _ in COLUMNS]
    rows = [
        [fmt.format(getattr(r, key)) for key, _, fmt in COLUMNS] for r in results
    ]
    widths = [
        max(len(header[i]), max((len(row[i]) for row in rows), default=0))
        for i in range(len(header))
    ]

    def line(cells: List[str]) -> str:
        return "  ".join(c.rjust(w) for c, w in zip(cells, widths))

    print(line(header))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(line(row))


def print_machine_info() -> None:
    print("=" * 100)
    print("STITCH linkage benchmark — load time excluded")
    print("=" * 100)
    print(f"  platform : {platform.platform()}")
    print(f"  processor: {platform.processor() or 'n/a'}")
    try:
        import os

        print(f"  cpu cores: {os.cpu_count()}")
    except Exception:
        pass
    try:
        import psutil

        print(f"  memory   : {psutil.virtual_memory().total / 1024**3:.1f} GiB")
    except ImportError:
        pass
    print(f"  python   : {platform.python_version()}")
    print(f"  pandas   : {pd.__version__}   numpy: {np.__version__}")
    print()


# ---------------------------------------------------------------------------
# Sweep definition
# ---------------------------------------------------------------------------
def build_sweep(quick: bool) -> tuple:
    """
    One-factor-at-a-time sweep around a baseline, so each row isolates the
    effect of a single dimension rather than exploding combinatorially.
    """
    if quick:
        baseline = dict(rows=2_000, geoids=200, lags=30)
        row_axis = [1_000, 2_000, 5_000]
        lag_axis = [7, 30, 90]
        geoid_axis = [100, 200, 500]
    else:
        baseline = dict(rows=10_000, geoids=500, lags=90)
        row_axis = [1_000, 5_000, 10_000, 50_000]
        lag_axis = [7, 30, 90, 365]
        geoid_axis = [100, 500, 2_000]

    configs = []
    seen = set()
    for rows in row_axis:
        configs.append(dict(baseline, rows=rows))
    for lags in lag_axis:
        configs.append(dict(baseline, lags=lags))
    for geoids in geoid_axis:
        configs.append(dict(baseline, geoids=geoids))

    unique = []
    for cfg in configs:
        key = (cfg["rows"], cfg["geoids"], cfg["lags"])
        if key not in seen:
            seen.add(key)
            unique.append(cfg)
    return unique, baseline


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="smaller, faster sweep")
    parser.add_argument("--repeat", type=int, default=1, help="timed repeats per config (min is kept)")
    parser.add_argument("--csv", type=Path, default=None, help="also write results to this CSV")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--reshist",
        choices=["both", "yes", "no"],
        default="both",
        help="run with residential history, without, or both",
    )
    args = parser.parse_args()

    configs, baseline = build_sweep(args.quick)
    rng = np.random.default_rng(args.seed)

    print_machine_info()
    print(f"  baseline : {baseline['rows']:,} rows, {baseline['geoids']:,} geoids, {baseline['lags']} lags")
    print(f"  configs  : {len(configs)} (one-factor-at-a-time around baseline)")
    print(f"  repeats  : {args.repeat} (fastest run kept)")
    print()

    modes = {"both": [False, True], "yes": [True], "no": [False]}[args.reshist]
    results: List[Result] = []

    with tempfile.TemporaryDirectory(prefix="stitch_bench_") as tmp:
        tmp = Path(tmp)

        # Contextual data is shared across configs with the same geoid pool size.
        ctx_dirs: dict = {}
        pools: dict = {}
        for n_geoids in sorted({c["geoids"] for c in configs}):
            print(f"  generating contextual data for {n_geoids:,} geoids...", flush=True)
            pool = make_geoid_pool(n_geoids, rng)
            ctx_dir = tmp / f"ctx_{n_geoids}"
            write_contextual_dir(ctx_dir, pool, rng)
            ctx_dirs[n_geoids] = ctx_dir
            pools[n_geoids] = pool

        print()
        for cfg in configs:
            pool = pools[cfg["geoids"]]
            survey_path = tmp / f"survey_{cfg['rows']}_{cfg['geoids']}.parquet"
            if not survey_path.exists():
                write_survey(survey_path, cfg["rows"], pool, rng)

            reshist_path = tmp / f"reshist_{cfg['rows']}_{cfg['geoids']}.parquet"
            if not reshist_path.exists():
                write_reshist(reshist_path, cfg["rows"], pool, rng)

            lags = list(range(cfg["lags"]))
            for use_reshist in modes:
                label = "with res-hist" if use_reshist else "no res-hist "
                print(
                    f"  running {label} | {cfg['rows']:,} rows, "
                    f"{cfg['geoids']:,} geoids, {cfg['lags']} lags ...",
                    end="",
                    flush=True,
                )
                result = run_config(
                    survey_path=survey_path,
                    reshist_path=reshist_path if use_reshist else None,
                    ctx_dir=ctx_dirs[cfg["geoids"]],
                    geoid_pool=cfg["geoids"],
                    lags=lags,
                    temp_root=tmp / "work",
                    repeat=args.repeat,
                )
                results.append(result)
                print(f" {result.t_total:.2f}s")

    print()
    print("=" * 100)
    print("RESULTS  (load time excluded; 'cells' = survey rows x lags)")
    print("=" * 100)
    print_table(results)
    print()
    print("  geoid s = building per-lag date + GEOID columns (prepare_lag_columns_batch)")
    print("  merge s = resolution alignment, contextual hash, per-lag merge, parquet writes")

    with_rh = [r for r in results if r.reshist == "yes"]
    without_rh = [r for r in results if r.reshist == "no"]
    if with_rh and without_rh:
        ratio = (sum(r.t_total for r in with_rh) / sum(r.t_total for r in without_rh))
        geoid_ratio = (
            sum(r.t_geoid for r in with_rh) / max(sum(r.t_geoid for r in without_rh), 1e-9)
        )
        print()
        print(f"  residential history costs {ratio:.2f}x total linkage time overall,")
        print(f"  and {geoid_ratio:.2f}x in the GEOID-construction phase specifically.")

    if args.csv:
        pd.DataFrame([asdict(r) for r in results]).to_csv(args.csv, index=False)
        print(f"\n  wrote {args.csv}")


if __name__ == "__main__":
    main()
