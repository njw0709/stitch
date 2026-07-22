#!/usr/bin/env python
"""
Benchmark STITCH linkage throughput on the current machine, excluding load time.

Sweeps three axes, one at a time around a baseline: survey rows, number of lags,
and number of unique GEOIDs (which sets how many rows of contextual data get
loaded). Residential history is always enabled.

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
import os
import platform
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List

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
MAX_MOVES = 3

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
            for g in rng.integers(
                10_000_000_000, 100_000_000_000, size=n_geoids - len(pool)
            )
        )
    return [str(g) for g in sorted(pool)]


def write_contextual_dir(
    out_dir: Path, geoids: List[str], rng: np.random.Generator
) -> int:
    """Write one parquet file per year in long format. Returns total row count."""
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


def write_survey(path: Path, n_rows: int, rng: np.random.Generator) -> None:
    """
    One row per person, with an interview date. No GEOID column: residential
    history supplies the GEOID for each lag date.
    """
    pd.DataFrame(
        {
            ID_COL: np.arange(10_000_001, 10_000_001 + n_rows),
            DATE_COL: pd.to_datetime(
                {
                    "year": rng.integers(*SURVEY_YEARS, size=n_rows),
                    "month": rng.integers(1, 13, size=n_rows),
                    "day": rng.integers(1, 29, size=n_rows),
                }
            ),
            "age": rng.integers(50, 90, size=n_rows),
        }
    ).to_parquet(path, index=False)


def write_reshist(
    path: Path, n_rows: int, geoids: List[str], rng: np.random.Generator
) -> None:
    """
    One row per residence. Every person gets an entry at RESHIST_START_YEAR plus
    0-MAX_MOVES later moves, so every lag date resolves to a real GEOID.
    """
    ids = np.arange(10_000_001, 10_000_001 + n_rows)
    n_moves = rng.integers(0, MAX_MOVES + 1, size=n_rows)

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
    for k in range(1, MAX_MOVES + 1):
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

    pd.concat(frames, ignore_index=True).to_parquet(path, index=False)


# ---------------------------------------------------------------------------
# Timing harness
# ---------------------------------------------------------------------------
@dataclass
class Result:
    survey_rows: int
    n_lags: int
    unique_geoids: int
    ctx_rows: int
    t_total: float
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
    ctx.geoid_filter = extract_unique_geoids(lag_df, GEOID_COL)

    available = set(ctx.list_years())
    years = [
        str(y) for y in compute_required_years(hrs, max(lags)) if str(y) in available
    ]
    ctx.preload_years(years)
    return sum(len(ctx[y].df) for y in years)


def run_config(
    *,
    survey_path: Path,
    reshist_path: Path,
    ctx_dir: Path,
    lags: List[int],
    temp_root: Path,
    repeat: int,
) -> Result:
    """Load everything (untimed), warm the cache (untimed), then time linkage."""
    with quiet():
        hrs = HRSInterviewData(
            survey_path,
            datecol=DATE_COL,
            move=True,
            residential_hist=ResidentialHistoryHRS(
                reshist_path, id_col=ID_COL, date_col="move_date", geoid_col="GEOID"
            ),
            hhidpn=ID_COL,
            geoid_col=GEOID_COL,
        )
        ctx = DailyMeasureDataDir(
            ctx_dir, data_col=DATA_COL, measure_type=None, file_extension=".parquet"
        )
        ctx_rows = warm_contextual_cache(hrs, ctx, lags)
        unique_geoids = len(ctx.geoid_filter)

    # --- timed: full linkage, warm cache, no file reads ---
    t_total = float("inf")
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
            t_total = min(t_total, time.perf_counter() - start)
        shutil.rmtree(run_dir)

    return Result(
        survey_rows=len(hrs.df),
        n_lags=len(lags),
        unique_geoids=unique_geoids,
        ctx_rows=ctx_rows,
        t_total=t_total,
        ms_per_lag=1000.0 * t_total / len(lags),
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
COLUMNS = [
    ("survey_rows", "rows", "{:,}"),
    ("n_lags", "lags", "{:,}"),
    ("unique_geoids", "geoids", "{:,}"),
    ("ctx_rows", "ctx rows", "{:,}"),
    ("t_total", "total s", "{:.2f}"),
    ("ms_per_lag", "ms/lag", "{:.1f}"),
]


def print_table(results: List[Result]) -> None:
    header = [label for _, label, _ in COLUMNS]
    rows = [[fmt.format(getattr(r, key)) for key, _, fmt in COLUMNS] for r in results]
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
    print("=" * 72)
    print("STITCH linkage benchmark — load time excluded, residential history on")
    print("=" * 72)
    print(f"  platform : {platform.platform()}")
    print(f"  cpu cores: {os.cpu_count()}")
    try:
        import psutil

        print(f"  memory   : {psutil.virtual_memory().total / 1024**3:.1f} GiB")
    except ImportError:
        pass
    print(f"  python   : {platform.python_version()}")
    print(f"  pandas   : {pd.__version__}   numpy: {np.__version__}")


# ---------------------------------------------------------------------------
# Sweep definition
# ---------------------------------------------------------------------------
def build_sweep(quick: bool):
    """
    One factor at a time around a baseline, so each row isolates the effect of a
    single axis rather than exploding combinatorially.
    """
    if quick:
        baseline = dict(rows=2_000, lags=30, geoids=200)
        axes = dict(
            rows=[1_000, 2_000, 5_000],
            lags=[7, 30, 90],
            geoids=[100, 200, 500],
        )
    else:
        baseline = dict(rows=10_000, lags=90, geoids=500)
        axes = dict(
            rows=[1_000, 5_000, 10_000, 50_000],
            lags=[7, 30, 90, 365],
            geoids=[100, 500, 2_000],
        )

    configs, seen = [], set()
    for axis, values in axes.items():
        for value in values:
            cfg = dict(baseline, **{axis: value})
            key = (cfg["rows"], cfg["lags"], cfg["geoids"])
            if key not in seen:
                seen.add(key)
                configs.append(cfg)
    return configs, baseline


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="smaller, faster sweep")
    parser.add_argument(
        "--repeat", type=int, default=1, help="timed repeats per config (fastest kept)"
    )
    parser.add_argument("--csv", type=Path, default=None, help="write results to CSV")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    configs, baseline = build_sweep(args.quick)
    rng = np.random.default_rng(args.seed)

    print_machine_info()
    print(
        f"  baseline : {baseline['rows']:,} rows, "
        f"{baseline['lags']} lags, {baseline['geoids']:,} geoids"
    )
    print(f"  configs  : {len(configs)}, one factor at a time")
    print()

    results: List[Result] = []
    with tempfile.TemporaryDirectory(prefix="stitch_bench_") as tmp:
        tmp = Path(tmp)

        # Contextual data and GEOID pools are shared across configs of equal size.
        pools, ctx_dirs = {}, {}
        for n_geoids in sorted({c["geoids"] for c in configs}):
            print(f"  generating contextual data: {n_geoids:,} geoids ...", flush=True)
            pools[n_geoids] = make_geoid_pool(n_geoids, rng)
            ctx_dirs[n_geoids] = tmp / f"ctx_{n_geoids}"
            write_contextual_dir(ctx_dirs[n_geoids], pools[n_geoids], rng)
        print()

        for cfg in configs:
            survey_path = tmp / f"survey_{cfg['rows']}.parquet"
            if not survey_path.exists():
                write_survey(survey_path, cfg["rows"], rng)

            reshist_path = tmp / f"reshist_{cfg['rows']}_{cfg['geoids']}.parquet"
            if not reshist_path.exists():
                write_reshist(reshist_path, cfg["rows"], pools[cfg["geoids"]], rng)

            print(
                f"  {cfg['rows']:>6,} rows | {cfg['lags']:>3} lags | "
                f"{cfg['geoids']:>5,} geoids ...",
                end="",
                flush=True,
            )
            result = run_config(
                survey_path=survey_path,
                reshist_path=reshist_path,
                ctx_dir=ctx_dirs[cfg["geoids"]],
                lags=list(range(cfg["lags"])),
                temp_root=tmp / "work",
                repeat=args.repeat,
            )
            results.append(result)
            print(f" {result.t_total:.2f}s")

    print()
    print("=" * 72)
    print("RESULTS  (load time excluded)")
    print("=" * 72)
    print_table(results)

    if args.csv:
        pd.DataFrame([asdict(r) for r in results]).to_csv(args.csv, index=False)
        print(f"\n  wrote {args.csv}")


if __name__ == "__main__":
    main()
