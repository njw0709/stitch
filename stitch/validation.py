"""
Configuration validation shared by the CLI, the notebook API and the GUI.

Every column name in a STITCH job plays exactly one *role* — the survey's ID /
date / GEOID columns, the residential history's ID / move-date / GEOID columns,
the contextual data's date / GEOID / measure columns. Pointing two roles at the
same column is never meaningful, and the failure it produces is far away from
the cause: the survey loader normalizes the date column and then the ID column
(``HRSInterviewData.__init__``), so picking one column for both silently turns
interview dates into epoch nanoseconds and the run dies much later inside the
lag machinery. The other collisions are worse still — several of them corrupt
quietly, because the parallel path swallows per-lag exceptions.

This module is the single source of truth for those checks. Every ``check_*``
function is pure and never raises: it returns a list of human-readable problem
strings. Callers decide how to surface them — :func:`validate_pipeline_args`
joins them into one ``ValueError`` for ``run_pipeline``, while the GUI wraps
them into ``(is_valid, message)`` tuples for ``QWizardPage.validatePage``.

The GUI checks are fast feedback, not a guarantee: ``QWizard`` only validates
the page being left, so a user who goes Back and edits an earlier page can
still assemble a bad job. The ``run_pipeline`` checks are the authoritative
gate.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple, Union

from .temporal import LinkageResolution

__all__ = [
    "duplicate_column_values",
    "check_column_role_collisions",
    "check_survey_column_roles",
    "check_residential_history_column_roles",
    "check_contextual_column_roles",
    "check_output_path_conflicts",
    "generated_column_names",
    "check_generated_column_collisions",
    "check_pipeline_args",
    "check_pipeline_survey_columns",
    "format_problems",
    "validate_pipeline_args",
    "validate_pipeline_survey_columns",
]


PROBLEM_HEADER = "STITCH configuration is not valid:"


# ----------------------------------------------------------------------
# Primitives
# ----------------------------------------------------------------------
def _clean(name) -> str:
    """Normalize a configured column name; blank/``None`` becomes ``""``.

    Only surrounding whitespace is stripped — names are compared *exactly*
    otherwise, because pandas column names are case-sensitive and columns like
    ``GEOID`` and ``geoid`` can legitimately coexist in the same file.
    """
    if name is None:
        return ""
    return str(name).strip()


def duplicate_column_values(columns: Iterable[Optional[str]]) -> Set[str]:
    """Column names claimed more than once (blank/``None`` entries ignored).

    Exposed for the GUI, which uses it to decide exactly which widgets get an
    error highlight.
    """
    seen: Set[str] = set()
    duplicates: Set[str] = set()
    for column in columns:
        name = _clean(column)
        if not name:
            continue
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    return duplicates


def _join_labels(labels: Sequence[str]) -> str:
    """``["a", "b", "c"]`` -> ``"a, b and c"``."""
    if len(labels) == 1:
        return labels[0]
    return ", ".join(labels[:-1]) + " and " + labels[-1]


def check_column_role_collisions(
    roles: Sequence[Tuple[str, Optional[str]]], *, scope: str
) -> List[str]:
    """Report every column that two or more *roles* point at.

    Parameters
    ----------
    roles : sequence of (label, column)
        ``label`` is the role's name in the user's vocabulary (it appears
        verbatim in the message), ``column`` the configured column name.
    scope : str
        Which input the roles belong to, e.g. ``"Survey data"``. Prefixes the
        message so a user with four files knows which one to fix.
    """
    problems: List[str] = []
    for column in _ordered_duplicates(roles):
        labels = [f"the {label}" for label, name in roles if _clean(name) == column]
        verb = "are both" if len(labels) == 2 else "are all"
        problems.append(
            f"{scope}: {_join_labels(labels)} {verb} set to {column!r}. "
            "Each role needs its own column."
        )
    return problems


def _ordered_duplicates(roles: Sequence[Tuple[str, Optional[str]]]) -> List[str]:
    """Duplicated column names, in order of first appearance in *roles*."""
    duplicates = duplicate_column_values(name for _label, name in roles)
    ordered: List[str] = []
    for _label, name in roles:
        column = _clean(name)
        if column in duplicates and column not in ordered:
            ordered.append(column)
    return ordered


# ----------------------------------------------------------------------
# Per-input role checks
# ----------------------------------------------------------------------
def check_survey_column_roles(
    date_col: Optional[str],
    id_col: Optional[str],
    geoid_col: Optional[str],
) -> List[str]:
    """The survey's date, ID and GEOID columns must be three distinct columns.

    All three roles are enforced even when residential history supplies the
    GEOIDs. The GEOID column looks unused in that mode, but its *name* still
    drives lag-column naming and the substring sweep that normalizes GEOID
    columns in the per-lag output — so a GEOID name colliding with the ID
    column rewrites the IDs as GEOIDs and the run fails later blaming the data.
    """
    return check_column_role_collisions(
        [
            ("date column", date_col),
            ("ID column", id_col),
            ("GEOID column", geoid_col),
        ],
        scope="Survey data",
    )


def check_residential_history_column_roles(
    id_col: Optional[str],
    date_col: Optional[str],
    geoid_col: Optional[str],
) -> List[str]:
    """The residential history's three columns must all be distinct."""
    return check_column_role_collisions(
        [
            ("ID column", id_col),
            ("move date column", date_col),
            ("GEOID column", geoid_col),
        ],
        scope="Residential history",
    )


def check_contextual_column_roles(
    date_col: Optional[str],
    geoid_col: Optional[str],
    data_cols: Optional[Iterable[str]],
) -> List[str]:
    """The contextual date, GEOID and measure columns must all be distinct.

    Also reports a measure column listed more than once: duplicate entries
    reach the readers as a duplicated ``usecols``, which either fails at load
    (Parquet/Feather/Stata) or silently produces duplicate labels (CSV).
    """
    columns = [_clean(c) for c in (data_cols or [])]
    columns = [c for c in columns if c]

    problems: List[str] = []
    counted: List[str] = []
    for column in columns:
        if columns.count(column) > 1 and column not in counted:
            counted.append(column)
            problems.append(
                f"Contextual data: the data column {column!r} is listed "
                f"{columns.count(column)} times. List each measure column only once."
            )

    unique_data_cols: List[str] = []
    for column in columns:
        if column not in unique_data_cols:
            unique_data_cols.append(column)

    problems.extend(
        check_column_role_collisions(
            [("date column", date_col), ("GEOID column", geoid_col)]
            + [("data column", c) for c in unique_data_cols],
            scope="Contextual data",
        )
    )
    return problems


# ----------------------------------------------------------------------
# Output path guards
# ----------------------------------------------------------------------
def _is_same_file(a: Path, b: Path) -> bool:
    """Whether two paths denote the same file.

    ``Path.samefile`` is the accurate test but needs both paths to exist, and
    the output file usually does not yet. The fallback resolves both and
    compares case-normalized strings, which is what makes this correct on
    Windows (case-insensitive, ``/`` and ``\\`` interchangeable).
    """
    try:
        if a.exists() and b.exists():
            return a.samefile(b)
    except OSError:
        pass
    try:
        return os.path.normcase(str(a.resolve())) == os.path.normcase(str(b.resolve()))
    except OSError:
        return False


def check_output_path_conflicts(
    output_path: Union[str, Path],
    survey_path: Optional[Union[str, Path]] = None,
    residential_history_path: Optional[Union[str, Path]] = None,
) -> List[str]:
    """The output file must not be one of the input files.

    The inputs are fully in memory by the time the output is written, so this
    otherwise succeeds and destroys the user's source data.
    """
    output_path = Path(output_path)
    problems: List[str] = []
    for label, candidate in (
        ("survey data file", survey_path),
        ("residential history file", residential_history_path),
    ):
        if not candidate:
            continue
        if _is_same_file(output_path, Path(candidate)):
            problems.append(
                f"The output file '{output_path}' is the same file as the {label}. "
                "Choose a different output file name or save directory so the "
                "input is not overwritten."
            )
    return problems


# ----------------------------------------------------------------------
# Generated column names
# ----------------------------------------------------------------------
def generated_column_names(
    *,
    date_col: Optional[str],
    geoid_col: Optional[str],
    data_cols: Optional[Iterable[str]],
    lags: Iterable[int],
    resolution: Union[str, LinkageResolution] = LinkageResolution.DAILY,
    post_lag_average: bool = False,
    start_lag: int = 0,
    max_lag: Optional[int] = None,
) -> List[str]:
    """Every column name a run with this configuration would create.

    Mirrors, and delegates to, the naming helpers that actually build them:
    ``HRSContextLinker._lag_date_colname`` / ``_lag_geoid_colname`` for the
    per-lag date and GEOID columns, ``{measure}_{lag date column}`` for the
    linked values, and ``{measure}_avg_{start}_{max}{unit}_prior`` for the
    post-lag average.

    The per-lag date and GEOID columns are listed regardless of
    ``include_lag_date``: they are always built on a copy of the survey frame,
    and it is that copy that a name collision breaks.
    """
    # Imported lazily: ``hrs`` imports this module for its constructor guards.
    from .hrs import HRSContextLinker

    res = LinkageResolution.from_str(resolution)
    lags = list(lags)
    date_col = _clean(date_col)
    geoid_col = _clean(geoid_col)
    measures = [c for c in (_clean(c) for c in (data_cols or [])) if c]

    names: List[str] = []
    for n in lags:
        lag_date_col = (
            HRSContextLinker._lag_date_colname(date_col, n, res) if date_col else ""
        )
        if lag_date_col:
            names.append(lag_date_col)
        if geoid_col:
            names.append(HRSContextLinker._lag_geoid_colname(geoid_col, n, res))
        if lag_date_col:
            names.extend(f"{measure}_{lag_date_col}" for measure in measures)

    if post_lag_average and measures:
        end = max_lag if max_lag is not None else max([start_lag, *lags])
        names.extend(
            f"{measure}_avg_{start_lag}_{end}{res.lag_unit}_prior"
            for measure in measures
        )
    return names


def check_generated_column_collisions(
    existing_columns: Iterable[str],
    *,
    max_examples: int = 5,
    **kwargs,
) -> List[str]:
    """Report generated column names that the survey data already carries.

    This is what a re-run on a previous STITCH output looks like. The lag
    builder concatenates its new columns onto the survey frame, so pre-existing
    names produce duplicate labels — which in the parallel path is swallowed
    per lag and leaves a run that reports success with nothing linked.

    Every collision is reported as a single message: a 366-lag re-run collides
    on more than a thousand columns.
    """
    # Not ``existing_columns or []``: a pandas Index has no truth value.
    existing = set() if existing_columns is None else set(existing_columns)
    if not existing:
        return []

    collisions: List[str] = []
    for name in generated_column_names(**kwargs):
        if name in existing and name not in collisions:
            collisions.append(name)
    if not collisions:
        return []

    if len(collisions) <= max_examples:
        listed = ", ".join(repr(c) for c in collisions)
        detail = f"already contains the column(s) {listed}"
    else:
        listed = ", ".join(repr(c) for c in collisions[:max_examples])
        detail = (
            f"already contains {len(collisions)} column(s) that STITCH would "
            f"create (e.g. {listed})"
        )
    return [
        f"The survey data {detail}. This usually means the survey file is the "
        "output of a previous STITCH run. Use the original survey file, or "
        "change the lag range."
    ]


# ----------------------------------------------------------------------
# Namespace-level entry points
# ----------------------------------------------------------------------
def _data_cols_from_args(args) -> Optional[List[str]]:
    """Measure columns as ``run_pipeline`` parses them; ``None`` when unknown."""
    data_col = getattr(args, "data_col", None)
    if data_col is None:
        return None
    if isinstance(data_col, str):
        return [c.strip() for c in data_col.split(",")]
    return [str(c).strip() for c in data_col]


def check_pipeline_args(args) -> List[str]:
    """Every configuration problem detectable without reading the data files.

    Reads optional attributes exactly the way ``run_pipeline`` does, so a
    partially-populated namespace validates rather than raising
    ``AttributeError``. Never mutates *args*: the namespace is hashed to match
    resumable temp directories, so normalizing it here would silently orphan
    every interrupted job.
    """
    problems: List[str] = []

    problems.extend(
        check_survey_column_roles(
            getattr(args, "date_col", None),
            getattr(args, "id_col", None),
            getattr(args, "geoid_col", None),
        )
    )

    if getattr(args, "residential_hist", None):
        problems.extend(
            check_residential_history_column_roles(
                getattr(args, "res_hist_id_col", None),
                getattr(args, "res_hist_date_col", None),
                getattr(args, "res_hist_geoid_col", None),
            )
        )

    problems.extend(
        check_contextual_column_roles(
            getattr(args, "context_date_col", None) or "Date",
            getattr(args, "contextual_geoid_col", None) or "GEOID10",
            _data_cols_from_args(args),
        )
    )

    save_dir = getattr(args, "save_dir", None)
    output_name = getattr(args, "output_name", None)
    if save_dir and output_name:
        problems.extend(
            check_output_path_conflicts(
                Path(save_dir) / Path(output_name),
                getattr(args, "survey_data", None),
                getattr(args, "residential_hist", None),
            )
        )

    return problems


def check_pipeline_survey_columns(args, survey_columns: Iterable[str]) -> List[str]:
    """Problems that need the survey's column list (generated-name collisions)."""
    data_cols = _data_cols_from_args(args)
    if data_cols is None:
        return []

    start_lag = int(getattr(args, "start_lag", 0) or 0)
    max_lag = int(getattr(args, "n_lags", 0) or 0) - 1
    if max_lag < start_lag:
        return []

    return check_generated_column_collisions(
        survey_columns,
        date_col=getattr(args, "date_col", None),
        geoid_col=getattr(args, "geoid_col", None),
        data_cols=data_cols,
        lags=range(start_lag, max_lag + 1),
        resolution=getattr(args, "linkage_resolution", "daily") or "daily",
        post_lag_average=bool(getattr(args, "post_lag_average", False)),
        start_lag=start_lag,
        max_lag=max_lag,
    )


def format_problems(problems: Sequence[str], *, header: str = PROBLEM_HEADER) -> str:
    """Render problems as one bulleted message."""
    return "\n".join([header, *(f"  - {problem}" for problem in problems)])


def validate_pipeline_args(args) -> None:
    """Raise ``ValueError`` listing every problem in *args*, or return quietly."""
    problems = check_pipeline_args(args)
    if problems:
        raise ValueError(format_problems(problems))


def validate_pipeline_survey_columns(args, survey_columns: Iterable[str]) -> None:
    """Raise ``ValueError`` if STITCH would overwrite existing survey columns."""
    problems = check_pipeline_survey_columns(args, survey_columns)
    if problems:
        raise ValueError(format_problems(problems))
