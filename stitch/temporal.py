"""
Temporal resolution primitives for linkage.

Linkage happens at one of three ordered resolutions::

    hourly  <  daily  <  monthly      (finest -> coarsest)

The chosen ``LinkageResolution`` determines the lag unit (hours / days /
months), the canonical "period key" used to align survey and contextual
timestamps for the exact merge, and — when the requested resolution is coarser
than the contextual data — how the contextual data is aggregated up.
"""

from __future__ import annotations

from enum import Enum
from typing import Iterable, Optional

import pandas as pd


class LinkageResolution(str, Enum):
    """Ordered temporal resolution for linkage."""

    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"

    # ------------------------------------------------------------------
    # Ordering
    # ------------------------------------------------------------------
    @property
    def order(self) -> int:
        """Integer rank; smaller is finer (hourly=0, daily=1, monthly=2)."""
        return {"hourly": 0, "daily": 1, "monthly": 2}[self.value]

    def is_finer_than(self, other: "LinkageResolution") -> bool:
        return self.order < other.order

    def is_coarser_than(self, other: "LinkageResolution") -> bool:
        return self.order > other.order

    @classmethod
    def from_str(cls, value) -> "LinkageResolution":
        """Coerce a string (case-insensitive) into a ``LinkageResolution``."""
        if isinstance(value, cls):
            return value
        return cls(str(value).strip().lower())

    # ------------------------------------------------------------------
    # Unit / labels
    # ------------------------------------------------------------------
    @property
    def lag_unit(self) -> str:
        """Singular unit label used in column suffixes and the GUI ("hour"/"day"/"month")."""
        return {"hourly": "hour", "daily": "day", "monthly": "month"}[self.value]

    @property
    def label(self) -> str:
        """Human-friendly capitalized label ("Hourly"/"Daily"/"Monthly")."""
        return self.value.capitalize()

    # ------------------------------------------------------------------
    # Timestamp math
    # ------------------------------------------------------------------
    def offset(self, n: int):
        """Return the offset to subtract for an ``n``-unit lag at this resolution."""
        if self is LinkageResolution.HOURLY:
            return pd.to_timedelta(n, unit="h")
        if self is LinkageResolution.DAILY:
            return pd.to_timedelta(n, unit="D")
        return pd.DateOffset(months=n)

    def floor(self, series: pd.Series) -> pd.Series:
        """Normalize timestamps to this resolution's canonical period key.

        - hourly  -> floored to the hour
        - daily   -> midnight of the day
        - monthly -> first day of the month (month start)
        """
        s = pd.to_datetime(series)
        if self is LinkageResolution.HOURLY:
            return s.dt.floor("h")
        if self is LinkageResolution.DAILY:
            return s.dt.normalize()
        return s.dt.to_period("M").dt.to_timestamp()

    def max_lag_days(self, n_units: int) -> int:
        """Upper bound (in days) of an ``n_units`` lag window, for year selection.

        Months are approximated at 31 days each so that year loading never drops
        a needed year.
        """
        if self is LinkageResolution.HOURLY:
            return max(1, (n_units + 23) // 24)
        if self is LinkageResolution.DAILY:
            return n_units
        return n_units * 31


class AggMethod(str, Enum):
    """How contextual data is reconciled when coarsened to a coarser resolution."""

    AVERAGE = "average"
    MIDPOINT = "midpoint"

    @classmethod
    def from_str(cls, value) -> "AggMethod":
        if isinstance(value, cls):
            return value
        return cls(str(value).strip().lower())


# ----------------------------------------------------------------------
# Resolution inference
# ----------------------------------------------------------------------

def _all_month_filenames(filenames: Iterable[str]) -> bool:
    """True if every filename carries an explicit ``YYYY_MM`` month token."""
    from .daily_measure import DailyMeasureDataDir

    names = list(filenames)
    if not names:
        return False
    for name in names:
        try:
            _, month = DailyMeasureDataDir._parse_period(str(name))
        except ValueError:
            return False
        if month is None:
            return False
    return True


def infer_temporal_resolution(
    series: pd.Series,
    filenames: Optional[Iterable[str]] = None,
) -> LinkageResolution:
    """Infer the temporal resolution of a timestamp column.

    Rules:
    - Any sub-day time-of-day component (varying hours, or non-zero
      hour/minute/second) -> ``hourly``.
    - Otherwise, if any calendar month contains more than one distinct day of
      data -> ``daily``.
    - Otherwise (at most one day per month) -> ``monthly``.

    ``filenames`` with explicit ``YYYY_MM`` tokens are used as a tiebreaker in
    favor of ``monthly`` when the timestamps alone are inconclusive (empty or
    one-day-per-month).
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        s = pd.to_datetime(series)
    else:
        s = pd.to_datetime(series, errors="coerce", format="mixed")
    s = s.dropna()

    if len(s) == 0:
        if filenames is not None and _all_month_filenames(filenames):
            return LinkageResolution.MONTHLY
        return LinkageResolution.DAILY

    # Sub-day component -> hourly.
    if (
        s.dt.hour.nunique() > 1
        or (s.dt.hour != 0).any()
        or (s.dt.minute != 0).any()
        or (s.dt.second != 0).any()
    ):
        return LinkageResolution.HOURLY

    # Daily vs monthly: how many distinct days occur within any single month?
    frame = pd.DataFrame({"y": s.dt.year, "m": s.dt.month, "d": s.dt.day})
    max_days_per_month = frame.groupby(["y", "m"])["d"].nunique().max()
    if max_days_per_month > 1:
        return LinkageResolution.DAILY

    # At most one day per month -> monthly.
    return LinkageResolution.MONTHLY
