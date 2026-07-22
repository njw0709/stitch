from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from tqdm import tqdm

from .daily_measure import DailyMeasureDataDir
from .io_utils import (
    infer_datetime_series,
    normalize_geoid_for_processing,
    normalize_geoid_value_for_processing,
    read_data,
    write_data,
)
from .temporal import LinkageResolution


# ---------------------------------------------------------------------
# 1. ResidentialHistoryHRS
# ---------------------------------------------------------------------
class ResidentialHistoryHRS:
    """
    Parses respondent-level residential move history and enables date-based
    GEOID lookup for linkage with contextual datasets.

    Expects a simple long-format table with one row per residence:

    * ``id_col``    — participant ID
    * ``date_col``  — when the participant started living at that location.
      The earliest entry per person is treated as their entry into the survey.
      The format is inferred per value (dates, "YYYY-MM", month names,
      numeric YYYY / YYYYMM / YYYYMMDD, ...); values coarser than the finest
      resolution are anchored to the midpoint of the period they span
      (e.g. 2013 → mid-year 2013-07-02 12:00).
    * ``geoid_col`` — location identifier for that residence
    """

    def __init__(
        self,
        filename: Union[str, Path],
        id_col: str = "hhidpn",
        date_col: str = "move_date",
        geoid_col: str = "GEOID",
        geoid_n_digits: int = 11,
        geoid_treatment: str = "code",
        geoid_numeric_type: str = "int",
    ):
        self.filename = Path(filename)
        self.id_col = id_col
        self.date_col = date_col
        self.geoid_col = geoid_col
        self.geoid_n_digits = geoid_n_digits
        self.geoid_treatment = geoid_treatment
        self.geoid_numeric_type = geoid_numeric_type

        # Load only once (file read can be expensive)
        self.df = read_data(self.filename)
        missing = [c for c in (id_col, date_col, geoid_col) if c not in self.df.columns]
        if missing:
            raise ValueError(
                f"Residential history file {self.filename} is missing "
                f"column(s) {missing}. Available columns: {list(self.df.columns)}"
            )
        # Normalize identifier type to integer (nullable) for consistent keying
        self.df[self.id_col] = pd.to_numeric(
            self.df[self.id_col], errors="coerce"
        ).astype("Int64")
        self._move_info = self._parse_move_info()

    def _parse_move_info(self) -> Dict[int, tuple[list[pd.Timestamp], list[str]]]:
        """
        Builds a dict mapping participant ID → (list of move dates, list of
        corresponding GEOIDs), sorted chronologically. The earliest entry is
        the participant's residence at survey entry.
        """
        print("📌 Parsing residential move history...")
        parsed_dates = infer_datetime_series(self.df[self.date_col])

        unparseable = parsed_dates.isna() & self.df[self.date_col].notna()
        if unparseable.any():
            examples = self.df.loc[unparseable, self.date_col].unique()[:5]
            print(
                f"⚠️  {int(unparseable.sum())} value(s) in {self.date_col!r} could "
                f"not be parsed as dates and were skipped. Examples: {list(examples)}"
            )
        if parsed_dates.isna().all():
            raise ValueError(
                f"No value in column {self.date_col!r} could be parsed as a date. "
                "Supported formats include dates (2010-03-15), year-month "
                "(2010-03), month names (March 2010), and numeric "
                "YYYY / YYYYMM / YYYYMMDD."
            )

        rows = self.df[[self.id_col, self.geoid_col]].copy()
        rows["_move_dt"] = parsed_dates
        rows = rows.dropna(subset=[self.id_col, "_move_dt"])

        move_info = {}
        for pid, df_person in tqdm(rows.groupby(self.id_col)):
            df_person = df_person.sort_values("_move_dt", kind="stable")
            geoids = [
                normalize_geoid_value_for_processing(
                    g,
                    treatment=self.geoid_treatment,
                    n_digits=self.geoid_n_digits,
                    numeric_type=self.geoid_numeric_type,
                )
                for g in df_person[self.geoid_col]
            ]
            move_info[int(pid)] = (list(df_person["_move_dt"]), geoids)
        debug = self.debug_move_info(move_info)
        print("Residential history parsed! Debug: {}".format(debug))
        return move_info

    def debug_move_info(self, move_info=None, n_samples: int = 5) -> dict:
        """
        Inspect _move_info contents for debugging.

        Parameters
        ----------
        move_info : dict, optional
            Move info dict to inspect. Defaults to self._move_info.
        n_samples : int
            Number of sample entries to include.

        Returns dict with:
        - key_count: number of keys in _move_info
        - key_types: types of keys
        - sample_keys: sample of keys
        - sample_entries: sample entries with dates/geoids
        """
        if move_info is None:
            move_info = self._move_info

        keys = list(move_info.keys())
        key_types = set(type(k).__name__ for k in keys)

        sample_keys = keys[:n_samples]
        sample_entries = {}
        for k in sample_keys:
            dates, geoids = move_info[k]
            sample_entries[k] = {
                "num_dates": len(dates),
                "first_date": str(dates[0]) if dates else None,
                "first_geoid": geoids[0] if geoids else None,
            }

        return {
            "key_count": len(keys),
            "key_types": list(key_types),
            "sample_keys": sample_keys,
            "sample_entries": sample_entries,
        }

    @staticmethod
    def _find_geoid_for_date(
        dt: pd.Timestamp, move_dates: list[pd.Timestamp], move_geoids: list[str]
    ) -> Optional[str]:
        """Return geoid for dt, or None if dt is earlier than first recorded move."""
        if dt < move_dates[0]:
            return None  # or pd.NA if you prefer pandas NA semantics

        if len(move_dates) == 1:
            return move_geoids[0]

        for i, move_dt in enumerate(move_dates):
            if move_dt > dt:
                return move_geoids[i - 1]

        return move_geoids[-1]

    def create_geoid_based_on_date(
        self, hhidpn_series: pd.Series, date_series: pd.Series, debug: bool = False
    ) -> pd.Series:
        """
        Returns a Series of GEOIDs aligned with hhidpn_series,
        based on the move history and the provided dates.

        If a person ID is not found in the residential history,
        returns NaN for that person's GEOID.

        Parameters
        ----------
        hhidpn_series : pd.Series
            Series of person IDs
        date_series : pd.Series
            Series of dates to look up GEOIDs for
        debug : bool, optional
            If True, print debug information about the lookup process
        """
        assert len(hhidpn_series) == len(date_series)
        geoids = []
        # Ensure lookup series is integer-typed (nullable) to match keys
        pid_series_int = pd.to_numeric(hhidpn_series, errors="coerce").astype("Int64")

        if debug:
            print(f"🔍 Debug Info for create_geoid_based_on_date:")
            print(
                f"  Input PIDs: {len(hhidpn_series)} total, {pid_series_int.nunique()} unique"
            )
            print(f"  _move_info keys: {len(self._move_info)} total")
            print(
                f"  Key types in _move_info: {set(type(k).__name__ for k in list(self._move_info.keys())[:5])}"
            )
            print(f"  Sample input PIDs (first 5): {list(pid_series_int[:5])}")
            print(
                f"  Sample _move_info keys (first 5): {list(self._move_info.keys())[:5]}"
            )

            # Check how many PIDs will be found
            found_count = sum(
                1
                for pid in pid_series_int
                if not pd.isna(pid) and int(pid) in self._move_info
            )
            print(f"  PIDs that will be found: {found_count}/{len(pid_series_int)}")

            # Sample of PIDs not found
            not_found_pids = [
                int(pid)
                for pid in pid_series_int[:10]
                if not pd.isna(pid) and int(pid) not in self._move_info
            ]
            if not_found_pids:
                print(f"  Sample PIDs not found (first 5): {not_found_pids[:5]}")

        for pid, dt in zip(pid_series_int, date_series):
            if pd.isna(pid):
                geoids.append(None)
                continue
            pid_key = int(pid)
            if pid_key not in self._move_info:
                # Person not found in residential history - return NaN
                geoids.append(None)
            else:
                move_dates, move_geoids = self._move_info[pid_key]
                geoids.append(self._find_geoid_for_date(dt, move_dates, move_geoids))
        return pd.Series(geoids, index=hhidpn_series.index, dtype="string")


# ---------------------------------------------------------------------
# 2. HRSEpigenetics
# ---------------------------------------------------------------------
class HRSInterviewData:
    """
    Wrapper around survey data with interview (or blood collection date
    for epigenetic biomarker data (e.g., HRS VBS)).
    adding date-based GEOID creation for linkage with contextual data.
    """

    def __init__(
        self,
        filename: Union[str, Path],
        datecol: str = "bcdate",
        move: bool = True,
        residential_hist: Optional[ResidentialHistoryHRS] = None,
        hhidpn: str = "hhidpn",
        geoid_col: str = "GEOID2010",
        geoid_n_digits: int = 11,
        geoid_treatment: str = "code",
        geoid_numeric_type: str = "int",
        linkage_resolution: Union[str, LinkageResolution] = LinkageResolution.DAILY,
    ):
        self.filename = Path(filename)
        self.df = read_data(self.filename)
        self.columns = self.df.columns
        assert datecol in self.columns, f"Date column `{datecol}` not in data!"

        self.datecol = datecol
        self.hhidpn = hhidpn
        self.move = move
        self.residential_hist = residential_hist
        self.geoid_col = geoid_col
        self.geoid_n_digits = geoid_n_digits
        self.geoid_treatment = geoid_treatment
        self.geoid_numeric_type = geoid_numeric_type
        self.linkage_resolution = LinkageResolution.from_str(linkage_resolution)

        # Normalize the interview/reference date column to datetime. The format
        # is inferred per value so coarse values (year-only, year-month) are
        # anchored to their period midpoint (e.g. 2013-03 -> 2013-03-16 12:00)
        # and numeric YYYY / YYYYMM / YYYYMMDD are handled, consistent with how
        # residential-history move dates are parsed.
        if datecol in self.df.columns:
            self.df[datecol] = infer_datetime_series(self.df[datecol])

        # Normalize identifier type to integer (nullable) for consistent joins/lookups
        if self.hhidpn in self.df.columns:
            self.df[self.hhidpn] = pd.to_numeric(
                self.df[self.hhidpn], errors="coerce"
            ).astype("Int64")

        # Format the GEOID column if it exists and no residential history
        if not move and geoid_col in self.columns:
            self.df[geoid_col] = normalize_geoid_for_processing(
                self.df[geoid_col],
                treatment=self.geoid_treatment,
                n_digits=self.geoid_n_digits,
                numeric_type=self.geoid_numeric_type,
            )

    def get_geoid_based_on_date(self, date_series: pd.Series) -> pd.Series:
        return self.residential_hist.create_geoid_based_on_date(
            self.df[self.hhidpn],
            date_series,
        )

    def save(self, save_name: Union[str, Path]) -> None:
        geoid_cols = [c for c in self.df.columns if self.geoid_col in c]
        df_to_save = self.df.copy()
        for col in geoid_cols:
            if col in df_to_save.columns:
                df_to_save[col] = normalize_geoid_for_processing(
                    df_to_save[col],
                    treatment=self.geoid_treatment,
                    n_digits=self.geoid_n_digits,
                    numeric_type=self.geoid_numeric_type,
                )
        write_data(df_to_save, save_name)


# ---------------------------------------------------------------------
# 3. HRSContextLinker
# ---------------------------------------------------------------------


class HRSContextLinker:
    """
    Handles temporal/geographic alignment between HRS epigenetic data
    and contextual daily measure data (e.g., heat index, Tmax, PM2.5),
    including:
    - n-day prior date column creation
    - GEOID column assignment based on residential history or static data
    - Single or batch merging with contextual data sources
    - Outputting merged columns for parallel workflows
    """

    # ------------------------------------------------------------------
    # Lag column naming helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _lag_suffix(n: int, resolution=LinkageResolution.DAILY) -> str:
        unit = LinkageResolution.from_str(resolution).lag_unit
        return f"{n}{unit}_prior"

    @staticmethod
    def _lag_date_colname(
        datecol: str, n: int, resolution=LinkageResolution.DAILY
    ) -> str:
        return f"{datecol}_{HRSContextLinker._lag_suffix(n, resolution)}"

    @staticmethod
    def _lag_geoid_colname(
        geoid_col: str, n: int, resolution=LinkageResolution.DAILY
    ) -> str:
        return f"{geoid_col}_{HRSContextLinker._lag_suffix(n, resolution)}"

    @staticmethod
    def _lag_n_from_date_col(
        lag_date_col: str, datecol: str, resolution=LinkageResolution.DAILY
    ) -> int:
        unit = LinkageResolution.from_str(resolution).lag_unit
        prefix = f"{datecol}_"
        suffix = f"{unit}_prior"
        if not lag_date_col.startswith(prefix) or not lag_date_col.endswith(suffix):
            raise ValueError(
                f"Lag date column {lag_date_col!r} does not match expected "
                f"pattern {datecol!r}_{{n}}{unit}_prior"
            )
        return int(lag_date_col[len(prefix) : -len(suffix)])

    # Backward-compatible alias (daily-only).
    @staticmethod
    def _lag_days_from_date_col(lag_date_col: str, datecol: str) -> int:
        return HRSContextLinker._lag_n_from_date_col(
            lag_date_col, datecol, LinkageResolution.DAILY
        )

    @staticmethod
    def _hrs_resolution(hrs_data: "HRSInterviewData") -> LinkageResolution:
        return getattr(hrs_data, "linkage_resolution", LinkageResolution.DAILY)

    # ------------------------------------------------------------------
    # 1. n-day prior date column
    # ------------------------------------------------------------------
    @staticmethod
    def make_n_day_prior_cols(hrs_data: "HRSInterviewData", n_day_prior: int) -> str:
        """
        Create a new column representing the date n days prior to the
        respondent's reference date column.
        """
        res = HRSContextLinker._hrs_resolution(hrs_data)
        colname = HRSContextLinker._lag_date_colname(hrs_data.datecol, n_day_prior, res)
        lag = hrs_data.df[hrs_data.datecol] - res.offset(n_day_prior)
        hrs_data.df[colname] = res.floor(lag)
        return colname

    # ------------------------------------------------------------------
    # 1b. Batch column preparation
    # ------------------------------------------------------------------
    @staticmethod
    def prepare_lag_columns_batch(
        hrs_data: "HRSInterviewData",
        n_days: List[int],
        geoid_col: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Pre-create all n-day-prior date and GEOID columns for multiple lags.
        Returns DataFrame with ID, original date, and all lag date/GEOID columns.

        This is more efficient than creating columns one at a time because:
        - All date columns are vectorized operations on the same base date
        - All GEOID lookups happen in a single pass
        - Results can be reused for multiple merges

        Parameters
        ----------
        hrs_data : HRSInterviewData
            HRS interview or epigenetic data object
        n_days : List[int]
            List of lag periods (in days) to create columns for
        geoid_col : str, optional
            Name of the GEOID column in HRS data

        Returns
        -------
        pd.DataFrame
            DataFrame with all original columns plus date/GEOID columns for each lag
        """
        # Start with copy of HRS data
        result_df = hrs_data.df.copy()
        res = HRSContextLinker._hrs_resolution(hrs_data)

        # Collect all new columns to avoid fragmentation
        new_columns = {}

        # Create date columns for all lags
        for n in tqdm(n_days, desc="Creating date columns", unit="lag"):
            date_colname = HRSContextLinker._lag_date_colname(hrs_data.datecol, n, res)
            lag = result_df[hrs_data.datecol] - res.offset(n)
            new_columns[date_colname] = res.floor(lag)

        # Create GEOID columns for all lags using the helper method
        if geoid_col is None:
            geoid_col = hrs_data.geoid_col

        for n in tqdm(n_days, desc="Creating GEOID columns", unit="lag"):
            date_colname = HRSContextLinker._lag_date_colname(hrs_data.datecol, n, res)
            geoid_colname = HRSContextLinker._lag_geoid_colname(geoid_col, n, res)

            # Use helper method to compute GEOIDs
            new_columns[geoid_colname] = HRSContextLinker._compute_geoid_for_date(
                hrs_data, new_columns[date_colname], geoid_col
            )

        # Concatenate all new columns at once to avoid fragmentation
        new_cols_df = pd.DataFrame(new_columns, index=result_df.index)
        result_df = pd.concat([result_df, new_cols_df], axis=1)

        return result_df

    # ------------------------------------------------------------------
    # 2. Geoid assignment for lag date
    # ------------------------------------------------------------------
    @staticmethod
    def _compute_geoid_for_date(
        hrs_data: "HRSInterviewData",
        date_series: pd.Series,
        geoid_col: Optional[str] = None,
    ) -> pd.Series:
        """
        Compute GEOID values for a given date series.

        Returns the GEOID Series without modifying any DataFrame.
        """
        if hrs_data.move:
            # Use residential history for dynamic lookup
            geoids = hrs_data.get_geoid_based_on_date(date_series)
        else:
            # Use the specified static GEOID column directly
            if geoid_col is None:
                geoid_col = hrs_data.geoid_col
            geoids = normalize_geoid_for_processing(
                hrs_data.df[geoid_col],
                treatment=hrs_data.geoid_treatment,
                n_digits=hrs_data.geoid_n_digits,
                numeric_type=hrs_data.geoid_numeric_type,
            )
        return geoids

    @staticmethod
    def make_geoid_day_prior(
        hrs_data: "HRSInterviewData",
        merge_date_col: str,
        geoid_col: Optional[str] = None,
        df: Optional[pd.DataFrame] = None,
    ) -> str:
        """
        Create a geoid column based on a lagged date column.
        If df is provided, operate on that DataFrame instead of hrs_data.df.
        """
        if geoid_col is None:
            geoid_col = hrs_data.geoid_col
        target_df = hrs_data.df if df is None else df
        res = HRSContextLinker._hrs_resolution(hrs_data)
        n = HRSContextLinker._lag_n_from_date_col(merge_date_col, hrs_data.datecol, res)
        colname = HRSContextLinker._lag_geoid_colname(geoid_col, n, res)

        # Compute GEOIDs using helper method
        geoids = HRSContextLinker._compute_geoid_for_date(
            hrs_data, target_df[merge_date_col], geoid_col
        )
        target_df[colname] = geoids

        return colname

    # ------------------------------------------------------------------
    # 3. Merge HRS with contextual data (single big merge)
    # ------------------------------------------------------------------
    @staticmethod
    def merge_with_contextual_data(
        hrs_data: "HRSInterviewData",
        contextual_dir: DailyMeasureDataDir,
        left_on: List[str],
        drop_left: bool = True,
    ) -> "HRSInterviewData":
        """
        Merge HRS data with contextual daily data across all years in a single merge.
        This is typically faster than looping year by year.
        """
        res = HRSContextLinker._hrs_resolution(hrs_data)
        date_col = left_on[0]
        n = HRSContextLinker._lag_n_from_date_col(date_col, hrs_data.datecol, res)
        nday_prior_str = HRSContextLinker._lag_suffix(n, res)

        # Build one contextual DataFrame from all years
        years = contextual_dir.list_years()
        contextual_df = pd.concat([contextual_dir[yr].df for yr in years], axis=0)
        first_context = contextual_dir[years[0]]
        right_on = [first_context.date_col, first_context.geoid_col]
        # Align contextual timestamps to the linkage period key so the exact
        # merge lines up with the (also floored) survey lag dates.
        contextual_df = contextual_df.copy()
        contextual_df[first_context.date_col] = res.floor(
            contextual_df[first_context.date_col]
        )

        # Check for overlapping columns
        overlap = set(hrs_data.df.columns) & set(contextual_df.columns) - set(right_on)
        if overlap:
            raise ValueError(f"Column overlap during merge: {overlap}")

        merged = pd.merge(
            hrs_data.df,
            contextual_df,
            how="left",
            left_on=left_on,
            right_on=right_on,
            suffixes=(None, None),
        )

        # Drop key columns if needed
        merged.drop(right_on, axis=1, inplace=True)
        if drop_left:
            merged.drop(left_on[1:], axis=1, inplace=True)

        # Rename contextual measure column to indicate lag
        data_col = first_context.data_col
        merged.rename(columns={data_col: f"{data_col}_{nday_prior_str}"}, inplace=True)

        hrs_data.df = merged
        return hrs_data

    # ------------------------------------------------------------------
    # 4. Output merged columns for a specific lag (no mutation)
    # ------------------------------------------------------------------
    @staticmethod
    def build_contextual_lookup(
        contextual_df: pd.DataFrame,
        contextual_date_col: str,
        contextual_geoid_col: str,
        contextual_data_col: Union[str, List[str]],
    ) -> pd.DataFrame:
        """Build a ``(date, geoid)``-indexed lookup table for fast per-lag joins.

        The expensive hash over the contextual data is built exactly once here;
        each lag then reuses it via a cheap :meth:`pandas.DataFrame.reindex`
        instead of re-hashing the whole contextual table in a fresh
        :func:`pandas.merge` (the previous per-lag behaviour). See
        :meth:`output_merged_columns`.

        The index must be unique for ``reindex`` to be well-defined; duplicate
        ``(date, geoid)`` pairs are dropped (keeping the first), mirroring the
        de-duplication the batch/parallel drivers already perform upstream.

        Parameters
        ----------
        contextual_df : pd.DataFrame
            Pre-loaded, filtered contextual data (concatenated across years).
        contextual_date_col, contextual_geoid_col : str
            Key column names in *contextual_df*.
        contextual_data_col : str or List[str]
            Measure column(s) to retain as lookup values.

        Returns
        -------
        pd.DataFrame
            Frame indexed by ``[contextual_date_col, contextual_geoid_col]`` with
            only the measure column(s) as data.
        """
        if isinstance(contextual_data_col, str):
            contextual_data_col = [contextual_data_col]

        keys = [contextual_date_col, contextual_geoid_col]
        lookup = contextual_df[keys + list(contextual_data_col)]
        # ``reindex`` requires a unique index; defensively drop any duplicate
        # key pairs (upstream drivers already dedupe, this makes it safe anywhere).
        if lookup.duplicated(subset=keys).any():
            lookup = lookup.drop_duplicates(subset=keys, keep="first")
        return lookup.set_index(keys)

    @staticmethod
    def output_merged_columns(
        hrs_data: "HRSInterviewData",
        n: int,
        id_col: str,
        precomputed_lag_df: pd.DataFrame,
        contextual_lookup: pd.DataFrame,
        contextual_data_col: Union[str, List[str]],
        include_lag_date: bool = False,
        geoid_col: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        For a specific lag n, join pre-computed lag columns with contextual data.

        Each lag is resolved with a single vectorized ``reindex`` against the
        pre-built ``(date, geoid)`` index in *contextual_lookup* (from
        :meth:`build_contextual_lookup`), so the contextual hash is built once for
        *all* lags rather than rebuilt on every lag. This is dramatically faster
        than a per-lag :func:`pandas.merge` when the number of lags is large.

        Parameters
        ----------
        hrs_data : HRSInterviewData
            HRS interview or epigenetic data object (used for metadata like datecol)
        n : int
            Lag period in days
        id_col : str
            Unique identifier column for joining (e.g., "hhidpn")
        precomputed_lag_df : pd.DataFrame
            Pre-computed DataFrame with date and GEOID columns for all lags.
            Should contain: id_col, {datecol}_{n}day_prior, {geoid_col}_{n}day_prior
        contextual_lookup : pd.DataFrame
            ``(date, geoid)``-indexed lookup from :meth:`build_contextual_lookup`,
            whose columns are the measure column(s).
        contextual_data_col : str or List[str]
            Name(s) of data/measure column(s) in contextual data (e.g., 'tmax', 'pm25', or ['tmax', 'pm25'])
        include_lag_date : bool, default False
            Whether to include the lagged date column in the output
        geoid_col : str, optional
            Name of the GEOID column in HRS data

        Returns
        -------
        pd.DataFrame
            DataFrame with ID, optionally lag date, and merged contextual column
        """
        if geoid_col is None:
            geoid_col = hrs_data.geoid_col

        res = HRSContextLinker._hrs_resolution(hrs_data)

        # Normalize contextual_data_col to list
        if isinstance(contextual_data_col, str):
            contextual_data_col = [contextual_data_col]

        # Extract pre-computed lag columns
        n_day_colname = HRSContextLinker._lag_date_colname(hrs_data.datecol, n, res)
        n_day_geoid_colname = HRSContextLinker._lag_geoid_colname(geoid_col, n, res)

        hrs_copy = precomputed_lag_df[
            [id_col, n_day_colname, n_day_geoid_colname]
        ].copy()

        # If no valid geoid, return empty contextual column
        if hrs_copy[n_day_geoid_colname].isna().all():
            out_cols = [id_col]
            if include_lag_date:
                out_cols.append(n_day_colname)
                out_cols.append(n_day_geoid_colname)
            return hrs_copy[out_cols]

        # Output column names (one per measure), suffixed with the lag date col.
        new_col_names = [f"{col}_{n_day_colname}" for col in contextual_data_col]

        # Single vectorized reindex against the pre-built (date, geoid) index.
        keys = pd.MultiIndex.from_arrays(
            [hrs_copy[n_day_colname], hrs_copy[n_day_geoid_colname]]
        )
        looked_up = contextual_lookup.reindex(keys)

        data = {id_col: hrs_copy[id_col].to_numpy()}
        if include_lag_date:
            data[n_day_colname] = hrs_copy[n_day_colname].to_numpy()
            data[n_day_geoid_colname] = hrs_copy[n_day_geoid_colname].to_numpy()
        for src, dst in zip(contextual_data_col, new_col_names):
            data[dst] = looked_up[src].to_numpy()
        return pd.DataFrame(data)
