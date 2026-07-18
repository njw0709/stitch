"""Tests for flexible datetime inference used by residential history parsing."""

import pandas as pd
import pytest

from stitch.io_utils import infer_datetime_series


class TestNumericInference:
    def test_year_only_int_anchors_to_midyear(self):
        result = infer_datetime_series(pd.Series([2013]))
        assert result.iloc[0] == pd.Timestamp("2013-07-02 12:00:00")

    def test_year_only_leap_year_midpoint(self):
        # 2016 spans 366 days -> exact midpoint is Jul 2 at 00:00
        result = infer_datetime_series(pd.Series([2016]))
        assert result.iloc[0] == pd.Timestamp("2016-07-02 00:00:00")

    def test_year_month_int_anchors_to_midmonth(self):
        # 2013-02 spans 28 days -> midpoint is the 15th at 00:00
        result = infer_datetime_series(pd.Series([201302]))
        assert result.iloc[0] == pd.Timestamp("2013-02-15 00:00:00")

    def test_year_month_int_31_day_month(self):
        # 2013-03 spans 31 days -> midpoint is the 16th at 12:00
        result = infer_datetime_series(pd.Series([201303]))
        assert result.iloc[0] == pd.Timestamp("2013-03-16 12:00:00")

    def test_full_date_int_anchors_to_noon(self):
        result = infer_datetime_series(pd.Series([20130310]))
        assert result.iloc[0] == pd.Timestamp("2013-03-10 12:00:00")

    def test_float_year(self):
        result = infer_datetime_series(pd.Series([2013.0]))
        assert result.iloc[0] == pd.Timestamp("2013-07-02 12:00:00")

    def test_invalid_month_is_nat(self):
        # 201399 -> month 99 is invalid
        result = infer_datetime_series(pd.Series([201399]))
        assert pd.isna(result.iloc[0])

    def test_out_of_range_number_is_nat(self):
        result = infer_datetime_series(pd.Series([999]))
        assert pd.isna(result.iloc[0])


class TestStringInference:
    def test_year_only_string(self):
        result = infer_datetime_series(pd.Series(["2013"]))
        assert result.iloc[0] == pd.Timestamp("2013-07-02 12:00:00")

    def test_year_month_string(self):
        result = infer_datetime_series(pd.Series(["2013-03"]))
        assert result.iloc[0] == pd.Timestamp("2013-03-16 12:00:00")

    def test_iso_date_string_anchors_to_noon(self):
        result = infer_datetime_series(pd.Series(["2013-03-10"]))
        assert result.iloc[0] == pd.Timestamp("2013-03-10 12:00:00")

    def test_month_name_string(self):
        result = infer_datetime_series(pd.Series(["March 2010"]))
        assert result.iloc[0] == pd.Timestamp("2010-03-16 12:00:00")

    def test_stata_style_date_string(self):
        result = infer_datetime_series(pd.Series(["21sep2018"]))
        assert result.iloc[0] == pd.Timestamp("2018-09-21 12:00:00")

    def test_datetime_with_time_preserved(self):
        result = infer_datetime_series(pd.Series(["2013-03-10 14:23:00"]))
        assert result.iloc[0] == pd.Timestamp("2013-03-10 14:23:00")

    def test_garbage_is_nat(self):
        result = infer_datetime_series(pd.Series(["not a date"]))
        assert pd.isna(result.iloc[0])

    def test_empty_and_none_are_nat(self):
        result = infer_datetime_series(pd.Series(["", None]))
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])


class TestMixedAndDatetime:
    def test_datetime_dtype_passthrough(self):
        s = pd.to_datetime(pd.Series(["2013-05-01", "2014-06-02"]))
        result = infer_datetime_series(s)
        assert result.iloc[0] == pd.Timestamp("2013-05-01")
        assert result.iloc[1] == pd.Timestamp("2014-06-02")

    def test_mixed_formats_in_one_series(self):
        s = pd.Series(["2013", "2014-03", "2015-06-10", "March 2016"])
        result = infer_datetime_series(s)
        assert result.iloc[0] == pd.Timestamp("2013-07-02 12:00:00")
        assert result.iloc[1] == pd.Timestamp("2014-03-16 12:00:00")
        assert result.iloc[2] == pd.Timestamp("2015-06-10 12:00:00")
        assert result.iloc[3] == pd.Timestamp("2016-03-16 12:00:00")

    def test_ordering_preserved_for_sorting(self):
        # Coarser and finer values must still sort chronologically
        s = pd.Series(["2013", "2013-03", "2012-12-31"])
        result = infer_datetime_series(s)
        assert result.iloc[2] < result.iloc[1] < result.iloc[0]
