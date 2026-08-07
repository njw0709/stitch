"""
Unit tests for the shared configuration validators in ``stitch.validation``.

Pure functions only — no Qt, no data files.
"""

import argparse
import os
from pathlib import Path

import pytest

from stitch.validation import (
    check_contextual_column_roles,
    check_generated_column_collisions,
    check_output_path_conflicts,
    check_pipeline_args,
    check_pipeline_survey_columns,
    check_residential_history_column_roles,
    check_survey_column_roles,
    duplicate_column_values,
    generated_column_names,
    validate_pipeline_args,
)


class TestDuplicateColumnValues:
    def test_finds_repeats(self):
        assert duplicate_column_values(["a", "a", "b"]) == {"a"}

    def test_ignores_blanks_and_none(self):
        assert duplicate_column_values([None, "", "   ", None, ""]) == set()

    def test_strips_whitespace(self):
        assert duplicate_column_values([" a", "a "]) == {"a"}

    def test_distinct_columns(self):
        assert duplicate_column_values(["a", "b", "c"]) == set()


class TestSurveyColumnRoles:
    def test_valid_config(self):
        assert check_survey_column_roles("iwdate", "hhidpn", "GEOID2010") == []

    def test_id_equals_date(self):
        problems = check_survey_column_roles("iwdate", "iwdate", "GEOID2010")
        assert len(problems) == 1
        assert "Survey data" in problems[0]
        assert "date column" in problems[0]
        assert "ID column" in problems[0]
        assert "'iwdate'" in problems[0]

    def test_geoid_equals_date(self):
        problems = check_survey_column_roles("iwdate", "hhidpn", "iwdate")
        assert len(problems) == 1
        assert "GEOID column" in problems[0]

    def test_geoid_equals_id(self):
        problems = check_survey_column_roles("iwdate", "hhidpn", "hhidpn")
        assert len(problems) == 1
        assert "'hhidpn'" in problems[0]

    def test_all_three_equal_is_one_message(self):
        problems = check_survey_column_roles("col", "col", "col")
        assert len(problems) == 1
        assert "are all set to" in problems[0]

    def test_blank_columns_are_not_collisions(self):
        assert check_survey_column_roles("iwdate", "", None) == []

    def test_comparison_is_case_sensitive(self):
        assert check_survey_column_roles("iwdate", "GEOID", "geoid") == []


class TestResidentialHistoryColumnRoles:
    def test_valid_config(self):
        assert check_residential_history_column_roles("hhidpn", "move_date", "GEOID") == []

    @pytest.mark.parametrize(
        "id_col,date_col,geoid_col",
        [
            ("x", "x", "GEOID"),
            ("x", "move_date", "x"),
            ("hhidpn", "x", "x"),
        ],
    )
    def test_each_pair_is_reported(self, id_col, date_col, geoid_col):
        problems = check_residential_history_column_roles(id_col, date_col, geoid_col)
        assert len(problems) == 1
        assert problems[0].startswith("Residential history:")

    def test_move_date_uses_gui_wording(self):
        problems = check_residential_history_column_roles("x", "x", "GEOID")
        assert "move date column" in problems[0]


class TestContextualColumnRoles:
    def test_valid_config(self):
        assert check_contextual_column_roles("Date", "GEOID10", ["HeatIndex"]) == []

    def test_date_equals_geoid(self):
        problems = check_contextual_column_roles("Date", "Date", ["HeatIndex"])
        assert len(problems) == 1
        assert problems[0].startswith("Contextual data:")

    def test_data_col_equals_date_col(self):
        problems = check_contextual_column_roles("Date", "GEOID10", ["Date"])
        assert len(problems) == 1
        assert "data column" in problems[0]

    def test_duplicate_data_cols(self):
        problems = check_contextual_column_roles("Date", "GEOID10", ["index", "index"])
        assert len(problems) == 1
        assert "listed 2 times" in problems[0]

    def test_duplicate_data_cols_after_strip(self):
        problems = check_contextual_column_roles("Date", "GEOID10", [" index ", "index"])
        assert len(problems) == 1

    def test_no_data_cols_still_checks_keys(self):
        assert check_contextual_column_roles("Date", "GEOID10", None) == []
        assert len(check_contextual_column_roles("Date", "Date", None)) == 1


class TestOutputPathConflicts:
    def test_distinct_paths(self, tmp_path):
        survey = tmp_path / "survey.dta"
        survey.write_text("x")
        assert check_output_path_conflicts(tmp_path / "out.dta", survey) == []

    def test_output_is_survey(self, tmp_path):
        survey = tmp_path / "survey.dta"
        survey.write_text("x")
        problems = check_output_path_conflicts(survey, survey)
        assert len(problems) == 1
        assert "same file as the survey data file" in problems[0]

    def test_output_is_residential_history(self, tmp_path):
        survey = tmp_path / "survey.dta"
        res_hist = tmp_path / "res.dta"
        for path in (survey, res_hist):
            path.write_text("x")
        problems = check_output_path_conflicts(res_hist, survey, res_hist)
        assert len(problems) == 1
        assert "same file as the residential history file" in problems[0]

    def test_output_not_yet_created_but_resolves_onto_survey(self, tmp_path):
        survey = tmp_path / "survey.dta"
        survey.write_text("x")
        nested = tmp_path / "results"
        nested.mkdir()
        # Output does not exist yet, so this exercises the resolve() fallback.
        problems = check_output_path_conflicts(nested / ".." / "survey.dta", survey)
        assert len(problems) == 1

    @pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
    def test_symlinked_survey(self, tmp_path):
        survey = tmp_path / "survey.dta"
        survey.write_text("x")
        link = tmp_path / "link.dta"
        link.symlink_to(survey)
        assert len(check_output_path_conflicts(link, survey)) == 1

    def test_no_inputs_given(self, tmp_path):
        assert check_output_path_conflicts(tmp_path / "out.dta") == []


class TestGeneratedColumnNames:
    def test_daily_names(self):
        names = generated_column_names(
            date_col="iwdate",
            geoid_col="GEOID2010",
            data_cols=["index"],
            lags=[0, 2],
        )
        assert "iwdate_0day_prior" in names
        assert "GEOID2010_0day_prior" in names
        assert "index_iwdate_2day_prior" in names

    def test_monthly_unit(self):
        names = generated_column_names(
            date_col="iwdate",
            geoid_col="GEOID",
            data_cols=["pm25"],
            lags=[1],
            resolution="monthly",
        )
        assert "pm25_iwdate_1month_prior" in names

    def test_start_lag_offset(self):
        names = generated_column_names(
            date_col="iwdate",
            geoid_col="GEOID",
            data_cols=["pm25"],
            lags=range(30, 33),
        )
        assert "iwdate_30day_prior" in names
        assert "iwdate_0day_prior" not in names

    def test_post_lag_average_name(self):
        names = generated_column_names(
            date_col="iwdate",
            geoid_col="GEOID",
            data_cols=["index"],
            lags=range(0, 3),
            resolution="monthly",
            post_lag_average=True,
            start_lag=0,
            max_lag=2,
        )
        assert "index_avg_0_2month_prior" in names


class TestGeneratedColumnCollisions:
    def _kwargs(self, **overrides):
        kwargs = dict(
            date_col="iwdate",
            geoid_col="GEOID2010",
            data_cols=["index"],
            lags=range(0, 3),
        )
        kwargs.update(overrides)
        return kwargs

    def test_no_collision(self):
        assert check_generated_column_collisions(
            ["hhidpn", "iwdate", "GEOID2010"], **self._kwargs()
        ) == []

    def test_empty_existing_columns(self):
        assert check_generated_column_collisions([], **self._kwargs()) == []

    def test_few_collisions_are_listed(self):
        problems = check_generated_column_collisions(
            ["hhidpn", "iwdate_0day_prior"], **self._kwargs()
        )
        assert len(problems) == 1
        assert "'iwdate_0day_prior'" in problems[0]
        assert "previous STITCH run" in problems[0]

    def test_many_collisions_collapse_to_a_count(self):
        existing = generated_column_names(**self._kwargs(lags=range(0, 100)))
        problems = check_generated_column_collisions(
            existing, **self._kwargs(lags=range(0, 100))
        )
        assert len(problems) == 1
        assert f"already contains {len(existing)} column(s)" in problems[0]
        assert "e.g." in problems[0]


class TestPipelineArgs:
    def _args(self, **overrides):
        args = argparse.Namespace(
            survey_data="survey.dta",
            context_dir="ctx",
            save_dir="out",
            output_name="linked.dta",
            id_col="hhidpn",
            date_col="iwdate",
            geoid_col="GEOID2010",
            measure_type="heat_index",
            data_col="index",
            contextual_geoid_col="GEOID10",
            context_date_col="Date",
            n_lags=3,
            residential_hist=None,
        )
        for key, value in overrides.items():
            setattr(args, key, value)
        return args

    def test_valid_args(self):
        assert check_pipeline_args(self._args()) == []

    def test_sparse_namespace_does_not_raise(self):
        """A namespace missing the optional attributes must still validate."""
        args = argparse.Namespace(
            survey_data="survey.dta",
            id_col="hhidpn",
            date_col="iwdate",
            geoid_col="GEOID2010",
            data_col="index",
        )
        assert check_pipeline_args(args) == []

    def test_reported_bug_id_equals_date(self):
        problems = check_pipeline_args(self._args(id_col="iwdate"))
        assert any("both set to 'iwdate'" in p for p in problems)

    def test_residential_history_only_checked_when_used(self):
        args = self._args(
            res_hist_id_col="x", res_hist_date_col="x", res_hist_geoid_col="GEOID"
        )
        assert check_pipeline_args(args) == []
        args.residential_hist = "res.dta"
        assert len(check_pipeline_args(args)) == 1

    def test_duplicate_data_cols_from_cli_string(self):
        problems = check_pipeline_args(self._args(data_col="index,index"))
        assert any("listed 2 times" in p for p in problems)

    def test_all_problems_are_reported_together(self):
        args = self._args(id_col="iwdate", context_date_col="GEOID10")
        problems = check_pipeline_args(args)
        assert len(problems) == 2

    def test_validate_raises_with_every_problem(self):
        args = self._args(id_col="iwdate", context_date_col="GEOID10")
        with pytest.raises(ValueError) as excinfo:
            validate_pipeline_args(args)
        message = str(excinfo.value)
        assert "Survey data:" in message
        assert "Contextual data:" in message

    def test_validate_does_not_mutate_args(self):
        args = self._args(data_col=" index , index ")
        before = dict(vars(args))
        with pytest.raises(ValueError):
            validate_pipeline_args(args)
        assert dict(vars(args)) == before

    def test_output_path_conflict(self, tmp_path):
        survey = tmp_path / "survey.dta"
        survey.write_text("x")
        args = self._args(
            survey_data=str(survey), save_dir=str(tmp_path), output_name="survey.dta"
        )
        problems = check_pipeline_args(args)
        assert any("same file as the survey data file" in p for p in problems)

    def test_survey_columns_check(self):
        args = self._args()
        assert check_pipeline_survey_columns(args, ["hhidpn", "iwdate"]) == []
        problems = check_pipeline_survey_columns(
            args, ["hhidpn", "iwdate", "index_iwdate_1day_prior"]
        )
        assert len(problems) == 1

    def test_survey_columns_check_skipped_without_data_col(self):
        args = self._args(data_col=None)
        assert check_pipeline_survey_columns(args, ["iwdate_0day_prior"]) == []
