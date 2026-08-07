"""
Tests for GUI validators.
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil

from stitch.gui.validators import (
    validate_file_exists,
    validate_directory_exists,
    validate_stata_file,
    validate_date_column,
    validate_contextual_directory,
    validate_contextual_column_roles,
    validate_generated_column_names,
    validate_output_path_conflicts,
    validate_residential_history_column_roles,
    validate_survey_column_roles,
    check_column_consistency,
    load_preview_data,
)


class TestFileValidators:
    """Test basic file/directory validators."""

    def test_validate_file_exists_valid(self, tmp_path):
        """Test validate_file_exists with valid file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")
        assert validate_file_exists(str(test_file)) is True

    def test_validate_file_exists_invalid(self):
        """Test validate_file_exists with non-existent file."""
        assert validate_file_exists("/nonexistent/file.txt") is False

    def test_validate_directory_exists_valid(self, tmp_path):
        """Test validate_directory_exists with valid directory."""
        assert validate_directory_exists(str(tmp_path)) is True

    def test_validate_directory_exists_invalid(self):
        """Test validate_directory_exists with non-existent directory."""
        assert validate_directory_exists("/nonexistent/directory") is False


class TestStataFileValidator:
    """Test Stata file validation."""

    def test_validate_stata_file_valid(self):
        """Test validate_stata_file with valid file."""
        # Use existing test data
        stata_file = Path("tests/test_data/fake_survey_data.dta")
        if stata_file.exists():
            is_valid, error_msg = validate_stata_file(str(stata_file))
            assert is_valid is True
            assert error_msg == ""

    def test_validate_stata_file_not_found(self):
        """Test validate_stata_file with non-existent file."""
        is_valid, error_msg = validate_stata_file("/nonexistent.dta")
        assert is_valid is False
        assert "not found" in error_msg.lower()

    def test_validate_stata_file_wrong_format(self, tmp_path):
        """Test validate_stata_file with non-Stata file."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col1,col2\n1,2\n")
        is_valid, error_msg = validate_stata_file(str(csv_file))
        assert is_valid is False
        assert "not a stata file" in error_msg.lower()


class TestDateColumnValidator:
    """Test date column validation."""

    def test_validate_date_column_valid(self):
        """Test validate_date_column with valid date column."""
        df = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=10)})
        is_valid, error_msg = validate_date_column(df, "date")
        assert is_valid is True
        assert error_msg == ""

    def test_validate_date_column_parseable(self):
        """Test validate_date_column with parseable strings."""
        df = pd.DataFrame({"date": ["2020-01-01", "2020-01-02", "2020-01-03"]})
        is_valid, error_msg = validate_date_column(df, "date")
        assert is_valid is True

    def test_validate_date_column_missing(self):
        """Test validate_date_column with missing column."""
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        is_valid, error_msg = validate_date_column(df, "date")
        assert is_valid is False
        assert "not found" in error_msg.lower()


class TestContextualDirectoryValidator:
    """Test contextual directory validation."""

    def test_validate_contextual_directory_valid(self, heat_index_dir):
        """Test with valid heat index directory."""
        is_valid, years, error_msg = validate_contextual_directory(
            str(heat_index_dir), measure_type="heat_index"
        )
        assert is_valid is True
        assert len(years) > 0
        assert "2016" in years or "2017" in years  # At least one year
        assert error_msg == ""

    def test_validate_contextual_directory_no_files(self, tmp_path):
        """Test with directory containing no matching files."""
        is_valid, years, error_msg = validate_contextual_directory(
            str(tmp_path), measure_type="nonexistent"
        )
        assert is_valid is False
        assert len(years) == 0
        assert "no files found" in error_msg.lower()

    def test_validate_contextual_directory_invalid_path(self):
        """Test with non-existent directory."""
        is_valid, years, error_msg = validate_contextual_directory(
            "/nonexistent/path", measure_type="test"
        )
        assert is_valid is False
        assert len(years) == 0
        assert "not found" in error_msg.lower()

    def test_validate_contextual_directory_with_extension(self, heat_index_dir):
        """Test with specific file extension."""
        is_valid, years, error_msg = validate_contextual_directory(
            str(heat_index_dir), measure_type="heat_index", file_extension=".csv"
        )
        assert is_valid is True
        assert len(years) > 0


class TestColumnConsistency:
    """Test column consistency checker."""

    def test_check_column_consistency_consistent(self, tmp_path):
        """Test with consistent columns across files."""
        # Create test CSV files with same columns
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df2 = pd.DataFrame({"col1": [5, 6], "col2": [7, 8]})

        file1 = tmp_path / "file1.csv"
        file2 = tmp_path / "file2.csv"

        df1.to_csv(file1, index=False)
        df2.to_csv(file2, index=False)

        is_valid, error_msg = check_column_consistency([file1, file2])
        assert is_valid is True
        assert error_msg == ""

    def test_check_column_consistency_inconsistent(self, tmp_path):
        """Test with inconsistent columns."""
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df2 = pd.DataFrame({"col1": [5, 6], "col3": [7, 8]})  # Different column

        file1 = tmp_path / "file1.csv"
        file2 = tmp_path / "file2.csv"

        df1.to_csv(file1, index=False)
        df2.to_csv(file2, index=False)

        is_valid, error_msg = check_column_consistency([file1, file2])
        assert is_valid is False
        assert "mismatch" in error_msg.lower()

    def test_check_column_consistency_empty_list(self):
        """Test with empty file list."""
        is_valid, error_msg = check_column_consistency([])
        assert is_valid is False
        assert "no files" in error_msg.lower()


class TestLoadPreviewData:
    """Test preview data loading."""

    def test_load_preview_data_csv(self, tmp_path):
        """Test loading CSV preview."""
        df = pd.DataFrame({"col1": range(10), "col2": range(10, 20)})
        csv_file = tmp_path / "test.csv"
        df.to_csv(csv_file, index=False)

        preview_df, error_msg = load_preview_data(str(csv_file), n_rows=5)
        assert preview_df is not None
        assert len(preview_df) == 5
        assert error_msg == ""

    def test_load_preview_data_stata(self):
        """Test loading Stata preview."""
        stata_file = Path("tests/test_data/fake_survey_data.dta")
        if stata_file.exists():
            preview_df, error_msg = load_preview_data(str(stata_file), n_rows=5)
            assert preview_df is not None
            assert len(preview_df) <= 5
            assert error_msg == ""

    def test_load_preview_data_invalid_file(self):
        """Test loading from non-existent file."""
        preview_df, error_msg = load_preview_data("/nonexistent.csv")
        assert preview_df is None
        assert "error" in error_msg.lower()


class TestColumnRoleValidators:
    """Test the (is_valid, error_message) wrappers around the core checks."""

    def test_survey_roles_valid(self):
        assert validate_survey_column_roles("iwdate", "hhidpn", "GEOID2010") == (
            True,
            "",
        )

    def test_survey_roles_collision(self):
        is_valid, error_msg = validate_survey_column_roles(
            "iwdate", "iwdate", "GEOID2010"
        )
        assert is_valid is False
        assert "iwdate" in error_msg

    def test_residential_history_roles_valid(self):
        assert validate_residential_history_column_roles(
            "hhidpn", "move_date", "GEOID"
        ) == (True, "")

    def test_residential_history_roles_collision(self):
        is_valid, error_msg = validate_residential_history_column_roles(
            "hhidpn", "move_date", "move_date"
        )
        assert is_valid is False
        assert "move date column" in error_msg

    def test_contextual_roles_valid(self):
        assert validate_contextual_column_roles("Date", "GEOID10", ["HeatIndex"]) == (
            True,
            "",
        )

    def test_contextual_roles_collision(self):
        is_valid, error_msg = validate_contextual_column_roles(
            "Date", "GEOID10", ["Date"]
        )
        assert is_valid is False
        assert "data column" in error_msg

    def test_output_path_valid(self, tmp_path):
        survey = tmp_path / "survey.dta"
        survey.write_text("x")
        assert validate_output_path_conflicts(
            str(tmp_path), "linked.dta", str(survey)
        ) == (True, "")

    def test_output_path_overwrites_survey(self, tmp_path):
        survey = tmp_path / "survey.dta"
        survey.write_text("x")
        is_valid, error_msg = validate_output_path_conflicts(
            str(tmp_path), "survey.dta", str(survey)
        )
        assert is_valid is False
        assert "survey data file" in error_msg

    def test_output_path_skipped_when_incomplete(self):
        assert validate_output_path_conflicts("", "", "survey.dta") == (True, "")

    def test_generated_names_valid(self):
        assert validate_generated_column_names(
            ["hhidpn", "iwdate", "GEOID2010"],
            date_col="iwdate",
            geoid_col="GEOID2010",
            data_cols=["index"],
            start_lag=0,
            end_lag=2,
            linkage_resolution="daily",
        ) == (True, "")

    def test_generated_names_collision(self):
        is_valid, error_msg = validate_generated_column_names(
            ["hhidpn", "index_iwdate_1day_prior"],
            date_col="iwdate",
            geoid_col="GEOID2010",
            data_cols=["index"],
            start_lag=0,
            end_lag=2,
            linkage_resolution="daily",
        )
        assert is_valid is False
        assert "index_iwdate_1day_prior" in error_msg

    def test_generated_names_skipped_without_columns(self):
        assert validate_generated_column_names(
            [],
            date_col="iwdate",
            geoid_col="GEOID2010",
            data_cols=["index"],
            start_lag=0,
            end_lag=2,
            linkage_resolution="daily",
        ) == (True, "")
