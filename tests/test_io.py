"""
Tests for the flexible I/O utilities in stitch.io_utils module.

Tests cover:
- Reading various file formats (CSV, Stata, Parquet, Feather, Excel)
- Writing various file formats
- Format detection
- Round-trip read/write consistency
- Error handling for unsupported formats
- Kwargs passing to underlying pandas functions
"""

from __future__ import annotations
import tempfile
import warnings
from pathlib import Path
import pytest
import pandas as pd
import numpy as np
from stitch.io_utils import (
    GeoidTruncationWarning,
    get_file_format,
    normalize_geoid_for_processing,
    read_data,
    write_data,
)


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "Date": pd.date_range("2020-01-01", periods=10),
            "GEOID10": [f"0100102{i:04d}" for i in range(10)],
            "Value": np.random.randn(10),
            "Category": ["A", "B", "C", "D", "E"] * 2,
            "Count": range(10, 20),
        }
    )


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestGetFileFormat:
    """Tests for get_file_format function."""

    def test_csv_format(self):
        assert get_file_format("data.csv") == "csv"
        assert get_file_format("/path/to/data.csv") == "csv"
        assert get_file_format(Path("data.csv")) == "csv"

    def test_stata_format(self):
        assert get_file_format("survey.dta") == "stata"
        assert get_file_format(Path("survey.dta")) == "stata"

    def test_parquet_formats(self):
        assert get_file_format("data.parquet") == "parquet"
        assert get_file_format("data.pq") == "parquet"
        assert get_file_format(Path("data.parquet")) == "parquet"

    def test_feather_format(self):
        assert get_file_format("cache.feather") == "feather"
        assert get_file_format(Path("cache.feather")) == "feather"

    def test_excel_formats(self):
        assert get_file_format("workbook.xlsx") == "excel"
        assert get_file_format("workbook.xls") == "excel"
        assert get_file_format(Path("workbook.xlsx")) == "excel"

    def test_case_insensitive(self):
        """Test that extensions are case-insensitive."""
        assert get_file_format("DATA.CSV") == "csv"
        assert get_file_format("Survey.DTA") == "stata"
        assert get_file_format("Cache.FEATHER") == "feather"

    def test_unsupported_format(self):
        """Test that unsupported formats raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported file format"):
            get_file_format("data.json")

        with pytest.raises(ValueError, match="Unsupported file format"):
            get_file_format("data.txt")


class TestCSVReadWrite:
    """Tests for CSV reading and writing."""

    def test_write_and_read_csv(self, sample_dataframe, temp_dir):
        """Test basic CSV write and read."""
        csv_path = temp_dir / "test.csv"

        write_data(sample_dataframe, csv_path, index=False)
        df_read = read_data(csv_path)

        assert len(df_read) == len(sample_dataframe)
        assert list(df_read.columns) == list(sample_dataframe.columns)

    def test_csv_with_index(self, sample_dataframe, temp_dir):
        """Test CSV with index preservation."""
        csv_path = temp_dir / "test_with_index.csv"

        write_data(sample_dataframe, csv_path, index=True)
        df_read = read_data(csv_path, index_col=0)

        assert len(df_read) == len(sample_dataframe)

    def test_csv_usecols(self, sample_dataframe, temp_dir):
        """Test reading specific columns from CSV."""
        csv_path = temp_dir / "test.csv"
        write_data(sample_dataframe, csv_path, index=False)

        df_read = read_data(csv_path, usecols=["Date", "Value"])

        assert len(df_read.columns) == 2
        assert "Date" in df_read.columns
        assert "Value" in df_read.columns

    def test_csv_dtype(self, sample_dataframe, temp_dir):
        """Test reading CSV with specific dtypes."""
        csv_path = temp_dir / "test.csv"
        write_data(sample_dataframe, csv_path, index=False)

        df_read = read_data(csv_path, dtype={"Count": "float64"})

        assert df_read["Count"].dtype == np.float64


class TestParquetReadWrite:
    """Tests for Parquet reading and writing."""

    def test_write_and_read_parquet(self, sample_dataframe, temp_dir):
        """Test basic Parquet write and read."""
        parquet_path = temp_dir / "test.parquet"

        write_data(sample_dataframe, parquet_path, index=False)
        df_read = read_data(parquet_path)

        assert len(df_read) == len(sample_dataframe)
        pd.testing.assert_frame_equal(df_read, sample_dataframe, check_dtype=False)

    def test_parquet_pq_extension(self, sample_dataframe, temp_dir):
        """Test .pq extension for Parquet."""
        pq_path = temp_dir / "test.pq"

        write_data(sample_dataframe, pq_path, index=False)
        df_read = read_data(pq_path)

        assert len(df_read) == len(sample_dataframe)

    def test_parquet_with_compression(self, sample_dataframe, temp_dir):
        """Test Parquet with compression."""
        parquet_path = temp_dir / "test_compressed.parquet"

        write_data(sample_dataframe, parquet_path, compression="gzip", index=False)
        df_read = read_data(parquet_path)

        assert len(df_read) == len(sample_dataframe)


class TestFeatherReadWrite:
    """Tests for Feather reading and writing."""

    def test_write_and_read_feather(self, sample_dataframe, temp_dir):
        """Test basic Feather write and read."""
        feather_path = temp_dir / "test.feather"

        write_data(sample_dataframe, feather_path)
        df_read = read_data(feather_path)

        assert len(df_read) == len(sample_dataframe)
        pd.testing.assert_frame_equal(df_read, sample_dataframe, check_dtype=False)

    def test_feather_columns(self, sample_dataframe, temp_dir):
        """Test reading specific columns from Feather."""
        feather_path = temp_dir / "test.feather"
        write_data(sample_dataframe, feather_path)

        df_read = read_data(feather_path, columns=["Date", "Value"])

        assert len(df_read.columns) == 2
        assert "Date" in df_read.columns
        assert "Value" in df_read.columns


class TestStataReadWrite:
    """Tests for Stata reading and writing."""

    def test_write_and_read_stata(self, sample_dataframe, temp_dir):
        """Test basic Stata write and read."""
        dta_path = temp_dir / "test.dta"

        # Convert date column to datetime if not already
        df_to_write = sample_dataframe.copy()
        df_to_write["Date"] = pd.to_datetime(df_to_write["Date"])

        write_data(df_to_write, dta_path)
        df_read = read_data(dta_path)

        assert len(df_read) == len(sample_dataframe)
        # Stata may add columns (like index), so just check that original columns are present
        for col in sample_dataframe.columns:
            assert col in df_read.columns

    def test_stata_no_index_parameter(self, sample_dataframe, temp_dir):
        """Test that index parameter is ignored for Stata (doesn't support it)."""
        dta_path = temp_dir / "test.dta"

        # Should not raise error even with index=True
        write_data(sample_dataframe, dta_path, index=True)
        df_read = read_data(dta_path)

        assert len(df_read) == len(sample_dataframe)


class TestExcelReadWrite:
    """Tests for Excel reading and writing."""

    def test_write_and_read_excel_xlsx(self, sample_dataframe, temp_dir):
        """Test basic Excel .xlsx write and read."""
        try:
            import openpyxl
        except ImportError:
            pytest.skip("openpyxl not installed (optional dependency)")

        excel_path = temp_dir / "test.xlsx"

        write_data(sample_dataframe, excel_path, index=False)
        df_read = read_data(excel_path)

        assert len(df_read) == len(sample_dataframe)
        assert list(df_read.columns) == list(sample_dataframe.columns)

    def test_excel_with_sheet_name(self, sample_dataframe, temp_dir):
        """Test Excel with custom sheet name."""
        try:
            import openpyxl
        except ImportError:
            pytest.skip("openpyxl not installed (optional dependency)")

        excel_path = temp_dir / "test_sheets.xlsx"

        write_data(sample_dataframe, excel_path, sheet_name="Data", index=False)
        df_read = read_data(excel_path, sheet_name="Data")

        assert len(df_read) == len(sample_dataframe)


class TestRoundTripConsistency:
    """Tests for round-trip read/write consistency across formats."""

    def test_csv_round_trip(self, sample_dataframe, temp_dir):
        """Test CSV round-trip preserves data."""
        path = temp_dir / "roundtrip.csv"
        write_data(sample_dataframe, path, index=False)
        df_read = read_data(path)

        # CSV doesn't preserve dtypes perfectly, so check shape and values
        assert df_read.shape == sample_dataframe.shape

    def test_parquet_round_trip(self, sample_dataframe, temp_dir):
        """Test Parquet round-trip preserves data exactly."""
        path = temp_dir / "roundtrip.parquet"
        write_data(sample_dataframe, path, index=False)
        df_read = read_data(path)

        pd.testing.assert_frame_equal(df_read, sample_dataframe, check_dtype=False)

    def test_feather_round_trip(self, sample_dataframe, temp_dir):
        """Test Feather round-trip preserves data exactly."""
        path = temp_dir / "roundtrip.feather"
        write_data(sample_dataframe, path)
        df_read = read_data(path)

        pd.testing.assert_frame_equal(df_read, sample_dataframe, check_dtype=False)


class TestErrorHandling:
    """Tests for error handling."""

    def test_read_nonexistent_file(self):
        """Test reading a file that doesn't exist."""
        with pytest.raises(FileNotFoundError):
            read_data("nonexistent_file.csv")

    def test_read_unsupported_format(self, temp_dir):
        """Test reading an unsupported format."""
        json_path = temp_dir / "test.json"
        json_path.write_text('{"key": "value"}')

        with pytest.raises(ValueError, match="Unsupported file format"):
            read_data(json_path)

    def test_write_unsupported_format(self, sample_dataframe, temp_dir):
        """Test writing an unsupported format."""
        json_path = temp_dir / "test.json"

        with pytest.raises(ValueError, match="Unsupported file format"):
            write_data(sample_dataframe, json_path)

    def test_write_creates_parent_directory(self, sample_dataframe, temp_dir):
        """Test that write_data creates parent directories if they don't exist."""
        nested_path = temp_dir / "subdir1" / "subdir2" / "test.csv"

        write_data(sample_dataframe, nested_path, index=False)

        assert nested_path.exists()
        df_read = read_data(nested_path)
        assert len(df_read) == len(sample_dataframe)


class TestKwargsPassthrough:
    """Tests that kwargs are properly passed to underlying pandas functions."""

    def test_csv_parse_dates(self, sample_dataframe, temp_dir):
        """Test that parse_dates kwargs work for CSV."""
        csv_path = temp_dir / "test.csv"
        write_data(sample_dataframe, csv_path, index=False)

        df_read = read_data(csv_path, parse_dates=["Date"])

        assert pd.api.types.is_datetime64_any_dtype(df_read["Date"])

    def test_csv_na_values(self, temp_dir):
        """Test that na_values kwargs work for CSV."""
        csv_path = temp_dir / "test_na.csv"
        df = pd.DataFrame({"A": [1, 2, -999, 4], "B": ["x", "y", "z", "MISSING"]})
        write_data(df, csv_path, index=False)

        df_read = read_data(csv_path, na_values={"A": [-999], "B": ["MISSING"]})

        assert pd.isna(df_read.loc[2, "A"])
        assert pd.isna(df_read.loc[3, "B"])

    @pytest.mark.parametrize(
        "ext", [".csv", ".parquet", ".feather", ".dta", ".xlsx"]
    )
    def test_dtype_applied_for_every_format(self, sample_dataframe, temp_dir, ext):
        """
        A requested dtype must be honoured regardless of file format.

        Only read_csv/read_excel accept ``dtype=``. read_parquet forwards
        unknown kwargs to pyarrow, which raised TypeError; read_feather and
        read_stata dropped it silently, leaving the column at its stored dtype.
        Both are now handled by casting after the read.
        """
        path = temp_dir / f"test_dtype{ext}"
        write_data(sample_dataframe, path, index=False)

        df_read = read_data(path, dtype={"Value": "float32"})

        assert df_read["Value"].dtype == "float32"

    @pytest.mark.parametrize("ext", [".csv", ".parquet", ".feather", ".dta", ".xlsx"])
    def test_dtype_none_is_accepted(self, sample_dataframe, temp_dir, ext):
        """An explicit ``dtype=None`` must be a no-op, not an error."""
        path = temp_dir / f"test_dtype_none{ext}"
        write_data(sample_dataframe, path, index=False)

        df_read = read_data(path, dtype=None)

        assert len(df_read) == len(sample_dataframe)

    def test_dtype_ignores_columns_absent_from_selection(
        self, sample_dataframe, temp_dir
    ):
        """A dtype map may name columns that usecols did not select."""
        path = temp_dir / "test_dtype_subset.parquet"
        write_data(sample_dataframe, path, index=False)

        df_read = read_data(
            path, usecols=["Date", "Value"], dtype={"Value": "float32", "Count": "int32"}
        )

        assert list(df_read.columns) == ["Date", "Value"]
        assert df_read["Value"].dtype == "float32"


class TestGeoidTruncationWarning:
    """The truncation warning must fire when, and only when, digits are dropped."""

    @pytest.mark.parametrize(
        "series",
        [
            pd.Series([12345678901.0, 6001020100.0]),  # float column
            pd.Series([12345678901.0, None]),  # float column with a gap
            pd.Series([12345678901], dtype="int64"),  # integer column
            pd.Series(["12345678901"]),  # already 11 digits
            pd.Series(["06001020100"]),  # leading zero preserved
        ],
        ids=["float", "float_with_na", "int64", "str_11", "str_leading_zero"],
    )
    def test_no_warning_when_nothing_is_truncated(self, series, recwarn):
        """
        Numeric columns previously warned on every value.

        Detection stringified the raw value, so a float's trailing ".0" counted
        as a twelfth digit -- while the conversion path casts through int() and
        drops it. The warning named "float representation artifacts" as its
        cause, which was exactly the case it handled correctly.
        """
        out = normalize_geoid_for_processing(series, "code", 11, "int")

        assert not [
            w for w in recwarn if w.category is GeoidTruncationWarning
        ], "warned although no value was truncated"
        assert all(v == "" or len(v) == 11 for v in out)

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("060010201001", "06001020100"),  # block group -> tract
            ("060010201001234", "06001020100"),  # block -> tract
        ],
    )
    def test_warns_and_truncates_longer_identifiers(self, value, expected):
        with pytest.warns(GeoidTruncationWarning) as record:
            out = normalize_geoid_for_processing(pd.Series([value]), "code", 11, "int")

        assert list(out) == [expected]
        message = str(record[0].message)
        assert "1 GEOID value(s)" in message
        assert f"{value} -> {expected}" in message, f"no before/after in: {message}"

    def test_warning_reports_counts_and_examples(self):
        """The message must say how much was affected, not just that it happened."""
        series = pd.Series(["060010201001", "060010201001", "060010201002"])

        with pytest.warns(GeoidTruncationWarning) as record:
            normalize_geoid_for_processing(series, "code", 11, "int")

        message = str(record[0].message)
        assert "3 GEOID value(s)" in message
        assert "2 distinct" in message

    def test_no_warning_when_padding_is_disabled(self):
        """With n_digits <= 0 nothing is truncated, so nothing should warn."""
        with warnings.catch_warnings():
            warnings.simplefilter("error", GeoidTruncationWarning)
            out = normalize_geoid_for_processing(
                pd.Series(["060010201001234"]), "code", 0, "int"
            )
        assert list(out) == ["060010201001234"]


class TestSpecialCases:
    """Tests for special cases and edge conditions."""

    def test_empty_dataframe(self, temp_dir):
        """Test reading and writing empty DataFrames."""
        # Create an empty DataFrame with columns (fully empty DataFrames are edge case)
        empty_df = pd.DataFrame(columns=["A", "B", "C"])

        # CSV
        csv_path = temp_dir / "empty.csv"
        write_data(empty_df, csv_path, index=False)
        df_read = read_data(csv_path)
        assert len(df_read) == 0
        assert list(df_read.columns) == ["A", "B", "C"]

        # Parquet
        parquet_path = temp_dir / "empty.parquet"
        write_data(empty_df, parquet_path, index=False)
        df_read = read_data(parquet_path)
        assert len(df_read) == 0
        assert list(df_read.columns) == ["A", "B", "C"]

    def test_single_row_dataframe(self, temp_dir):
        """Test reading and writing single-row DataFrames."""
        single_row = pd.DataFrame({"A": [1], "B": ["x"]})

        csv_path = temp_dir / "single.csv"
        write_data(single_row, csv_path, index=False)
        df_read = read_data(csv_path)

        assert len(df_read) == 1
        assert df_read["A"].iloc[0] == 1
        assert df_read["B"].iloc[0] == "x"

    def test_large_number_of_columns(self, temp_dir):
        """Test DataFrame with many columns."""
        many_cols = pd.DataFrame({f"col_{i}": range(10) for i in range(100)})

        parquet_path = temp_dir / "many_cols.parquet"
        write_data(many_cols, parquet_path, index=False)
        df_read = read_data(parquet_path)

        assert len(df_read.columns) == 100
        assert len(df_read) == 10

    def test_unicode_data(self, temp_dir):
        """Test handling of Unicode data."""
        unicode_df = pd.DataFrame(
            {"text": ["Hello", "你好", "Привет", "🌍🌎🌏"], "value": [1, 2, 3, 4]}
        )

        # Test CSV
        csv_path = temp_dir / "unicode.csv"
        write_data(unicode_df, csv_path, index=False)
        df_read = read_data(csv_path)
        assert df_read["text"].iloc[1] == "你好"
        assert df_read["text"].iloc[3] == "🌍🌎🌏"

        # Test Parquet
        parquet_path = temp_dir / "unicode.parquet"
        write_data(unicode_df, parquet_path, index=False)
        df_read = read_data(parquet_path)
        pd.testing.assert_frame_equal(df_read, unicode_df)


class TestPathTypes:
    """Tests for different path types (str vs Path)."""

    def test_string_path(self, sample_dataframe, temp_dir):
        """Test using string paths."""
        csv_path = str(temp_dir / "test.csv")

        write_data(sample_dataframe, csv_path, index=False)
        df_read = read_data(csv_path)

        assert len(df_read) == len(sample_dataframe)

    def test_pathlib_path(self, sample_dataframe, temp_dir):
        """Test using pathlib.Path objects."""
        csv_path = Path(temp_dir) / "test.csv"

        write_data(sample_dataframe, csv_path, index=False)
        df_read = read_data(csv_path)

        assert len(df_read) == len(sample_dataframe)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
