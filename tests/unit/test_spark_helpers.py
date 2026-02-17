"""Unit tests for Spark helper utilities."""

import pandas as pd
import pytest

from shared.utils.spark_helpers import (
    clean_df_for_spark,
    drop_columns_if_exist,
    safe_column_rename,
)


class TestCleanDfForSpark:
    """Tests for clean_df_for_spark function."""

    def test_empty_dataframe(self):
        """Empty DataFrame should return unchanged."""
        df = pd.DataFrame()
        result = clean_df_for_spark(df)
        assert result.empty

    def test_single_type_columns(self):
        """Columns with single type should not be modified."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = clean_df_for_spark(df)
        assert list(result["a"]) == [1, 2, 3]
        assert list(result["b"]) == ["x", "y", "z"]

    def test_mixed_string_and_number(self):
        """Mixed string and number should convert to string."""
        df = pd.DataFrame({"mixed": [1, "two", 3]})
        result = clean_df_for_spark(df)
        assert result["mixed"].dtype == object
        assert list(result["mixed"]) == ["1", "two", "3"]

    def test_mixed_int_and_float(self):
        """Mixed int and float should convert to float."""
        df = pd.DataFrame({"mixed": [1, 2.5, 3]})
        result = clean_df_for_spark(df)
        assert result["mixed"].dtype == float

    def test_all_null_column(self):
        """All-null column should convert to string."""
        df = pd.DataFrame({"nulls": [None, None, None]})
        result = clean_df_for_spark(df)
        assert result["nulls"].dtype == object

    def test_nested_dict_column(self):
        """Column with dicts should convert to string."""
        df = pd.DataFrame({"nested": [{"a": 1}, {"b": 2}, "plain"]})
        result = clean_df_for_spark(df)
        assert result["nested"].dtype == object


class TestDropColumnsIfExist:
    """Tests for drop_columns_if_exist function."""

    def test_drop_existing_columns(self):
        """Should drop columns that exist."""
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = drop_columns_if_exist(df, ["a", "c"])
        assert list(result.columns) == ["b"]

    def test_drop_nonexistent_columns(self):
        """Should ignore columns that don't exist."""
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = drop_columns_if_exist(df, ["c", "d"])
        assert list(result.columns) == ["a", "b"]

    def test_drop_mixed(self):
        """Should drop existing and ignore nonexistent."""
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = drop_columns_if_exist(df, ["a", "z"])
        assert list(result.columns) == ["b"]


class TestSafeColumnRename:
    """Tests for safe_column_rename function."""

    def test_rename_existing_columns(self):
        """Should rename columns that exist."""
        df = pd.DataFrame({"old_name": [1]})
        result = safe_column_rename(df, {"old_name": "new_name"})
        assert list(result.columns) == ["new_name"]

    def test_rename_nonexistent_columns(self):
        """Should ignore columns that don't exist."""
        df = pd.DataFrame({"a": [1]})
        result = safe_column_rename(df, {"z": "new_z"})
        assert list(result.columns) == ["a"]
