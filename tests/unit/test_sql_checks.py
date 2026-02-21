"""Unit tests for tests/helpers/sql_checks.py — no database required."""
from __future__ import annotations

import pytest

from tests.helpers.sql_checks import timestamp_expression


class TestTimestampExpression:
    """timestamp_expression builds the right SQL cast for each supported format."""

    @pytest.mark.parametrize(
        "fmt,expected_fragment",
        [
            ("timestamp", '"created_at"::timestamptz'),
            ("epoch_millis", 'to_timestamp(("created_at")::numeric / 1000.0)'),
            (
                "epoch_millis_wrapped",
                'to_timestamp((regexp_replace("created_at"::text, \'[^0-9]\', \'\', \'g\'))::numeric / 1000.0)',
            ),
            # Case and whitespace should be normalised.
            ("TIMESTAMP", '"created_at"::timestamptz'),
            ("  epoch_millis  ", 'to_timestamp(("created_at")::numeric / 1000.0)'),
        ],
    )
    def test_known_formats(self, fmt: str, expected_fragment: str):
        result = timestamp_expression("created_at", fmt)
        assert result == expected_fragment

    def test_unknown_format_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported timestamp format"):
            timestamp_expression("col", "unix_seconds")

    def test_column_name_is_quoted(self):
        """Column names with special characters must be double-quoted."""
        result = timestamp_expression('my "col"', "timestamp")
        # Inner double-quotes are escaped by doubling them.
        assert '"my ""col"""' in result

    @pytest.mark.parametrize("fmt", ["timestamp", "epoch_millis", "epoch_millis_wrapped"])
    def test_all_formats_reference_column(self, fmt: str):
        """Every format should include a reference to the column."""
        result = timestamp_expression("ts_col", fmt)
        assert "ts_col" in result
