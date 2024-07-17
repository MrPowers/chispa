from __future__ import annotations

import warnings

import pytest

from chispa import DataFramesNotEqualError, assert_basic_rows_equality
from chispa.default_formats import DefaultFormats
from chispa.formatting import Color, Style

from .spark import spark


def test_default_formats_deprecation_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        DefaultFormats()
        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)
        assert "DefaultFormats is deprecated" in str(w[-1].message)


def test_default():
    df = DefaultFormats()
    assert df.mismatched_rows.color == Color.RED
    assert df.mismatched_rows.style is None
    assert df.matched_rows.color == Color.BLUE
    assert df.matched_rows.style is None
    assert df.mismatched_cells.color == Color.RED
    assert df.mismatched_cells.style == [Style.UNDERLINE]
    assert df.matched_cells.color == Color.BLUE
    assert df.matched_cells.style is None


def test_custom_mismatched_rows_format():
    df = DefaultFormats(mismatched_rows=["green", "bold", "underline"])
    assert df.mismatched_rows.color == Color.GREEN
    assert df.mismatched_rows.style == [Style.BOLD, Style.UNDERLINE]


def test_custom_matched_rows_format():
    df = DefaultFormats(matched_rows=["yellow"])
    assert df.matched_rows.color == Color.YELLOW
    assert df.matched_rows.style is None


def test_custom_mismatched_cells_format():
    df = DefaultFormats(mismatched_cells=["purple", "blink"])
    assert df.mismatched_cells.color == Color.PURPLE
    assert df.mismatched_cells.style == [Style.BLINK]


def test_custom_matched_cells_format():
    df = DefaultFormats(matched_cells=["cyan", "invert", "hide"])
    assert df.matched_cells.color == Color.CYAN
    assert df.matched_cells.style == [Style.INVERT, Style.HIDE]


def test_invalid():
    with pytest.raises(ValueError, match=r"Invalid value: invalid_value.*"):
        DefaultFormats(mismatched_rows=["red", "invalid_value"])


def test_that_default_formats_still_works():
    data1 = [(1, "jose"), (2, "li"), (3, "laura")]
    df1 = spark.createDataFrame(data1, ["num", "expected_name"])
    data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df2 = spark.createDataFrame(data2, ["name", "expected_name"])
    with pytest.raises(DataFramesNotEqualError):
        assert_basic_rows_equality(df1.collect(), df2.collect(), formats=DefaultFormats())
