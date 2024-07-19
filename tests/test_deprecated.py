from __future__ import annotations

import warnings
from dataclasses import dataclass

import pytest

from chispa import DataFramesNotEqualError, assert_basic_rows_equality
from chispa.default_formats import DefaultFormats
from chispa.formatting import FormattingConfig

from .spark import spark


def test_default_formats_deprecation_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        DefaultFormats()
        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)
        assert "DefaultFormats is deprecated" in str(w[-1].message)


def test_that_default_formats_still_works():
    data1 = [(1, "jose"), (2, "li"), (3, "laura")]
    df1 = spark.createDataFrame(data1, ["num", "expected_name"])
    data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df2 = spark.createDataFrame(data2, ["name", "expected_name"])
    with pytest.raises(DataFramesNotEqualError):
        assert_basic_rows_equality(df1.collect(), df2.collect(), formats=DefaultFormats())


def test_deprecated_arbitrary_dataclass():
    data1 = [(1, "jose"), (2, "li"), (3, "laura")]
    df1 = spark.createDataFrame(data1, ["num", "expected_name"])
    data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df2 = spark.createDataFrame(data2, ["name", "expected_name"])

    @dataclass
    class CustomFormats:
        mismatched_rows = ["green"]  # noqa: RUF012
        matched_rows = ["yellow"]  # noqa: RUF012
        mismatched_cells = ["purple", "bold"]  # noqa: RUF012
        matched_cells = ["cyan"]  # noqa: RUF012

    with warnings.catch_warnings(record=True) as w:
        try:
            assert_basic_rows_equality(df1.collect(), df2.collect(), formats=CustomFormats())
            # should not reach the line below due to the raised error.
            # pytest.raises does not work as expected since then we cannot verify the warning.
            assert False
        except DataFramesNotEqualError:
            warnings.simplefilter("always")
            assert len(w) == 1
            assert issubclass(w[-1].category, DeprecationWarning)
            assert "Using an arbitrary dataclass is deprecated." in str(w[-1].message)


def test_invalid_value_in_default_formats():
    @dataclass
    class InvalidFormats:
        mismatched_rows = ["green"]  # noqa: RUF012
        matched_rows = ["yellow"]  # noqa: RUF012
        mismatched_cells = ["purple", "invalid"]  # noqa: RUF012
        matched_cells = ["cyan"]  # noqa: RUF012

    with pytest.raises(ValueError):
        FormattingConfig._from_arbitrary_dataclass(InvalidFormats())
