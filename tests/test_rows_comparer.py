from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from pyspark.sql import SparkSession

from chispa import DataFramesNotEqualError, assert_basic_rows_equality
from chispa.formatting import Color, Style
from chispa.row_comparer import are_rows_equal_enhanced
from chispa.rows_comparer import assert_generic_rows_equality


@dataclass
class ArbitraryFormats:
    """Deliberately uses colors that differ from the built-in defaults, so the output can be told apart."""

    mismatched_rows: list[str] = field(default_factory=lambda: ["green"])
    matched_rows: list[str] = field(default_factory=lambda: ["cyan"])
    mismatched_cells: list[str] = field(default_factory=lambda: ["purple"])
    matched_cells: list[str] = field(default_factory=lambda: ["yellow"])


def describe_assert_basic_rows_equality():
    def it_throws_with_row_mismatches(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_basic_rows_equality(df1.collect(), df2.collect())

    def it_throws_when_rows_have_different_lengths(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li"), (3, "laura"), (4, "bill")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1, "jose"), (2, "li"), (3, "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_basic_rows_equality(df1.collect(), df2.collect())

    def it_works_when_rows_are_the_same(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1, "jose"), (2, "li"), (3, "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert_basic_rows_equality(df1.collect(), df2.collect())

    def it_throws_when_duplicate_column_names_hide_a_mismatch(spark: SparkSession):
        df1 = spark.createDataFrame([(1, 2)], ["a", "a"])
        df2 = spark.createDataFrame([(1, 999)], ["a", "a"])
        with pytest.raises(DataFramesNotEqualError):
            assert_basic_rows_equality(df1.collect(), df2.collect())

    def it_throws_when_the_first_df_has_fewer_rows(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1, "jose"), (2, "li"), (3, "laura")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_basic_rows_equality(df1.collect(), df2.collect())


def describe_assert_generic_rows_equality():
    def it_uses_default_formats_when_none_are_given(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1, "jose"), (2, "laura")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_generic_rows_equality(
                df1.collect(), df2.collect(), are_rows_equal_enhanced, {"allow_nan_equality": True}
            )
        message = str(exc_info.value)
        # the defaults are red (underlined for cells) for mismatches and blue for matches
        assert Color.RED.value in message
        assert Style.UNDERLINE.value in message
        assert Color.BLUE.value in message
        assert Color.PURPLE.value not in message

    def it_converts_an_arbitrary_dataclass_to_a_formatting_config(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1, "jose"), (2, "laura")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_generic_rows_equality(
                df1.collect(),
                df2.collect(),
                are_rows_equal_enhanced,
                {"allow_nan_equality": True},
                formats=ArbitraryFormats(),
            )
        message = str(exc_info.value)
        assert Color.PURPLE.value in message
        assert Color.CYAN.value in message
        assert Color.RED.value not in message

    def it_throws_when_duplicate_column_names_hide_a_mismatch(spark: SparkSession):
        df1 = spark.createDataFrame([(1, 2)], ["a", "a"])
        df2 = spark.createDataFrame([(1, 999)], ["a", "a"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_generic_rows_equality(
                df1.collect(), df2.collect(), are_rows_equal_enhanced, {"allow_nan_equality": True}
            )
        assert "a=999" in str(exc_info.value)
