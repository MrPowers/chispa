from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from chispa import DataFramesNotEqualError, assert_basic_rows_equality


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
