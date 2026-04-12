from __future__ import annotations

import math

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from chispa import DataFramesNotEqualError, assert_approx_df_equality, assert_df_equality
from chispa.dataframe_comparer import are_dfs_equal
from chispa.schema_comparer import SchemasNotEqualError


def describe_assert_df_equality():
    def it_throws_with_schema_mismatches(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(SchemasNotEqualError):
            assert_df_equality(df1, df2)

    def it_can_work_with_different_row_orders(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(2, "li"), (1, "jose")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        assert_df_equality(df1, df2, transforms=[lambda df: df.sort(df.columns)])

    def it_can_work_with_different_row_orders_with_a_flag(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(2, "li"), (1, "jose")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        assert_df_equality(df1, df2, ignore_row_order=True)

    def it_can_work_with_struct_columns_and_ignore_row_order(spark: SparkSession):
        data1 = [((1, "jose"),), ((2, "li"),)]
        df1 = spark.createDataFrame(data1, ["person"])
        data2 = [((2, "li"),), ((1, "jose"),)]
        df2 = spark.createDataFrame(data2, ["person"])
        assert_df_equality(df1, df2, ignore_row_order=True)

    def it_can_work_with_mixed_columns_and_ignore_row_order(spark: SparkSession):
        data1 = [((1, "jose"), 100), ((2, "li"), 200)]
        df1 = spark.createDataFrame(data1, ["person", "score"])
        data2 = [((2, "li"), 200), ((1, "jose"), 100)]
        df2 = spark.createDataFrame(data2, ["person", "score"])
        assert_df_equality(df1, df2, ignore_row_order=True)

    def it_can_work_with_nested_struct_columns_and_ignore_row_order(spark: SparkSession):
        data1 = [(((1, "jose"), 30),), (((2, "li"), 40),)]
        df1 = spark.createDataFrame(data1, ["nested_person"])
        data2 = [(((2, "li"), 40),), (((1, "jose"), 30),)]
        df2 = spark.createDataFrame(data2, ["nested_person"])
        assert_df_equality(df1, df2, ignore_row_order=True)

    def it_can_work_with_different_row_and_column_orders(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [("li", 2), ("jose", 1)]
        df2 = spark.createDataFrame(data2, ["name", "num"])
        assert_df_equality(df1, df2, ignore_row_order=True, ignore_column_order=True)

    def it_raises_for_row_insensitive_with_diff_content(spark: SparkSession):
        data1 = [(1, "XXXX"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(2, "li"), (1, "jose")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_df_equality(df1, df2, transforms=[lambda df: df.sort(df.columns)])

    def it_throws_with_schema_column_order_mismatch(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [("jose", 1), ("li", 1)]
        df2 = spark.createDataFrame(data2, ["name", "num"])
        with pytest.raises(SchemasNotEqualError):
            assert_df_equality(df1, df2)

    def it_does_not_throw_on_schema_column_order_mismatch_with_transforms(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [("jose", 1), ("li", 2)]
        df2 = spark.createDataFrame(data2, ["name", "num"])
        assert_df_equality(df1, df2, transforms=[lambda df: df.select(sorted(df.columns))])

    def it_throws_with_schema_mismatch(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "different_name"])
        data2 = [("jose", 1), ("li", 2)]
        df2 = spark.createDataFrame(data2, ["name", "num"])
        with pytest.raises(SchemasNotEqualError):
            assert_df_equality(df1, df2, transforms=[lambda df: df.select(sorted(df.columns))])

    def it_throws_with_content_mismatches(spark: SparkSession):
        data1 = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_df_equality(df1, df2)

    def it_throws_with_length_mismatches(spark: SparkSession):
        data1 = [("jose", "jose"), ("li", "li"), ("laura", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("jose", "jose"), ("li", "li")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_df_equality(df1, df2)

    def it_can_consider_nan_values_equal(spark: SparkSession):
        data1 = [(float("nan"), "jose"), (2.0, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(float("nan"), "jose"), (2.0, "li")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        assert_df_equality(df1, df2, allow_nan_equality=True)

    def it_does_not_consider_nan_values_equal_by_default(spark: SparkSession):
        data1 = [(float("nan"), "jose"), (2.0, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(float("nan"), "jose"), (2.0, "li")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_df_equality(df1, df2, allow_nan_equality=False)

    def it_can_consider_nan_values_equal_in_array_fields(spark: SparkSession):
        data1 = [([1.0, float("nan"), 3.0], "jose"), ([4.0, 5.0], "li")]
        df1 = spark.createDataFrame(data1, ["nums", "name"])
        data2 = [([1.0, float("nan"), 3.0], "jose"), ([4.0, 5.0], "li")]
        df2 = spark.createDataFrame(data2, ["nums", "name"])
        assert_df_equality(df1, df2, allow_nan_equality=True)

    def it_raises_when_array_nan_positions_are_different_with_allow_nan_equality(spark: SparkSession):
        data1 = [([1.0, float("nan"), 3.0], "jose"), ([4.0, 5.0], "li")]
        df1 = spark.createDataFrame(data1, ["nums", "name"])
        data2 = [([float("nan"), 1.0, 3.0], "jose"), ([4.0, 5.0], "li")]
        df2 = spark.createDataFrame(data2, ["nums", "name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_df_equality(df1, df2, allow_nan_equality=True)

    def it_does_not_consider_nan_values_equal_in_array_fields_by_default(spark: SparkSession):
        data1 = [([1.0, float("nan"), 3.0], "jose"), ([4.0, 5.0], "li")]
        df1 = spark.createDataFrame(data1, ["nums", "name"])
        data2 = [([1.0, float("nan"), 3.0], "jose"), ([4.0, 5.0], "li")]
        df2 = spark.createDataFrame(data2, ["nums", "name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_df_equality(df1, df2, allow_nan_equality=False)

    def it_can_ignore_metadata(spark: SparkSession):
        rows_data = [("jose", 1), ("li", 2), ("luisa", 3)]
        schema1 = StructType([
            StructField("name", StringType(), True, {"hi": "no"}),
            StructField("age", IntegerType(), True),
        ])
        schema2 = StructType([
            StructField("name", StringType(), True, {"hi": "whatever"}),
            StructField("age", IntegerType(), True),
        ])
        df1 = spark.createDataFrame(rows_data, schema1)
        df2 = spark.createDataFrame(rows_data, schema2)
        assert_df_equality(df1, df2, ignore_metadata=True)

    def it_catches_mismatched_metadata(spark: SparkSession):
        rows_data = [("jose", 1), ("li", 2), ("luisa", 3)]
        schema1 = StructType([
            StructField("name", StringType(), True, {"hi": "no"}),
            StructField("age", IntegerType(), True),
        ])
        schema2 = StructType([
            StructField("name", StringType(), True, {"hi": "whatever"}),
            StructField("age", IntegerType(), True),
        ])
        df1 = spark.createDataFrame(rows_data, schema1)
        df2 = spark.createDataFrame(rows_data, schema2)
        with pytest.raises(SchemasNotEqualError):
            assert_df_equality(df1, df2)

    def it_can_ignore_columns(spark: SparkSession):
        data1 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "boo"), ("luisa", "boo")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert_df_equality(df1, df2, ignore_columns=["expected_name"])

    def it_throws_when_dfs_are_not_same_with_ignored_columns(spark: SparkSession):
        data1 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "boo"), ("luisa", "boo")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert assert_df_equality(df1, df2, ignore_columns=["name"])

    def it_works_when_sorting_and_dropping_columns(spark: SparkSession):
        data1 = [("b", "jose", 10), ("a", "jose", 20)]
        df1 = spark.createDataFrame(data1, ["ignore_me", "name", "score"])
        data2 = [("a", "jose", 10), ("b", "jose", 20)]
        df2 = spark.createDataFrame(data2, ["ignore_me", "name", "score"])
        assert_df_equality(df1, df2, ignore_columns=["ignore_me"], ignore_row_order=True)


def describe_are_dfs_equal():
    def it_returns_false_with_schema_mismatches(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert are_dfs_equal(df1, df2) is False

    def it_returns_false_with_content_mismatches(spark: SparkSession):
        data1 = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert are_dfs_equal(df1, df2) is False

    def it_returns_true_when_dfs_are_same(spark: SparkSession):
        data1 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert are_dfs_equal(df1, df2) is True


def describe_assert_approx_df_equality():
    def it_throws_with_content_mismatch(spark: SparkSession):
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (1.0, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li"), (1.0, "laura"), (None, "hi")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_approx_df_equality(df1, df2, 0.1)

    def it_throws_with_with_length_mismatch(spark: SparkSession):
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_approx_df_equality(df1, df2, 0.1)

    def it_does_not_throw_with_no_mismatch(spark: SparkSession):
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li"), (1.2, "laura"), (None, None)]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        assert_approx_df_equality(df1, df2, 0.1)

    def it_does_not_throw_with_different_row_col_order(spark: SparkSession):
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("li", 1.05), ("laura", 1.2), (None, None), ("jose", 1.0)]
        df2 = spark.createDataFrame(data2, ["expected_name", "num"])
        assert_approx_df_equality(df1, df2, 0.1, ignore_row_order=True, ignore_column_order=True)

    def it_does_not_throw_with_nan_values(spark: SparkSession):
        data1 = [
            (1.0, "jose"),
            (1.1, "li"),
            (1.2, "laura"),
            (None, None),
            (float("nan"), "buk"),
        ]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [
            (1.0, "jose"),
            (1.05, "li"),
            (1.2, "laura"),
            (None, None),
            (math.nan, "buk"),
        ]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        assert_approx_df_equality(df1, df2, 0.1, allow_nan_equality=True)

    def it_can_ignore_columns(spark: SparkSession):
        data1 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "boo"), ("luisa", "boo")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert_approx_df_equality(df1, df2, 0.1, ignore_columns=["expected_name"])

    def it_throws_when_dfs_are_not_same_with_ignored_columns(spark: SparkSession):
        data1 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "boo"), ("luisa", "boo")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert assert_approx_df_equality(df1, df2, 0.1, ignore_columns=["name"])

    def it_can_ignore_metadata(spark: SparkSession):
        schema1 = StructType([
            StructField("num", IntegerType(), True, {"comment": "a"}),
            StructField("name", StringType(), True),
        ])
        schema2 = StructType([
            StructField("num", IntegerType(), True, {"comment": "b"}),
            StructField("name", StringType(), True),
        ])
        df1 = spark.createDataFrame([(1, "jose"), (2, "li")], schema=schema1)
        df2 = spark.createDataFrame([(1, "jose"), (2, "li")], schema=schema2)
        assert_approx_df_equality(df1, df2, 0.1, ignore_metadata=True)

    def it_does_not_throw_with_struct_columns_and_ignore_row_order(spark: SparkSession):
        data1 = [((1.0, "jose"),), ((1.1, "li"),)]
        df1 = spark.createDataFrame(data1, ["person"])
        data2 = [((1.1, "li"),), ((1.0, "jose"),)]
        df2 = spark.createDataFrame(data2, ["person"])
        assert_approx_df_equality(df1, df2, 0.1, ignore_row_order=True)
