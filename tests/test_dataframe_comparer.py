from __future__ import annotations

import math
from dataclasses import dataclass, field

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType

from chispa import DataFramesNotEqualError, assert_approx_df_equality, assert_df_equality
from chispa.dataframe_comparer import _contains_map_type, are_dfs_equal
from chispa.formatting import Color, FormattingConfig
from chispa.schema_comparer import SchemasNotEqualError


@dataclass
class ArbitraryFormats:
    """Deliberately uses colors that differ from the built-in defaults, so the output can be told apart."""

    mismatched_rows: list[str] = field(default_factory=lambda: ["green"])
    matched_rows: list[str] = field(default_factory=lambda: ["cyan"])
    mismatched_cells: list[str] = field(default_factory=lambda: ["purple"])
    matched_cells: list[str] = field(default_factory=lambda: ["yellow"])


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

    def it_can_work_with_map_columns_and_ignore_row_order(spark: SparkSession):
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("props", MapType(StringType(), IntegerType())),
        ])
        df1 = spark.createDataFrame([(1, {"a": 10}), (2, {"b": 20})], schema=schema)
        df2 = spark.createDataFrame([(2, {"b": 20}), (1, {"a": 10})], schema=schema)
        assert_df_equality(df1, df2, ignore_row_order=True)

    def it_can_work_with_multi_entry_map_columns_and_ignore_row_order(spark: SparkSession):
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("props", MapType(StringType(), IntegerType())),
        ])
        df1 = spark.createDataFrame(
            [(1, {"a": 10, "b": 20}), (2, {"x": 30, "y": 40})],
            schema=schema,
        )
        df2 = spark.createDataFrame(
            [(2, {"y": 40, "x": 30}), (1, {"b": 20, "a": 10})],
            schema=schema,
        )
        assert_df_equality(df1, df2, ignore_row_order=True)

    def it_can_work_with_struct_containing_map_and_ignore_row_order(spark: SparkSession):
        schema = StructType([
            StructField(
                "s",
                StructType([
                    StructField("name", StringType()),
                    StructField("props", MapType(StringType(), IntegerType())),
                ]),
            )
        ])
        df1 = spark.createDataFrame(
            [({"name": "a", "props": {"k": 1}},), ({"name": "b", "props": {"k": 2}},)], schema=schema
        )
        df2 = spark.createDataFrame(
            [({"name": "b", "props": {"k": 2}},), ({"name": "a", "props": {"k": 1}},)], schema=schema
        )
        assert_df_equality(df1, df2, ignore_row_order=True)

    def it_can_work_with_array_of_maps_and_ignore_row_order(spark: SparkSession):
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("items", ArrayType(MapType(StringType(), IntegerType()))),
        ])
        df1 = spark.createDataFrame([(1, [{"a": 1}]), (2, [{"b": 2}])], schema=schema)
        df2 = spark.createDataFrame([(2, [{"b": 2}]), (1, [{"a": 1}])], schema=schema)
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

    def it_converts_an_arbitrary_dataclass_to_a_formatting_config(spark: SparkSession):
        df1 = spark.createDataFrame([(1, "jose"), (2, "li")], ["num", "expected_name"])
        df2 = spark.createDataFrame([(1, "jose"), (2, "laura")], ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_df_equality(df1, df2, formats=ArbitraryFormats())
        message = str(exc_info.value)
        assert Color.PURPLE.value in message
        assert Color.YELLOW.value in message
        assert Color.RED.value not in message

    def it_works_when_sorting_and_dropping_columns(spark: SparkSession):
        data1 = [("b", "jose", 10), ("a", "jose", 20)]
        df1 = spark.createDataFrame(data1, ["ignore_me", "name", "score"])
        data2 = [("a", "jose", 10), ("b", "jose", 20)]
        df2 = spark.createDataFrame(data2, ["ignore_me", "name", "score"])
        assert_df_equality(df1, df2, ignore_columns=["ignore_me"], ignore_row_order=True)

    def it_shows_the_full_diff_table_by_default(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(1, "jose"), (2, "lima")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_df_equality(df1, df2)
        assert "lima" in str(exc_info.value)

    def it_hides_the_diff_table_when_full_log_is_false(spark: SparkSession):
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(1, "jose"), (2, "lima")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_df_equality(df1, df2, full_log=False)
        message = str(exc_info.value)
        assert "lima" not in message
        assert "DataFrames are not equal" in message


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

    def it_converts_an_arbitrary_dataclass_to_a_formatting_config(spark: SparkSession):
        df1 = spark.createDataFrame([(1.0, "jose"), (2.0, "li")], ["num", "expected_name"])
        df2 = spark.createDataFrame([(1.0, "jose"), (2.0, "laura")], ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_approx_df_equality(df1, df2, 0.1, formats=ArbitraryFormats())
        message = str(exc_info.value)
        assert Color.PURPLE.value in message
        assert Color.CYAN.value in message
        assert Color.RED.value not in message

    def it_keeps_a_formatting_config_as_is(spark: SparkSession):
        data = [(1.0, "jose"), (1.1, "li")]
        df1 = spark.createDataFrame(data, ["num", "expected_name"])
        df2 = spark.createDataFrame(data, ["num", "expected_name"])
        assert_approx_df_equality(df1, df2, 0.1, formats=FormattingConfig(mismatched_rows={"color": "green"}))

    def it_does_not_throw_on_schema_column_order_mismatch_with_transforms(spark: SparkSession):
        data = [(1.0, "jose"), (1.1, "li")]
        df1 = spark.createDataFrame(data, ["num", "expected_name"])
        df2 = spark.createDataFrame([(v, n) for n, v in data], ["expected_name", "num"])
        assert_approx_df_equality(df1, df2, 0.1, transforms=[lambda df: df.select(sorted(df.columns))])

    def it_falls_back_to_exact_comparison_with_zero_precision(spark: SparkSession):
        data = [(1.0, "jose"), (1.1, "li")]
        df1 = spark.createDataFrame(data, ["num", "expected_name"])
        df2 = spark.createDataFrame(data, ["num", "expected_name"])
        assert_approx_df_equality(df1, df2, 0)

    def it_throws_with_zero_precision_and_a_content_mismatch(spark: SparkSession):
        df1 = spark.createDataFrame([(1.0, "jose")], ["num", "expected_name"])
        df2 = spark.createDataFrame([(1.1, "jose")], ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_approx_df_equality(df1, df2, 0)

    def it_can_consider_nan_values_equal_with_zero_precision(spark: SparkSession):
        data = [(1.0, "jose"), (float("nan"), "li")]
        df1 = spark.createDataFrame(data, ["num", "expected_name"])
        df2 = spark.createDataFrame(data, ["num", "expected_name"])
        assert_approx_df_equality(df1, df2, 0, allow_nan_equality=True)

    def it_throws_with_zero_precision_nan_equality_and_a_content_mismatch(spark: SparkSession):
        df1 = spark.createDataFrame([(float("nan"), "jose")], ["num", "expected_name"])
        df2 = spark.createDataFrame([(float("nan"), "li")], ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_approx_df_equality(df1, df2, 0, allow_nan_equality=True)

    def it_shows_the_full_diff_table_by_default(spark: SparkSession):
        df1 = spark.createDataFrame([(1.0, "jose")], ["num", "expected_name"])
        df2 = spark.createDataFrame([(1.1, "jose")], ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_approx_df_equality(df1, df2, 0)
        assert "jose" in str(exc_info.value)

    def it_hides_the_diff_table_when_full_log_is_false(spark: SparkSession):
        df1 = spark.createDataFrame([(1.0, "jose")], ["num", "expected_name"])
        df2 = spark.createDataFrame([(1.1, "jose")], ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as exc_info:
            assert_approx_df_equality(df1, df2, 0, full_log=False)
        message = str(exc_info.value)
        assert "jose" not in message
        assert "DataFrames are not equal" in message


def describe_contains_map_type():
    def it_returns_false_for_a_non_complex_type():
        assert _contains_map_type(IntegerType()) is False

    def it_returns_true_for_a_map_nested_in_an_array():
        assert _contains_map_type(ArrayType(MapType(StringType(), IntegerType()))) is True

    def it_returns_false_for_an_array_without_maps():
        assert _contains_map_type(ArrayType(ArrayType(IntegerType()))) is False

    def it_returns_true_for_a_map_nested_in_a_struct():
        dt = StructType([StructField("m", MapType(StringType(), IntegerType()), True)])
        assert _contains_map_type(dt) is True
