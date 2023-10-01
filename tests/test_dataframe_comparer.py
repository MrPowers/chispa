import pytest

from .spark import *
from chispa import *
from chispa.dataframe_comparer import are_dfs_equal
from chispa.schema_comparer import SchemasNotEqualError
import math
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


def describe_assert_df_equality():
    def it_throws_with_schema_mismatches():
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_df_equality(df1, df2)


    def it_can_work_with_different_row_orders():
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(2, "li"), (1, "jose")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        assert_df_equality(df1, df2, transforms=[lambda df: df.sort(df.columns)])


    def it_can_work_with_different_row_orders_with_a_flag():
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(2, "li"), (1, "jose")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        assert_df_equality(df1, df2, ignore_row_order=True)


    def it_can_work_with_different_row_and_column_orders():
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [("li", 2), ("jose", 1)]
        df2 = spark.createDataFrame(data2, ["name", "num"])
        assert_df_equality(df1, df2, ignore_row_order=True, ignore_column_order=True)


    def it_raises_for_row_insensitive_with_diff_content():
        data1 = [(1, "XXXX"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(2, "li"), (1, "jose")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        with pytest.raises(DataFramesNotEqualError):
            assert_df_equality(df1, df2, transforms=[lambda df: df.sort(df.columns)])


    def it_throws_with_schema_column_order_mismatch():
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [("jose", 1), ("li", 1)]
        df2 = spark.createDataFrame(data2, ["name", "num"])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_df_equality(df1, df2)


    def it_does_not_throw_on_schema_column_order_mismatch_with_transforms():
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [("jose", 1), ("li", 2)]
        df2 = spark.createDataFrame(data2, ["name", "num"])
        assert_df_equality(df1, df2, transforms=[
            lambda df: df.select(sorted(df.columns))
        ])


    def it_throws_with_schema_mismatch():
        data1 = [(1, "jose"), (2, "li")]
        df1 = spark.createDataFrame(data1, ["num", "different_name"])
        data2 = [("jose", 1), ("li", 2)]
        df2 = spark.createDataFrame(data2, ["name", "num"])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_df_equality(df1, df2, transforms=[lambda df: df.select(sorted(df.columns))])


    def it_throws_with_content_mismatches():
        data1 = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as e_info:
            assert_df_equality(df1, df2)


    def it_throws_with_length_mismatches():
        data1 = [("jose", "jose"), ("li", "li"), ("laura", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("jose", "jose"), ("li", "li")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as e_info:
            assert_df_equality(df1, df2)


    def it_can_consider_nan_values_equal():
        data1 = [(float('nan'), "jose"), (2.0, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(float('nan'), "jose"), (2.0, "li")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        assert_df_equality(df1, df2, allow_nan_equality=True)


    def it_does_not_consider_nan_values_equal_by_default():
        data1 = [(float('nan'), "jose"), (2.0, "li")]
        df1 = spark.createDataFrame(data1, ["num", "name"])
        data2 = [(float('nan'), "jose"), (2.0, "li")]
        df2 = spark.createDataFrame(data2, ["num", "name"])
        with pytest.raises(DataFramesNotEqualError) as e_info:
            assert_df_equality(df1, df2, allow_nan_equality=False)


    def it_can_ignore_metadata():
        rows_data = [("jose", 1), ("li", 2), ("luisa", 3)]
        schema1 = StructType(
            [
                StructField("name", StringType(), True, {"hi": "no"}),
                StructField("age", IntegerType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("name", StringType(), True, {"hi": "whatever"}),
                StructField("age", IntegerType(), True),
            ]
        )
        df1 = spark.createDataFrame(rows_data, schema1)
        df2 = spark.createDataFrame(rows_data, schema2)
        assert_df_equality(df1, df2, ignore_metadata=True)


    def it_catches_mismatched_metadata():
        rows_data = [("jose", 1), ("li", 2), ("luisa", 3)]
        schema1 = StructType(
            [
                StructField("name", StringType(), True, {"hi": "no"}),
                StructField("age", IntegerType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("name", StringType(), True, {"hi": "whatever"}),
                StructField("age", IntegerType(), True),
            ]
        )
        df1 = spark.createDataFrame(rows_data, schema1)
        df2 = spark.createDataFrame(rows_data, schema2)
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_df_equality(df1, df2)



def describe_are_dfs_equal():
    def it_returns_false_with_schema_mismatches():
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert are_dfs_equal(df1, df2) == False


    def it_returns_false_with_content_mismatches():
        data1 = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert are_dfs_equal(df1, df2) == False


    def it_returns_true_when_dfs_are_same():
        data1 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert are_dfs_equal(df1, df2) == True


def describe_assert_approx_df_equality():
    def it_throws_with_content_mismatch():
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (1.0, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li"), (1.0, "laura"), (None, "hi")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as e_info:
            assert_approx_df_equality(df1, df2, 0.1)


    def it_throws_with_with_length_mismatch():
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as e_info:
            assert_approx_df_equality(df1, df2, 0.1)



    def it_does_not_throw_with_no_mismatch():
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li"), (1.2, "laura"), (None, None)]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        assert_approx_df_equality(df1, df2, 0.1)


    def it_does_not_throw_with_different_row_col_order():
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("li", 1.05), ("laura", 1.2), (None, None), ("jose", 1.0)]
        df2 = spark.createDataFrame(data2, ["expected_name", "num"])
        assert_approx_df_equality(df1, df2, 0.1, ignore_row_order=True, ignore_column_order=True)


    def it_does_not_throw_with_nan_values():
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None), (float("nan"), "buk")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li"), (1.2, "laura"), (None, None), (math.nan, "buk")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        assert_approx_df_equality(df1, df2, 0.1, allow_nan_equality=True)
