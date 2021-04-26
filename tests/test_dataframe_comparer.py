import pytest

from spark import *
from chispa import *
from chispa.schema_comparer import SchemasNotEqualError
from chispa.row_comparer import RowsNotEqualError


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
        with pytest.raises(RowsNotEqualError):
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
        with pytest.raises(RowsNotEqualError) as e_info:
            assert_df_equality(df1, df2)


    def it_throws_with_length_mismatches():
        data1 = [("jose", "jose"), ("li", "li"), ("laura", "laura")]
        df1 = spark.createDataFrame(data1, ["name", "expected_name"])
        data2 = [("jose", "jose"), ("li", "li")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(RowsNotEqualError) as e_info:
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
        with pytest.raises(RowsNotEqualError) as e_info:
            assert_df_equality(df1, df2, allow_nan_equality=False)


def describe_assert_approx_df_equality():
    def it_throws_with_content_mismatch():
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (1.0, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li"), (1.0, "laura"), (None, "hi")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(RowsNotEqualError) as e_info:
            assert_df_equality(df1, df2, precision=0.1)


    def it_throws_with_with_length_mismatch():
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li")]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        with pytest.raises(RowsNotEqualError) as e_info:
            assert_df_equality(df1, df2, precision=0.1)


    def it_does_not_throw_with_no_mismatch():
        data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1.0, "jose"), (1.05, "li"), (1.2, "laura"), (None, None)]
        df2 = spark.createDataFrame(data2, ["num", "expected_name"])
        assert_df_equality(df1, df2, precision=0.1)
