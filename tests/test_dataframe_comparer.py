import pytest

from spark import *
from chispa import *
from chispa.dataframe_comparer import are_dfs_equal
from chispa.schema_comparer import SchemasNotEqualError


def describe_assert_column_equality():
    def it_throws_with_schema_mismatches():
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_df_equality(df1, df2)


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

