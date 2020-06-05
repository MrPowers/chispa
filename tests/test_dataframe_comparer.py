import pytest

from chispa.spark import *
from chispa.dataframe_comparer import *


def test_assert_df_equality_with_schema_mismatch():
    data1 = [(1, "jose"), (2, "li"), (3, "laura")]
    df1 = spark.createDataFrame(data1, ["num", "expected_name"])

    data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df2 = spark.createDataFrame(data2, ["name", "expected_name"])

    with pytest.raises(SchemasNotEqualError) as e_info:
        assert_df_equality(df1, df2)


def test_assert_df_equality_with_content_mismatch():
    data1 = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df1 = spark.createDataFrame(data1, ["name", "expected_name"])

    data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df2 = spark.createDataFrame(data2, ["name", "expected_name"])

    with pytest.raises(DataFramesNotEqualError) as e_info:
        assert_df_equality(df1, df2)


def test_are_dfs_equal_with_schema_mismatch():
    data1 = [(1, "jose"), (2, "li"), (3, "laura")]
    df1 = spark.createDataFrame(data1, ["num", "expected_name"])

    data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df2 = spark.createDataFrame(data2, ["name", "expected_name"])

    assert are_dfs_equal(df1, df2) == False


def test_are_dfs_equal_with_content_mismatch():
    data1 = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df1 = spark.createDataFrame(data1, ["name", "expected_name"])

    data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df2 = spark.createDataFrame(data2, ["name", "expected_name"])

    assert are_dfs_equal(df1, df2) == False


def test_are_dfs_equal_when_dfs_are_the_same():
    data1 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df1 = spark.createDataFrame(data1, ["name", "expected_name"])

    data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
    df2 = spark.createDataFrame(data2, ["name", "expected_name"])

    assert are_dfs_equal(df1, df2) == True

