import pytest

from spark import *
from chispa import *
from chispa.dataframe_comparer import are_dfs_equal
from chispa.schema_comparer import SchemasNotEqualError


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


def test_assert_approx_df_equality_with_content_mismatch():
    data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (1.0, None)]
    df1 = spark.createDataFrame(data1, ["num", "expected_name"])

    data2 = [(1.0, "jose"), (1.05, "li"), (1.0, "laura"), (None, "hi")]
    df2 = spark.createDataFrame(data2, ["num", "expected_name"])

    with pytest.raises(DataFramesNotEqualError) as e_info:
        assert_approx_df_equality(df1, df2, 0.1)


def test_assert_approx_df_equality_with_no_mismatch():
    data1 = [(1.0, "jose"), (1.1, "li"), (1.2, "laura"), (None, None)]
    df1 = spark.createDataFrame(data1, ["num", "expected_name"])

    data2 = [(1.0, "jose"), (1.05, "li"), (1.2, "laura"), (None, None)]
    df2 = spark.createDataFrame(data2, ["num", "expected_name"])

    assert_approx_df_equality(df1, df2, 0.1)
