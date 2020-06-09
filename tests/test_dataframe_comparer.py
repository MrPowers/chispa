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


def test_are_rows_equal():
    data1 = [
        ("bob", "jose"),
        ("li", "li"),
        ("luisa", "laura"),
        ("luisa", "laura"),
        (None, None),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["name", "expected_name"])

    rows = df1.collect()
    row0 = rows[0] # bob,jose
    row1 = rows[1] # li,li
    row2 = rows[2] # luisa,laura
    row3 = rows[3] # luisa,laura
    row4 = rows[4] # None,None
    row5 = rows[5] # None,None

    assert are_rows_equal(row0, row1) == False
    assert are_rows_equal(row2, row3) == True
    assert are_rows_equal(row4, row5) == True


def test_are_rows_approx_equal():
    data1 = [
        (1.1, "li"),
        (1.05, "li"),
        (5.0, "laura"),
        (5.0, "laura"),
        (5.0, "laura"),
        (5.9, "laura"),
        (None, None),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["name", "expected_name"])

    rows = df1.collect()
    row0 = rows[0]
    row1 = rows[1]
    row2 = rows[2]
    row3 = rows[3]
    row4 = rows[4]
    row5 = rows[5]
    row6 = rows[6]
    row7 = rows[7]

    assert are_rows_approx_equal(row0, row1, 0.1) == True
    assert are_rows_approx_equal(row2, row3, 0.1) == True
    assert are_rows_approx_equal(row4, row5, 0.1) == False
    assert are_rows_approx_equal(row6, row7, 0.1) == True


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
