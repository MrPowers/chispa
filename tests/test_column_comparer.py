import pytest

from spark import *
from chispa import *


def test_assert_column_equality_with_mismatch():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    with pytest.raises(ColumnsNotEqualError) as e_info:
        assert_column_equality(df, "name", "expected_name")


def test_assert_column_equality_no_mismatches():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "luisa"), (None, None)]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    assert_column_equality(df, "name", "expected_name")


def test_assert_column_equality_no_mismatches():
    data = [(1, 1), (10, 10), (8, 8), (None, None)]
    df = spark.createDataFrame(data, ["num1", "num2"])
    assert_column_equality(df, "num1", "num2")


def test_assert_approx_column_equality_no_mismatches():
    data = [(1.1, 1.1), (1.0004, 1.0005), (.4, .45), (None, None)]
    df = spark.createDataFrame(data, ["num1", "num2"])
    assert_approx_column_equality(df, "num1", "num2", 0.1)


def test_assert_approx_column_equality_with_mismatch():
    data = [(1.5, 1.1), (1.0004, 1.0005), (.4, .45)]
    df = spark.createDataFrame(data, ["num1", "num2"])
    with pytest.raises(ColumnsNotEqualError) as e_info:
        assert_approx_column_equality(df, "num1", "num2", 0.1)


def test_assert_approx_column_equality_with_none_edge_case():
    data = [(1.1, 1.1), (2.2, 2.2), (3.3, None)]
    df = spark.createDataFrame(data, ["num1", "num2"])
    with pytest.raises(ColumnsNotEqualError) as e_info:
        assert_approx_column_equality(df, "num1", "num2", 0.1)


def test_assert_approx_column_equality_with_none_edge_case2():
    data = [(1.1, 1.1), (2.2, 2.2), (None, 3.3)]
    df = spark.createDataFrame(data, ["num1", "num2"])
    with pytest.raises(ColumnsNotEqualError) as e_info:
        assert_approx_column_equality(df, "num1", "num2", 0.1)

