import pytest

from spark import *
from chispa import *


def describe_assert_column_equality():
    def it_throws_error_with_data_mismatch():
        data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
        df = spark.createDataFrame(data, ["name", "expected_name"])
        with pytest.raises(ColumnsNotEqualError) as e_info:
            assert_column_equality(df, "name", "expected_name")

    def it_doesnt_throw_without_mismatch():
        data = [("jose", "jose"), ("li", "li"), ("luisa", "luisa"), (None, None)]
        df = spark.createDataFrame(data, ["name", "expected_name"])
        assert_column_equality(df, "name", "expected_name")


    def it_works_with_integer_values():
        data = [(1, 1), (10, 10), (8, 8), (None, None)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        assert_column_equality(df, "num1", "num2")


    def it_equates_nans_when_allow_nan_equality_is_True():
        data = [(1.0, 1.0), (10.3, 10.3), (float('nan'), float('nan')), (None, None)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        assert_column_equality(df, "num1", "num2", allow_nan_equality=True)


def describe_assert_approx_column_equality():
    def it_works_with_no_mismatches():
        data = [(1.1, 1.1), (1.0004, 1.0005), (.4, .45), (None, None)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        assert_column_equality(df, "num1", "num2", precision=0.1)


    def it_throws_when_difference_is_bigger_than_precision():
        data = [(1.5, 1.1), (1.0004, 1.0005), (.4, .45)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        with pytest.raises(ColumnsNotEqualError) as e_info:
            assert_column_equality(df, "num1", "num2", precision=0.1)


    def it_throws_when_comparing_floats_with_none():
        data = [(1.1, 1.1), (2.2, 2.2), (3.3, None)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        with pytest.raises(ColumnsNotEqualError) as e_info:
            assert_column_equality(df, "num1", "num2", precision=0.1)


    def it_throws_when_comparing_none_with_floats():
        data = [(1.1, 1.1), (2.2, 2.2), (None, 3.3)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        with pytest.raises(ColumnsNotEqualError) as e_info:
            assert_column_equality(df, "num1", "num2", precision=0.1)
