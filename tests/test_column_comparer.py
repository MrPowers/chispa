import pytest

from chispa.spark import *
from chispa.column_comparer import *

def test_assert_column_equality_with_mismatch():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])

    with pytest.raises(ColumnsNotEqualError) as e_info:
        assert_column_equality(df, "name", "expected_name")


def test_assert_column_equality_no_mismatches():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "luisa")]
    df = spark.createDataFrame(data, ["name", "expected_name"])

    assert_column_equality(df, "name", "expected_name")
