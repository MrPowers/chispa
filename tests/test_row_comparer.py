import pytest

from spark import *
from chispa.row_comparer import are_rows_equal, are_rows_approx_equal
from pyspark.sql import Row


def test_are_rows_equal():
    assert are_rows_equal(Row("bob", "jose"), Row("li", "li")) == False
    assert are_rows_equal(Row("luisa", "laura"), Row("luisa", "laura")) == True
    assert are_rows_equal(Row(None, None), Row(None, None)) == True


def test_are_rows_approx_equal():
    assert are_rows_approx_equal(Row(num = 1.1, first_name = "li"), Row(num = 1.05, first_name = "li"), 0.1) == True
    assert are_rows_approx_equal(Row(num = 5.0, first_name = "laura"), Row(num = 5.0, first_name = "laura"), 0.1) == True
    assert are_rows_approx_equal(Row(num = 5.0, first_name = "laura"), Row(num = 5.9, first_name = "laura"), 0.1) == False
    assert are_rows_approx_equal(Row(num = None, first_name = None), Row(num = None, first_name = None), 0.1) == True

