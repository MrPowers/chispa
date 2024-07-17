from chispa.row_comparer import are_rows_equal_enhanced, are_rows_equal, are_rows_approx_equal
from pyspark.sql import Row


def test_are_rows_equal():
    assert are_rows_equal(Row("bob", "jose"), Row("li", "li")) is False
    assert are_rows_equal(Row("luisa", "laura"), Row("luisa", "laura")) is True
    assert are_rows_equal(Row(None, None), Row(None, None)) is True


def test_are_rows_equal_enhanced():
    assert are_rows_equal_enhanced(Row(n1="bob", n2="jose"), Row(n1="li", n2="li"), False) is False
    assert are_rows_equal_enhanced(Row(n1="luisa", n2="laura"), Row(n1="luisa", n2="laura"), False) is True
    assert are_rows_equal_enhanced(Row(n1=None, n2=None), Row(n1=None, n2=None), False) is True

    assert are_rows_equal_enhanced(Row(n1="bob", n2="jose"), Row(n1="li", n2="li"), True) is False
    assert are_rows_equal_enhanced(Row(n1=float("nan"), n2="jose"), Row(n1=float("nan"), n2="jose"), True) is True
    assert are_rows_equal_enhanced(Row(n1=float("nan"), n2="jose"), Row(n1="hi", n2="jose"), True) is False


def test_are_rows_approx_equal():
    assert are_rows_approx_equal(Row(num=1.1, first_name="li"), Row(num=1.05, first_name="li"), 0.1) is True
    assert are_rows_approx_equal(Row(num=5.0, first_name="laura"), Row(num=5.0, first_name="laura"), 0.1) is True
    assert are_rows_approx_equal(Row(num=5.0, first_name="laura"), Row(num=5.9, first_name="laura"), 0.1) is False
    assert are_rows_approx_equal(Row(num=None, first_name=None), Row(num=None, first_name=None), 0.1) is True
