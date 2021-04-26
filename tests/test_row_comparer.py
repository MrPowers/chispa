from chispa.row_comparer import are_rows_equal
from pyspark.sql import Row


def describe_are_rows_equal():
    def returns_False_when_string_values_are_not_equal():
        assert are_rows_equal(Row(n1="bob", n2="jose"), Row(n1="li", n2="li")) == False
    def returns_True_when_string_values_are_equal():
        assert are_rows_equal(Row(n1="luisa", n2="laura"), Row(n1="luisa", n2="laura")) == True
    def returns_True_when_both_rows_are_None():
        assert are_rows_equal(Row(n1=None, n2=None), Row(n1=None, n2=None)) == True


def describe_are_rows_equal_when_allowing_nan_equality():
    def returns_False_when_no_NaN_values_to_compare_and_other_values_are_not_equal():
        assert are_rows_equal(Row(n1="bob", n2="jose"), Row(n1="li", n2="li"), allow_nan_equality=True) == False
    def returns_True_when_either_the_values_are_equal_or_both_nan():
        assert are_rows_equal(Row(n1=float('nan'), n2="jose"), Row(n1=float('nan'), n2="jose"), allow_nan_equality=True) == True
    def returns_False_when_comparing_nan_to_string():
        assert are_rows_equal(Row(n1=float('nan'), n2="jose"), Row(n1="hi", n2="jose"), allow_nan_equality=True) == False


def describe_are_rows_equal_when_given_precision():
    def returns_True_when_float_value_difference_is_less_than_precision():
        assert are_rows_equal(Row(num = 1.1, first_name = "li"), Row(num = 1.05, first_name = "li"), precision=0.1) == True
    def returns_True_when_float_values_are_exactly_equal():
        assert are_rows_equal(Row(num = 5.0, first_name = "laura"), Row(num = 5.0, first_name = "laura"), precision=0.1) == True
    def returns_False_when_float_value_difference_is_more_than_precision():
        assert are_rows_equal(Row(num = 5.0, first_name = "laura"), Row(num = 5.9, first_name = "laura"), precision=0.1) == False
    def returns_True_when_all_values_are_Nones():
        assert are_rows_equal(Row(num = None, first_name = None), Row(num = None, first_name = None), precision=0.1) == True

