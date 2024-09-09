from __future__ import annotations

from prettytable import PrettyTable
from pyspark.sql import DataFrame

from chispa.formatting import blue


class ColumnsNotEqualError(Exception):
    """The columns are not equal"""

    pass


def assert_column_equality(df: DataFrame, col_name1: str, col_name2: str) -> None:
    rows = df.select(col_name1, col_name2).collect()
    col_name_1_elements = [x[0] for x in rows]
    col_name_2_elements = [x[1] for x in rows]
    if col_name_1_elements != col_name_2_elements:
        zipped = list(zip(col_name_1_elements, col_name_2_elements))
        t = PrettyTable([col_name1, col_name2])
        for elements in zipped:
            if elements[0] == elements[1]:
                t.add_row([blue(str(elements[0])), blue(str(elements[1]))])
            else:
                t.add_row([str(elements[0]), str(elements[1])])
        raise ColumnsNotEqualError("\n" + t.get_string())


def assert_approx_column_equality(df: DataFrame, col_name1: str, col_name2: str, precision: float) -> None:
    rows = df.select(col_name1, col_name2).collect()
    col_name_1_elements = [x[0] for x in rows]
    col_name_2_elements = [x[1] for x in rows]
    all_rows_equal = True
    zipped = list(zip(col_name_1_elements, col_name_2_elements))
    t = PrettyTable([col_name1, col_name2])
    for elements in zipped:
        first = blue(str(elements[0]))
        second = blue(str(elements[1]))
        # when one is None and the other isn't, they're not equal
        if (elements[0] is None) != (elements[1] is None):
            all_rows_equal = False
            t.add_row([str(elements[0]), str(elements[1])])
        # when both are None, they're equal
        elif elements[0] is None and elements[1] is None:
            t.add_row([first, second])
        # when the diff is less than the threshhold, they're approximately equal
        elif abs(elements[0] - elements[1]) < precision:
            t.add_row([first, second])
        # otherwise, they're not equal
        else:
            all_rows_equal = False
            t.add_row([str(elements[0]), str(elements[1])])
    if all_rows_equal is False:
        raise ColumnsNotEqualError("\n" + t.get_string())
