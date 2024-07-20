from __future__ import annotations

from prettytable import PrettyTable

from chispa import Color


class ColumnsNotEqualError(Exception):
    """The columns are not equal"""

    pass


def assert_column_equality(df, col_name1, col_name2):
    elements = df.select(col_name1, col_name2).collect()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    if colName1Elements != colName2Elements:
        zipped = list(zip(colName1Elements, colName2Elements))
        t = PrettyTable([col_name1, col_name2])
        for elements in zipped:
            if elements[0] == elements[1]:
                first = Color.LIGHT_BLUE + str(elements[0]) + Color.LIGHT_RED
                second = Color.LIGHT_BLUE + str(elements[1]) + Color.LIGHT_RED
                t.add_row([first, second])
            else:
                t.add_row([str(elements[0]), str(elements[1])])
        raise ColumnsNotEqualError("\n" + t.get_string())


def assert_approx_column_equality(df, col_name1, col_name2, precision):
    elements = df.select(col_name1, col_name2).collect()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    all_rows_equal = True
    zipped = list(zip(colName1Elements, colName2Elements))
    t = PrettyTable([col_name1, col_name2])
    for elements in zipped:
        first = Color.LIGHT_BLUE + str(elements[0]) + Color.LIGHT_RED
        second = Color.LIGHT_BLUE + str(elements[1]) + Color.LIGHT_RED
        # when one is None and the other isn't, they're not equal
        if (elements[0] is None and elements[1] is not None) or (elements[0] is not None and elements[1] is None):
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
