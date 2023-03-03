import chispa.six as six
from chispa.prettytable import PrettyTable
from chispa.bcolors import *
import chispa

def assert_basic_rows_equality(rows1, rows2):
    if rows1 != rows2:
        t = PrettyTable(["df1", "df2"])
        zipped = list(six.moves.zip_longest(rows1, rows2))
        for r1, r2 in zipped:
            if r1 == r2:
                t.add_row([blue(r1), blue(r2)])
            else:
                t.add_row([r1, r2])
        raise chispa.DataFramesNotEqualError("\n" + t.get_string())


def assert_generic_rows_equality(rows1, rows2, row_equality_fun, row_equality_fun_args):
    df1_rows = rows1
    df2_rows = rows2
    zipped = list(six.moves.zip_longest(df1_rows, df2_rows))
    t = PrettyTable(["df1", "df2"])
    allRowsEqual = True
    for r1, r2 in zipped:
        # rows are not equal when one is None and the other isn't
        if (r1 is not None and r2 is None) or (r2 is not None and r1 is None):
            allRowsEqual = False
            t.add_row([r1, r2])
        # rows are equal
        elif row_equality_fun(r1, r2, *row_equality_fun_args):
            first = bcolors.LightBlue + str(r1) + bcolors.LightRed
            second = bcolors.LightBlue + str(r2) + bcolors.LightRed
            t.add_row([first, second])
        # otherwise, rows aren't equal
        else:
            allRowsEqual = False
            t.add_row([r1, r2])
    if allRowsEqual == False:
        raise chispa.DataFramesNotEqualError("\n" + t.get_string())

