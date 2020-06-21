from chispa.prettytable import PrettyTable
from chispa.bcolors import *
from chispa.schema_comparer import assert_schema_equality, assert_schema_equality_ignore_nullable, are_schemas_equal_ignore_nullable
from chispa.row_comparer import are_rows_approx_equal
import chispa.six as six


class DataFramesNotEqualError(Exception):
   """The DataFrames are not equal"""
   pass


def assert_df_equality(df1, df2, ignore_nullable = False):
    s1 = df1.schema
    s2 = df2.schema
    if ignore_nullable:
        assert_schema_equality_ignore_nullable(s1, s2)
    else:
        assert_schema_equality(s1, s2)
    rows1 = df1.collect()
    rows2 = df2.collect()
    if rows1 != rows2:
        t = PrettyTable(["df1", "df2"])
        zipped = list(six.moves.zip_longest(rows1, rows2))
        for r1, r2 in zipped:
            if r1 == r2:
                t.add_row([blue(r1), blue(r2)])
            else:
                t.add_row([r1, r2])
        raise DataFramesNotEqualError("\n" + t.get_string())


def are_dfs_equal(df1, df2):
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True


def assert_approx_df_equality(df1, df2, precision):
    s1 = df1.schema
    s2 = df2.schema
    assert_schema_equality(s1, s2)
    df1_rows = df1.collect()
    df2_rows = df2.collect()
    zipped = list(six.moves.zip_longest(df1_rows, df2_rows))
    t = PrettyTable(["df1", "df2"])
    allRowsEqual = True
    for r1, r2 in zipped:
        # rows are not equal when one is None and the other isn't
        if (r1 is not None and r2 is None) or (r2 is not None and r1 is None):
            allRowsEqual = False
            t.add_row([r1, r2])
        # rows are equal
        elif are_rows_approx_equal(r1, r2, precision):
            first = bcolors.LightBlue + str(r1) + bcolors.LightRed
            second = bcolors.LightBlue + str(r2) + bcolors.LightRed
            t.add_row([first, second])
        # otherwise, rows aren't equal
        else:
            allRowsEqual = False
            t.add_row([r1, r2])
    if allRowsEqual == False:
        raise DataFramesNotEqualError("\n" + t.get_string())

