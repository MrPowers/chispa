from chispa.prettytable import PrettyTable
from chispa.bcolors import *
from chispa.schema_comparer import assert_schema_equality
from chispa.row_comparer import *
import chispa.six as six
from functools import reduce


class DataFramesNotEqualError(Exception):
   """The DataFrames are not equal"""
   pass


def assert_df_equality(df1, df2, ignore_nullable=False, transforms=None, allow_nan_equality=False, ignore_column_order=False, ignore_row_order=False):
    if transforms is None:
        transforms = []
    if ignore_column_order:
        transforms.append(lambda df: df.select(sorted(df.columns)))
    if ignore_row_order:
        transforms.append(lambda df: df.sort(df.columns))
    df1 = reduce(lambda acc, fn: fn(acc), transforms, df1)
    df2 = reduce(lambda acc, fn: fn(acc), transforms, df2)
    assert_schema_equality(df1.schema, df2.schema, ignore_nullable)
    if allow_nan_equality:
        assert_generic_rows_equality(df1, df2, are_rows_equal_enhanced, [True])
    else:
        assert_basic_rows_equality(df1, df2)


def are_dfs_equal(df1, df2):
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True


def assert_approx_df_equality(df1, df2, precision, ignore_nullable=False):
    assert_schema_equality(df1.schema, df2.schema, ignore_nullable)
    assert_generic_rows_equality(df1, df2, are_rows_approx_equal, [precision])


def assert_generic_rows_equality(df1, df2, row_equality_fun, row_equality_fun_args):
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
        elif row_equality_fun(r1, r2, *row_equality_fun_args):
            first = bcolors.LightBlue + str(r1) + bcolors.LightRed
            second = bcolors.LightBlue + str(r2) + bcolors.LightRed
            t.add_row([first, second])
        # otherwise, rows aren't equal
        else:
            allRowsEqual = False
            t.add_row([r1, r2])
    if allRowsEqual == False:
        raise DataFramesNotEqualError("\n" + t.get_string())


def assert_basic_rows_equality(df1, df2):
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
