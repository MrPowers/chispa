from chispa.schema_comparer import assert_schema_equality
from chispa.row_comparer import *
from chispa.rows_comparer import assert_basic_rows_equality, assert_generic_rows_equality
from functools import reduce


class DataFramesNotEqualError(Exception):
   """The DataFrames are not equal"""
   pass


def assert_df_equality(df1, df2, ignore_nullable=False, transforms=None, allow_nan_equality=False,
                       ignore_column_order=False, ignore_row_order=False, underline_cells=False, ignore_metadata=False):
    if transforms is None:
        transforms = []
    if ignore_column_order:
        transforms.append(lambda df: df.select(sorted(df.columns)))
    if ignore_row_order:
        transforms.append(lambda df: df.sort(df.columns))
    df1 = reduce(lambda acc, fn: fn(acc), transforms, df1)
    df2 = reduce(lambda acc, fn: fn(acc), transforms, df2)
    assert_schema_equality(df1.schema, df2.schema, ignore_nullable, ignore_metadata)
    if allow_nan_equality:
        assert_generic_rows_equality(
            df1.collect(), df2.collect(), are_rows_equal_enhanced, [True], underline_cells=underline_cells)
    else:
        assert_basic_rows_equality(
            df1.collect(), df2.collect(), underline_cells=underline_cells)


def are_dfs_equal(df1, df2):
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True


def assert_approx_df_equality(df1, df2, precision, ignore_nullable=False, transforms=None, allow_nan_equality=False,
                       ignore_column_order=False, ignore_row_order=False):
    if transforms is None:
        transforms = []
    if ignore_column_order:
        transforms.append(lambda df: df.select(sorted(df.columns)))
    if ignore_row_order:
        transforms.append(lambda df: df.sort(df.columns))
    df1 = reduce(lambda acc, fn: fn(acc), transforms, df1)
    df2 = reduce(lambda acc, fn: fn(acc), transforms, df2)
    assert_schema_equality(df1.schema, df2.schema, ignore_nullable)
    if precision != 0:
        assert_generic_rows_equality(df1.collect(), df2.collect(), are_rows_approx_equal, [precision, allow_nan_equality])
    elif allow_nan_equality:
        assert_generic_rows_equality(df1.collect(), df2.collect(), are_rows_equal_enhanced, [True])
    else:
        assert_basic_rows_equality(df1.collect(), df2.collect())
