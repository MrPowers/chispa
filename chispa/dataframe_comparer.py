from functools import reduce
from typing import Callable, Optional

from pyspark.sql import DataFrame

from chispa.schema_comparer import assert_schema_equality
from chispa.row_comparer import assert_rows_equality


def assert_df_equality(
    df1: DataFrame,
    df2: DataFrame,
    precision: Optional[float] = None,
    ignore_nullable: bool = False,
    allow_nan_equality: bool = False,
    ignore_column_order: bool = False,
    ignore_row_order: bool = False,
    transforms: Callable[[DataFrame], DataFrame] = None,
) -> None:
    """Assert that two PySpark DataFrames are equal.

    Parameters
    ----------
    precision : float, optional
        Absolute tolerance when checking for equality.
    ignore_nullable : bool, default False
        Ignore nullable option when comparing schemas.
    allow_nan_equality : bool, default False
        When True, treats two NaN values as equal.
    ignore_column_order : bool, default False
        When True, sorts columns before comparing.
    ignore_row_order : bool, default False
        When True, sorts all rows before comparing.
    transforms : callable
        Additional transforms to make to DataFrame before comparison.

    """
    # Apply row and column order transforms + custom transforms.
    if transforms is None:
        transforms = []
    if ignore_column_order:
        transforms.append(lambda df: df.select(sorted(df.columns)))
    if ignore_row_order:
        transforms.append(lambda df: df.sort(df.columns))

    df1 = reduce(lambda acc, fn: fn(acc), transforms, df1)
    df2 = reduce(lambda acc, fn: fn(acc), transforms, df2)

    # Check schema and row equality.
    assert_schema_equality(df1.schema, df2.schema, ignore_nullable)
    assert_rows_equality(df1, df2, precision, allow_nan_equality)
