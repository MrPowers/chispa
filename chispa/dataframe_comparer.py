from __future__ import annotations

from collections.abc import Callable
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType

from chispa.formatting import FormattingConfig
from chispa.row_comparer import are_rows_approx_equal, are_rows_equal_enhanced
from chispa.rows_comparer import (
    assert_basic_rows_equality,
    assert_generic_rows_equality,
)
from chispa.schema_comparer import assert_schema_equality


def _get_hashable_columns(df: DataFrame) -> list[str]:
    """Return list of column names that need hash() for sorting.

    Spark's sort() cannot order struct, array, or map types directly.
    These columns need to be hashed before sorting when ignore_row_order=True.
    """
    hashable_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType | ArrayType | MapType):
            hashable_cols.append(field.name)
    return hashable_cols


def _sort_df_for_row_order_comparison(df: DataFrame) -> DataFrame:
    """Sort DataFrame for row-order-insensitive comparison.

    For struct/array/map columns, applies hash() before sorting since
    Spark cannot directly sort these complex types.
    """
    hash_cols = _get_hashable_columns(df)
    if not hash_cols:
        # No complex types, sort normally
        return df.sort(df.columns)

    # Build sort expressions: hash complex cols, sort primitive cols normally
    sort_exprs = []
    for col_name in df.columns:
        if col_name in hash_cols:
            sort_exprs.append(F.hash(F.col(col_name)))
        else:
            sort_exprs.append(F.col(col_name))

    return df.sort(*sort_exprs)


class DataFramesNotEqualError(Exception):
    """The DataFrames are not equal"""

    pass


def assert_df_equality(
    df1: DataFrame,
    df2: DataFrame,
    ignore_nullable: bool = False,
    transforms: list[Callable] | None = None,  # type: ignore[type-arg]
    allow_nan_equality: bool = False,
    ignore_column_order: bool = False,
    ignore_row_order: bool = False,
    underline_cells: bool = False,
    ignore_metadata: bool = False,
    ignore_columns: list[str] | None = None,
    formats: FormattingConfig | None = None,
) -> None:
    if not formats:
        formats = FormattingConfig()
    elif not isinstance(formats, FormattingConfig):
        formats = FormattingConfig._from_arbitrary_dataclass(formats)

    if transforms is None:
        transforms = []
    if ignore_column_order:
        transforms.append(lambda df: df.select(sorted(df.columns)))
    if ignore_columns:
        transforms.append(lambda df: df.drop(*ignore_columns))
    if ignore_row_order:
        transforms.append(_sort_df_for_row_order_comparison)

    df1 = reduce(lambda acc, fn: fn(acc), transforms, df1)
    df2 = reduce(lambda acc, fn: fn(acc), transforms, df2)

    assert_schema_equality(df1.schema, df2.schema, ignore_nullable, ignore_metadata)

    if allow_nan_equality:
        assert_generic_rows_equality(
            df1.collect(),
            df2.collect(),
            are_rows_equal_enhanced,
            {"allow_nan_equality": True},
            underline_cells=underline_cells,
            formats=formats,
        )
    else:
        assert_basic_rows_equality(
            df1.collect(),
            df2.collect(),
            underline_cells=underline_cells,
            formats=formats,
        )


def are_dfs_equal(df1: DataFrame, df2: DataFrame) -> bool:
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True


def assert_approx_df_equality(
    df1: DataFrame,
    df2: DataFrame,
    precision: float,
    ignore_nullable: bool = False,
    transforms: list[Callable] | None = None,  # type: ignore[type-arg]
    allow_nan_equality: bool = False,
    ignore_column_order: bool = False,
    ignore_row_order: bool = False,
    ignore_columns: list[str] | None = None,
    formats: FormattingConfig | None = None,
) -> None:
    if not formats:
        formats = FormattingConfig()
    elif not isinstance(formats, FormattingConfig):
        formats = FormattingConfig._from_arbitrary_dataclass(formats)

    if transforms is None:
        transforms = []
    if ignore_column_order:
        transforms.append(lambda df: df.select(sorted(df.columns)))
    if ignore_columns:
        transforms.append(lambda df: df.drop(*ignore_columns))
    if ignore_row_order:
        transforms.append(_sort_df_for_row_order_comparison)

    df1 = reduce(lambda acc, fn: fn(acc), transforms, df1)
    df2 = reduce(lambda acc, fn: fn(acc), transforms, df2)

    assert_schema_equality(df1.schema, df2.schema, ignore_nullable)

    if precision != 0:
        assert_generic_rows_equality(
            df1.collect(),
            df2.collect(),
            are_rows_approx_equal,
            {"precision": precision, "allow_nan_equality": allow_nan_equality},
            formats=formats,
        )
    elif allow_nan_equality:
        assert_generic_rows_equality(
            df1.collect(), df2.collect(), are_rows_equal_enhanced, {"allow_nan_equality": True}, formats=formats
        )
    else:
        assert_basic_rows_equality(df1.collect(), df2.collect(), formats=formats)
