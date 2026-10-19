from __future__ import annotations

from collections.abc import Callable
from functools import reduce
from typing import cast

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DataType, MapType, StructType

from chispa.formatting import FormattingConfig
from chispa.row_comparer import are_rows_approx_equal, are_rows_equal_enhanced
from chispa.rows_comparer import (
    assert_basic_rows_equality,
    assert_generic_rows_equality,
)
from chispa.schema_comparer import assert_schema_equality


def _contains_map_type(dt: ArrayType | MapType | StructType) -> bool:
    """Return True if the data type contains a MapType anywhere in its tree."""
    if isinstance(dt, MapType):
        return True
    if isinstance(dt, ArrayType):
        return isinstance(dt.elementType, (StructType, ArrayType, MapType)) and _contains_map_type(dt.elementType)
    if isinstance(dt, StructType):
        return any(
            isinstance(f.dataType, (StructType, ArrayType, MapType)) and _contains_map_type(f.dataType)
            for f in dt.fields
        )
    return False


def _contains_struct_type(dt: DataType) -> bool:
    """Return True if the data type is a struct or has a struct anywhere in its tree."""
    if isinstance(dt, StructType):
        return True
    if isinstance(dt, ArrayType):
        return _contains_struct_type(dt.elementType)
    if isinstance(dt, MapType):
        return _contains_struct_type(dt.keyType) or _contains_struct_type(dt.valueType)
    return False


def _sort_nested_fields(col: Column, dt: DataType) -> Column:
    """Return the column with the fields of every struct in its type tree sorted by name."""
    if not _contains_struct_type(dt):
        return col
    if isinstance(dt, StructType):
        fields = sorted(dt.fields, key=lambda f: f.name)
        sorted_struct = F.struct(*[_sort_nested_fields(col.getField(f.name), f.dataType).alias(f.name) for f in fields])
        # F.struct builds a struct of nulls from a null struct, so keep the nulls
        return F.when(col.isNotNull(), sorted_struct)
    if isinstance(dt, ArrayType):
        return F.transform(col, lambda element: _sort_nested_fields(element, dt.elementType))
    # the only other types that contain a struct are maps
    map_type = cast(MapType, dt)
    sorted_map = col
    if _contains_struct_type(map_type.keyType):
        sorted_map = F.transform_keys(sorted_map, lambda k, _: _sort_nested_fields(k, map_type.keyType))
    if _contains_struct_type(map_type.valueType):
        sorted_map = F.transform_values(sorted_map, lambda _, v: _sort_nested_fields(v, map_type.valueType))
    return sorted_map


def _sort_columns_for_column_order_comparison(df: DataFrame) -> DataFrame:
    """Sort the columns of the DataFrame by name, including the fields of nested structs."""
    fields = sorted(df.schema.fields, key=lambda f: f.name)
    return df.select(*[
        _sort_nested_fields(F.col(f.name), f.dataType).alias(f.name)
        if _contains_struct_type(f.dataType)
        else F.col(f.name)
        for f in fields
    ])


def _sort_df_for_row_order_comparison(df: DataFrame) -> DataFrame:
    """Sort DataFrame for row-order-insensitive comparison.

    Struct/array columns use hash() for efficient sorting. Columns containing
    MapType anywhere in their type tree use to_json() instead, because Spark's
    hash() does not support MapType.
    """
    sort_exprs: list[Column] = []
    has_complex = False
    for field in df.schema.fields:
        col = F.col(field.name)
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            has_complex = True
            if _contains_map_type(field.dataType):
                sort_exprs.append(F.to_json(col))
            else:
                sort_exprs.append(F.hash(col))
        else:
            sort_exprs.append(col)

    if not has_complex:
        return df.sort(*df.columns)

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
    full_log: bool = True,
) -> None:
    if not formats:
        formats = FormattingConfig()
    elif not isinstance(formats, FormattingConfig):
        formats = FormattingConfig._from_arbitrary_dataclass(formats)

    transforms = [] if transforms is None else list(transforms)
    if ignore_column_order:
        transforms.append(_sort_columns_for_column_order_comparison)
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
            full_log=full_log,
        )
    else:
        assert_basic_rows_equality(
            df1.collect(),
            df2.collect(),
            underline_cells=underline_cells,
            formats=formats,
            full_log=full_log,
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
    full_log: bool = True,
) -> None:
    if not formats:
        formats = FormattingConfig()
    elif not isinstance(formats, FormattingConfig):
        formats = FormattingConfig._from_arbitrary_dataclass(formats)

    transforms = [] if transforms is None else list(transforms)
    if ignore_column_order:
        transforms.append(_sort_columns_for_column_order_comparison)
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
            full_log=full_log,
        )
    elif allow_nan_equality:
        assert_generic_rows_equality(
            df1.collect(),
            df2.collect(),
            are_rows_equal_enhanced,
            {"allow_nan_equality": True},
            formats=formats,
            full_log=full_log,
        )
    else:
        assert_basic_rows_equality(df1.collect(), df2.collect(), formats=formats, full_log=full_log)
