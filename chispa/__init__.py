from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame

from chispa.default_formats import DefaultFormats
from chispa.formatting import Color, Format, FormattingConfig, Style

from .column_comparer import (
    ColumnsNotEqualError,
    assert_approx_column_equality,
    assert_column_equality,
)
from .dataframe_comparer import (
    DataFramesNotEqualError,
    assert_approx_df_equality,
    assert_df_equality,
)
from .rows_comparer import assert_basic_rows_equality
from .schema_comparer import SchemasNotEqualError


class Chispa:
    def __init__(self, formats: FormattingConfig | None = None) -> None:
        if not formats:
            self.formats = FormattingConfig()
        elif isinstance(formats, FormattingConfig):
            self.formats = formats
        else:
            self.formats = FormattingConfig._from_arbitrary_dataclass(formats)

    def assert_df_equality(
        self,
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
    ) -> None:
        return assert_df_equality(
            df1,
            df2,
            ignore_nullable,
            transforms,
            allow_nan_equality,
            ignore_column_order,
            ignore_row_order,
            underline_cells,
            ignore_metadata,
            ignore_columns,
            self.formats,
        )


__all__ = (
    "Chispa",
    "Color",
    "ColumnsNotEqualError",
    "DataFramesNotEqualError",
    "DefaultFormats",
    "Format",
    "FormattingConfig",
    "SchemasNotEqualError",
    "Style",
    "assert_approx_column_equality",
    "assert_approx_df_equality",
    "assert_basic_rows_equality",
    "assert_column_equality",
    "assert_df_equality",
)
