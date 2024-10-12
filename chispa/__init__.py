from __future__ import annotations

import os
import sys
from glob import glob
from typing import Callable

from pyspark.sql import DataFrame

# Add PySpark to the library path based on the value of SPARK_HOME if pyspark is not already in our path
try:
    from pyspark import context  # noqa: F401
except ImportError:
    # We need to add PySpark, try use findspark, or failback to the "manually" find it
    try:
        import findspark  # type: ignore[import-untyped]

        findspark.init()
    except ImportError:
        try:
            spark_home = os.environ["SPARK_HOME"]
            sys.path.append(os.path.join(spark_home, "python"))
            py4j_src_zip = glob(os.path.join(spark_home, "python", "lib", "py4j-*-src.zip"))
            if len(py4j_src_zip) == 0:
                raise ValueError(
                    "py4j source archive not found in {}".format(os.path.join(spark_home, "python", "lib"))
                )
            else:
                py4j_src_zip = sorted(py4j_src_zip)[::-1]
                sys.path.append(py4j_src_zip[0])
        except KeyError:
            print("Can't find Apache Spark. Please set environment variable SPARK_HOME to root of installation!")
            exit(-1)

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
    "DataFramesNotEqualError",
    "assert_df_equality",
    "assert_approx_df_equality",
    "ColumnsNotEqualError",
    "assert_column_equality",
    "assert_approx_column_equality",
    "assert_basic_rows_equality",
    "Style",
    "Color",
    "FormattingConfig",
    "Format",
    "Chispa",
    "DefaultFormats",
)
