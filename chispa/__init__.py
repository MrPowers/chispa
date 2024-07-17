import os
import sys
from glob import glob

# Add PySpark to the library path based on the value of SPARK_HOME if pyspark is not already in our path
try:
    from pyspark import context  # noqa: F401
except ImportError:
    # We need to add PySpark, try use findspark, or failback to the "manually" find it
    try:
        import findspark

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
    def __init__(self, formats=DefaultFormats(), default_output=None):
        self.formats = formats
        self.default_outputs = default_output

    def assert_df_equality(
        self,
        df1,
        df2,
        ignore_nullable=False,
        transforms=None,
        allow_nan_equality=False,
        ignore_column_order=False,
        ignore_row_order=False,
        underline_cells=False,
        ignore_metadata=False,
    ):
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
    "DefaultFormats",
    "Chispa",
)
