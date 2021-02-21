import sys
import os
from glob import glob

# Add PySpark to the library path based on the value of SPARK_HOME if pyspark is not already in our path
try:
    from pyspark import context
except ImportError:
    # We need to add PySpark, try use findspark, or failback to the "manually" find it
    try:
        import findspark
        findspark.init()
    except ImportError:
        try:
            spark_home = os.environ['SPARK_HOME']
            sys.path.append(os.path.join(spark_home, 'python'))
            py4j_src_zip = glob(os.path.join(spark_home, 'python', 'lib', 'py4j-*-src.zip'))
            if len(py4j_src_zip) == 0:
                raise ValueError('py4j source archive not found in %s'
                                 % os.path.join(spark_home, 'python', 'lib'))
            else:
                py4j_src_zip = sorted(py4j_src_zip)[::-1]
                sys.path.append(py4j_src_zip[0])
        except KeyError:
            print("Can't find Apache Spark. Please set environment variable SPARK_HOME to root of installation!")
            exit(-1)

from .dataframe_comparer import DataFramesNotEqualError, assert_df_equality, assert_approx_df_equality
from .column_comparer import ColumnsNotEqualError, assert_column_equality, assert_approx_column_equality
