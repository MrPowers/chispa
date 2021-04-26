from typing import Optional, Any

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from chispa.bcolors import blue
from chispa.prettytable import PrettyTable
from chispa.number_helpers import check_equal


class ColumnsNotEqualError(Exception):
   """The columns are not equal"""
   pass


def assert_column_equality(
    df: DataFrame,
    col_name1: str,
    col_name2: str,
    precision: Optional[float] = None,
    allow_nan_equality: bool = False,
) -> None:
    """Assert that two columns in a PySpark DataFrame are equal.

    Parameters
    ----------
    precision : float, optional
        Absolute tolerance when checking for equality.
    allow_nan_equality : bool, default False
        When True, treats two NaN values as equal.

    """
    all_rows_equal = True
    t = PrettyTable([col_name1, col_name2])

    # Zip both columns together for iterating through elements.
    columns = df.select(col_name1, col_name2).collect()
    zipped = zip(*[list(map(lambda x: x[i], columns)) for i in [0, 1]])

    for elements in zipped:
        if are_elements_equal(*elements, precision, allow_nan_equality):
            t.add_row([blue(e) for e in elements])
        else:
            all_rows_equal = False
            t.add_row([str(e) for e in elements])

    if all_rows_equal == False:
        raise ColumnsNotEqualError("\n" + t.get_string())


def are_elements_equal(
    e1: DataType,
    e2: DataType,
    precision: Optional[float] = None,
    allow_nan_equality: bool = False,
) -> bool:
    """
    Return True if both elements are equal.

    Parameters
    ----------
    precision : float, optional
        Absolute tolerance when checking for equality.
    allow_nan_equality: bool, default False
        When True, treats two NaN values as equal.

    """
    # If both elements are None they are considered equal.
    if e1 is None and e2 is None:
        return True
    # If one element is None and the other isn't, then they are not equal.
    if (e1 is None and e2 is not None) or (e2 is None and e1 is not None):
        return False

    # Compare the elements.
    return check_equal(e1, e2, precision, allow_nan_equality)

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession
        .builder
        .master("local[2]")
        .appName("test_context")
        .config("spark.sql.shuffle.partitions", 1)
        # .config("spark.jars", jar_path)
        # This stops progress bars appearing in the console whilst running
        .config('spark.ui.showConsoleProgress', 'false')
        .getOrCreate()
    )

    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    assert_column_equality(df, "name", "expected_name")