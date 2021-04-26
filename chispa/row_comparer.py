from typing import Optional

from pyspark.sql import Row, DataFrame

from chispa.bcolors import blue
import chispa.six as six
from chispa.number_helpers import check_equal
from chispa.prettytable import PrettyTable


class RowsNotEqualError(Exception):
   """The DataFrame Rows are not all equal."""
   pass


def assert_rows_equality(
    df1: DataFrame,
    df2: DataFrame,
    precision: Optional[float] = None,
    allow_nan_equality: bool = False,
) -> None:
    """Asserts that all row values in the two DataFrames are equal.

    Raises an error with a PrettyTable row comparison if not.

    Parameters
    ----------
    precision : float, optional
        Absolute tolerance when checking for equality.
    allow_nan_equality: bool, default False
        When True, treats two NaN values as equal.

    """
    t = PrettyTable(["df1", "df2"])
    allRowsEqual = True

    zipped = six.moves.zip_longest(df1.collect(), df2.collect())
    for r1, r2 in zipped:
        if are_rows_equal(r1, r2, precision, allow_nan_equality):
            t.add_row([blue(r1), blue(r2)])
        else:
            allRowsEqual = False
            t.add_row([r1, r2])

    if allRowsEqual == False:
        raise RowsNotEqualError("\n" + t.get_string())


def are_rows_equal(
    r1: Row,
    r2: Row,
    precision: Optional[float] = None,
    allow_nan_equality: bool = False,
) -> bool:
    """
    Return True if both rows are equal.

    Parameters
    ----------
    precision : float, optional
        Absolute tolerance when checking for equality.
    allow_nan_equality: bool, default False
        When True, treats two NaN values as equal.

    """
    # If both rows are None they are considered equal.
    if r1 is None and r2 is None:
        return True
    if (r1 is None and r2 is not None) or (r2 is None and r1 is not None):
        return False

    # Compare the values for each row. Order matters.
    for v1, v2 in zip(r1.asDict().values(), r2.asDict().values()):
        if not check_equal(v1, v2, precision, allow_nan_equality):
            return False
    return True
