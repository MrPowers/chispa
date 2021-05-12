import math
from typing import Optional


def isnan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def check_equal(
    x, y,
    precision: Optional[float] = None,
    allow_nan_equality: bool = False,
) -> bool:
    """Return True if x and y are equal.

    Parameters
    ----------
    precision : float, optional
        Absolute tolerance when checking for equality.
    allow_nan_equality: bool, defaults to False
        When True, treats two NaN values as equal.

    """
    both_floats = (isinstance(x, float) & isinstance(y, float))
    if (precision is not None) & both_floats:
        both_equal = abs(x - y) < precision
    else:
        both_equal = (x == y)

    both_nan = (isnan(x) and isnan(y)) if allow_nan_equality else False

    return both_equal or both_nan
