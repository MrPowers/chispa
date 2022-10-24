import math


def isnan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def nan_safe_equality(x, y) -> bool:
    return (x == y) or (isnan(x) and isnan(y))


def nan_safe_approx_equality(x, y, precision) -> bool:
    return (abs(x-y)<=precision) or (isnan(x) and isnan(y))