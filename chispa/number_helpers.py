import math


def isnan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def nan_safe_equality(x, y) -> bool:
    return (x == y) or (isnan(x) and isnan(y))