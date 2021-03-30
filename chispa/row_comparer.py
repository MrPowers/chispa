from pyspark.sql import Row
from chispa.number_helpers import nan_safe_equality


def are_rows_equal(r1: Row, r2: Row) -> bool:
    return r1 == r2


def are_rows_equal_enhanced(r1: Row, r2: Row, allow_nan_equality: bool) -> bool:
    if r1 is None and r2 is None:
        return True
    if (r1 is None and r2 is not None) or (r2 is None and r1 is not None):
        return False
    d1 = r1.asDict()
    d2 = r2.asDict()
    if allow_nan_equality:
        for key in d1.keys() & d2.keys():
            if not(nan_safe_equality(d1[key], d2[key])):
                return False
        return True
    else:
        return r1 == r2


def are_rows_approx_equal(r1: Row, r2: Row, precision: float) -> bool:
    if r1 is None and r2 is None:
        return True
    if (r1 is None and r2 is not None) or (r2 is None and r1 is not None):
        return False
    d1 = r1.asDict()
    d2 = r2.asDict()
    allEqual = True
    for key in d1.keys() & d2.keys():
        if isinstance(d1[key], float) and isinstance(d2[key], float):
            if abs(d1[key] - d2[key]) > precision:
                allEqual = False
        elif d1[key] != d2[key]:
            allEqual = False
    return allEqual

