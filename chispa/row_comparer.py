from __future__ import annotations

import math

from pyspark.sql import Row

from chispa.number_helpers import isnan, nan_safe_approx_equality


def are_rows_equal(r1: Row, r2: Row) -> bool:
    return r1 == r2


def _nan_safe_deep_equality(left: object, right: object) -> bool:
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return False
        return all(_nan_safe_deep_equality(left_item, right_item) for left_item, right_item in zip(left, right))
    if isinstance(left, tuple) and isinstance(right, tuple):
        if len(left) != len(right):
            return False
        return all(_nan_safe_deep_equality(left_item, right_item) for left_item, right_item in zip(left, right))
    return (left == right) or (isnan(left) and isnan(right))


def are_rows_equal_enhanced(r1: Row | None, r2: Row | None, allow_nan_equality: bool) -> bool:
    if r1 is None and r2 is None:
        return True
    if r1 is None or r2 is None:
        return False
    if allow_nan_equality:
        # compare by position: asDict() keeps only the last of several columns with the same name
        return _nan_safe_deep_equality(r1, r2)
    else:
        return r1 == r2


def are_rows_approx_equal(r1: Row | None, r2: Row | None, precision: float, allow_nan_equality: bool = False) -> bool:
    if r1 is None and r2 is None:
        return True
    if r1 is None or r2 is None:
        return False
    if len(r1) != len(r2):
        return False
    allEqual = True
    # compare by position: asDict() keeps only the last of several columns with the same name
    for v1, v2 in zip(r1, r2):
        if isinstance(v1, float) and isinstance(v2, float):
            if allow_nan_equality and not (nan_safe_approx_equality(v1, v2, precision)):
                allEqual = False
            elif not (allow_nan_equality) and math.isnan(abs(v1 - v2)):
                allEqual = False
            elif abs(v1 - v2) > precision:
                allEqual = False
        elif v1 != v2:
            allEqual = False
    return allEqual
