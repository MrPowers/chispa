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
    d1 = r1.asDict()
    d2 = r2.asDict()
    if allow_nan_equality:
        for key in d1.keys() & d2.keys():
            if not (_nan_safe_deep_equality(d1[key], d2[key])):
                return False
        return True
    else:
        return r1 == r2


def are_rows_approx_equal(r1: Row | None, r2: Row | None, precision: float, allow_nan_equality: bool = False) -> bool:
    if r1 is None and r2 is None:
        return True
    if r1 is None or r2 is None:
        return False
    d1 = r1.asDict()
    d2 = r2.asDict()
    allEqual = True
    for key in d1.keys() & d2.keys():
        if isinstance(d1[key], float) and isinstance(d2[key], float):
            if allow_nan_equality and not (nan_safe_approx_equality(d1[key], d2[key], precision)):
                allEqual = False
            elif not (allow_nan_equality) and math.isnan(abs(d1[key] - d2[key])):
                allEqual = False
            elif abs(d1[key] - d2[key]) > precision:
                allEqual = False
        elif d1[key] != d2[key]:
            allEqual = False
    return allEqual
