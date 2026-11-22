from __future__ import annotations

from chispa.number_helpers import nan_safe_approx_equality, nan_safe_equality


def test_nan_safe_equality():
    assert nan_safe_equality(float("nan"), float("nan")) is True
    assert nan_safe_equality(["101", "2"], ["10", "2"]) is False


def test_nan_safe_approx_equality():
    # a difference exactly equal to the precision is within tolerance
    assert nan_safe_approx_equality(10, 5, 5.0) is True
    assert nan_safe_approx_equality(10, 9, 5.0) is True
    assert nan_safe_approx_equality(10, 5, 4.0) is False
    # nan - nan is nan, so only the isnan check can make these equal
    assert nan_safe_approx_equality(float("nan"), float("nan"), 5.0) is True
    assert nan_safe_approx_equality(float("nan"), 5, 5.0) is False
    # inf - inf is nan too, but equal infinities are equal
    assert nan_safe_approx_equality(float("inf"), float("inf"), 5.0) is True
    assert nan_safe_approx_equality(float("inf"), float("-inf"), 5.0) is False
