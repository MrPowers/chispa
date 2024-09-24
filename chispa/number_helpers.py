from __future__ import annotations

import math
from decimal import Decimal
from typing import Any


def isnan(x: Any) -> bool:
    try:
        return math.isnan(x)
    except TypeError:
        return False


def nan_safe_equality(x: int | float, y: int | float | Decimal) -> bool:
    return (x == y) or (isnan(x) and isnan(y))


def nan_safe_approx_equality(x: int | float, y: int | float, precision: float | Decimal) -> bool:
    return (abs(x - y) <= precision) or (isnan(x) and isnan(y))
