from __future__ import annotations

import pytest
from pyspark.sql import Row

from chispa.row_comparer import are_rows_approx_equal, are_rows_equal, are_rows_equal_enhanced


@pytest.mark.parametrize(
    ("r1", "r2", "expected"),
    [
        pytest.param(Row("bob", "jose"), Row("li", "li"), False, id="different values"),
        pytest.param(Row("luisa", "laura"), Row("luisa", "laura"), True, id="identical values"),
        pytest.param(Row(None, None), Row(None, None), True, id="None values"),
    ],
)
def test_are_rows_equal(r1, r2, expected):
    assert are_rows_equal(r1, r2) is expected


@pytest.mark.parametrize(
    ("r1", "r2", "allow_nan_equality", "expected"),
    [
        pytest.param(Row(n1="bob", n2="jose"), Row(n1="li", n2="li"), False, False, id="different values"),
        pytest.param(Row(n1="luisa", n2="laura"), Row(n1="luisa", n2="laura"), False, True, id="identical values"),
        pytest.param(Row(n1=None, n2=None), Row(n1=None, n2=None), False, True, id="None values"),
        pytest.param(None, None, False, True, id="both rows missing"),
        pytest.param(Row(n1="Alex", n2="Bob"), None, False, False, id="one row missing"),
        pytest.param(
            Row(n1="Alex", n2="Bob", n3=["Shadow"]),
            Row(n1="Alex", n2="Bob", n3=["Shadow", "Sky"]),
            True,
            False,
            id="lists of different lengths",
        ),
        pytest.param(Row(n1=("Shadow",)), Row(n1=("Shadow", "Sky")), True, False, id="tuples of different lengths"),
        pytest.param(Row(n1=("Shadow", "Sky")), Row(n1=("Shadow", "Sky")), True, True, id="identical tuples"),
        pytest.param(
            Row(n1="bob", n2="jose"), Row(n1="li", n2="li"), True, False, id="different values with nan equality"
        ),
        pytest.param(Row(n1=float("nan"), n2="jose"), Row(n1=float("nan"), n2="jose"), True, True, id="matching nans"),
        pytest.param(Row(n1=float("nan"), n2="jose"), Row(n1="hi", n2="jose"), True, False, id="nan vs non-nan"),
        pytest.param(
            Row(nested=[[1.0, float("nan")], [3.0, 4.0]], name="jose"),
            Row(nested=[[1.0, float("nan")], [3.0, 4.0]], name="jose"),
            True,
            True,
            id="nested lists with nans in the same position",
        ),
        pytest.param(
            Row(nested=[[1.0, float("nan")], [3.0, 4.0]], name="jose"),
            Row(nested=[[float("nan"), 1.0], [3.0, 4.0]], name="jose"),
            True,
            False,
            id="nested lists with nans in different positions",
        ),
        pytest.param(Row("a", "a")(1.0, 2.0), Row("a", "a")(999.0, 2.0), True, False, id="duplicate field names"),
    ],
)
def test_are_rows_equal_enhanced(r1, r2, allow_nan_equality, expected):
    assert are_rows_equal_enhanced(r1, r2, allow_nan_equality) is expected


@pytest.mark.parametrize(
    ("r1", "r2", "precision", "allow_nan_equality", "expected"),
    [
        pytest.param(
            Row(num=1.1, first_name="li"), Row(num=1.05, first_name="li"), 0.1, False, True, id="within precision"
        ),
        pytest.param(
            Row(num=5.0, first_name="laura"), Row(num=5.0, first_name="laura"), 0.1, False, True, id="identical values"
        ),
        pytest.param(
            Row(num=5.0, first_name="laura"),
            Row(num=5.9, first_name="laura"),
            0.1,
            False,
            False,
            id="outside precision",
        ),
        pytest.param(
            Row(num=None, first_name=None), Row(num=None, first_name=None), 0.1, False, True, id="None values"
        ),
        pytest.param(None, None, 0.1, False, True, id="both rows missing"),
        pytest.param(Row(num=10, first_name="Vlad"), None, 0.1, False, False, id="one row missing"),
        pytest.param(
            Row(num=10.5, first_name="Vlad"),
            Row(num=10.0, first_name="Vlad"),
            0.1,
            True,
            False,
            id="outside precision with nan equality",
        ),
        pytest.param(
            Row(num=10.5, first_name="Vlad"),
            Row(num=float("nan"), first_name="Vlad"),
            0.1,
            False,
            False,
            id="nan vs number",
        ),
        pytest.param(Row(first_name="Alex"), Row(first_name="El"), 0.1, False, False, id="different strings"),
        pytest.param(Row("a", "a")(1.0, 2.0), Row("a", "a")(999.0, 2.0), 0.1, False, False, id="duplicate field names"),
        pytest.param(Row(num=1.0), Row(num=1.0, first_name="li"), 0.1, False, False, id="different lengths"),
    ],
)
def test_are_rows_approx_equal(r1, r2, precision, allow_nan_equality, expected):
    assert are_rows_approx_equal(r1, r2, precision, allow_nan_equality) is expected
