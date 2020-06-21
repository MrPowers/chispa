import pytest

from spark import *
from chispa.row_comparer import are_rows_equal, are_rows_approx_equal


def test_are_rows_equal():
    data1 = [
        ("bob", "jose"),
        ("li", "li"),
        ("luisa", "laura"),
        ("luisa", "laura"),
        (None, None),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["name", "expected_name"])

    rows = df1.collect()
    row0 = rows[0] # bob,jose
    row1 = rows[1] # li,li
    row2 = rows[2] # luisa,laura
    row3 = rows[3] # luisa,laura
    row4 = rows[4] # None,None
    row5 = rows[5] # None,None

    assert are_rows_equal(row0, row1) == False
    assert are_rows_equal(row2, row3) == True
    assert are_rows_equal(row4, row5) == True


def test_are_rows_approx_equal():
    data1 = [
        (1.1, "li"),
        (1.05, "li"),
        (5.0, "laura"),
        (5.0, "laura"),
        (5.0, "laura"),
        (5.9, "laura"),
        (None, None),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["name", "expected_name"])

    rows = df1.collect()
    row0 = rows[0]
    row1 = rows[1]
    row2 = rows[2]
    row3 = rows[3]
    row4 = rows[4]
    row5 = rows[5]
    row6 = rows[6]
    row7 = rows[7]

    assert are_rows_approx_equal(row0, row1, 0.1) == True
    assert are_rows_approx_equal(row2, row3, 0.1) == True
    assert are_rows_approx_equal(row4, row5, 0.1) == False
    assert are_rows_approx_equal(row6, row7, 0.1) == True

